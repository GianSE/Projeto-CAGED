"""
Audita consistência das camadas: schema, tipos, completude e tradução.

POR QUE ISTO EXISTE
-------------------
O MTE reescreveu o layout da RAIS em 2023 — renomeou colunas, acrescentou
"_codigo", encurtou "remun" para "rem". Cada ponto do pipeline que casava nome
exato quebrou, e nenhum deu erro:

  1. o recorte de TI foi pulado  -> 402 milhões de linhas de mercado completo
                                    entraram no bucket de tecnologia
  2. a tradução saiu vazia       -> 33 colunas viraram 0
  3. a tipagem numérica não pegou -> remuneração DOUBLE até 2022, VARCHAR depois
  4. a retomada colapsou anos    -> 2007 com 37 arquivos, 2009 com 1

Todos reportaram "sucesso" com código de saída 0. O que revelou cada um foi
conferir NÚMEROS depois: contagem de linhas, cobertura por coluna, leitura de
volta. Este módulo transforma essas conferências avulsas em rotina.

A verificação mais valiosa é a de DERIVA DE SCHEMA entre partições: a mesma
coluna com tipo diferente em anos diferentes quebra qualquer consulta que
atravesse a série, e é invisível ao olhar um ano só.

Uso:
    python -m auditoria.consistencia
    python -m auditoria.consistencia --bucket silver-ti --tabela rais_vinc
"""
import argparse
import re
import sys
from collections import defaultdict

from extracao_ftp.config_extracao import (
    BUCKET_BRONZE,
    BUCKET_SILVER,
    BUCKET_SILVER_TI,
    conectar_duckdb,
)

TABELAS = ("caged_mov", "caged_for", "caged_exc", "caged_old", "caged_ajustes",
           "rais_estab", "rais_vinc")


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def _origem(caminho: str) -> str:
    """Identidade do arquivo de origem: partição + nome, sem índice nem pedaço."""
    partes = caminho.split("/")
    nome = re.sub(r"_parte\d+$", "", re.sub(r"_\d{1,2}$", "",
                  partes[-1].removesuffix(".parquet")))
    return "/".join([p for p in partes[:-1] if "=" in p] + [nome])


def completude(con, fs, bucket: str, tabela: str) -> list[str]:
    """Toda partição do bronze tem correspondente na camada auditada?"""
    esperado, obtido = defaultdict(set), defaultdict(set)
    for c in fs.glob(f"{BUCKET_BRONZE}/{tabela}/**/*.parquet"):
        if m := re.search(r"ano=(\d{4})", c):
            esperado[int(m.group(1))].add(c.split("/")[-1].removesuffix(".parquet"))
    for c in fs.glob(f"{bucket}/{tabela}/**/*.parquet"):
        if m := re.search(r"ano(?:_particao)?=(\d{4})", c):
            obtido[int(m.group(1))].add(_origem(c).split("/")[-1])

    problemas = []
    for ano in sorted(esperado):
        e, o = len(esperado[ano]), len(obtido.get(ano, set()))
        if o == e:
            continue
        faltantes = sorted(esperado[ano] - obtido.get(ano, set()))
        # Numa camada de TI, origem que não tem NENHUMA linha de tecnologia
        # legitimamente não gera arquivo. `caged_ajustes` em 2002 é o caso: o
        # bronze tem o arquivo, o recorte devolve zero linhas, e a silver não
        # grava nada. Acusar isso como falta transformaria a única verificação
        # de completude num alarme que se aprende a ignorar.
        if bucket == BUCKET_SILVER_TI:
            vazias = [n for n in faltantes if _sem_tecnologia(con, tabela, ano, n)]
            faltantes = [n for n in faltantes if n not in vazias]
            for n in vazias:
                print(f"      ℹ️  {tabela} {ano}: origem {n} não tem nenhuma linha "
                      f"de tecnologia — nada a gravar")
        if faltantes:
            problemas.append(f"{ano}: {len(faltantes)} origem(ns) sem saída "
                             f"({', '.join(faltantes[:3])})")
    return problemas


def _sem_tecnologia(con, tabela: str, ano: int, origem: str) -> bool:
    """
    A origem tem zero linha dentro do recorte de tecnologia?

    Reaproveita o MESMO predicado que o construtor usa. Reescrevê-lo aqui
    criaria duas definições de "o que é TI" que divergiriam na primeira
    mudança — e a auditoria passaria a medir outra coisa.
    """
    from gold_caged import escopo_tecnologia as esc

    try:
        alvo = (f"'s3://{BUCKET_BRONZE}/{tabela}/ano={ano}/**/{origem}.parquet'")
        colunas = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({alvo})").fetchall()]
        col_cnae, col_cbo = esc.colunas_da_tabela(tabela, colunas)
        predicado = esc.sql_filtro_tecnologia(
            f'"{col_cnae}"' if col_cnae else None,
            f'"{col_cbo}"' if col_cbo else None)
        if not predicado:
            return False
        n = con.execute(
            f"SELECT count(*) FROM read_parquet({alvo}) WHERE {predicado}").fetchone()[0]
        return n == 0
    except Exception:
        # Na dúvida, reporta como falta: falso alarme é melhor que dado sumido
        # sem aviso.
        return False


def _prefixo_particao(fs, bucket: str, tabela: str) -> str:
    """
    Qual esquema de partição esta tabela usa: `ano=` ou `ano_particao=`.

    Os dois convivem no lake: a silver de TI do CAGED foi construída espelhando
    o caminho do bronze (`ano=`), antes de existir a saída hive, enquanto o
    mercado completo e a RAIS usam `ano_particao=`. É uma inconsistência real
    do dado publicado, não da auditoria — mas ela precisa ler os dois para
    conseguir apontar as demais.
    """
    for c in fs.glob(f"{bucket}/{tabela}/**/*.parquet")[:1]:
        return "ano_particao" if "ano_particao=" in c else "ano"
    return "ano_particao"


def deriva_de_schema(con, bucket: str, tabela: str, prefixo: str = "ano_particao") -> list[str]:
    """
    A mesma coluna muda de tipo — ou some — entre anos?

    É a checagem que pega o estrago do layout de 2023. Um ano isolado parece
    perfeito; o problema só aparece comparando os anos entre si.
    """
    anos = con.execute(f"""
        SELECT DISTINCT regexp_extract(file, 'ano_particao=(\\d{{4}})', 1) AS ano
        FROM glob('s3://{bucket}/{tabela}/**/*.parquet') ORDER BY 1
    """).fetchall()
    anos = [a[0] for a in anos if a[0]]
    if len(anos) < 2:
        return []

    tipos_por_ano = {}
    for ano in anos:
        try:
            linhas = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet"
                f"('s3://{bucket}/{tabela}/{prefixo}={ano}/**/*.parquet') LIMIT 0"
            ).fetchall()
            tipos_por_ano[ano] = {r[0]: r[1] for r in linhas}
        except Exception as e:
            return [f"{ano}: não consegui ler o schema ({str(e)[:60]})"]

    # Só interessa coluna que EXISTE em mais de um ano com tipos divergentes,
    # ou que desaparece no meio da série (some no fim = layout novo, esperado).
    problemas = []
    todas = set().union(*tipos_por_ano.values())
    for col in sorted(todas):
        presentes = [a for a in anos if col in tipos_por_ano[a]]
        tipos = {tipos_por_ano[a][col] for a in presentes}
        if len(tipos) > 1:
            amostra = ", ".join(f"{a}={tipos_por_ano[a][col]}" for a in presentes[:2])
            problemas.append(f"{col}: tipos divergentes ({amostra}, …)")
        elif 1 < len(presentes) < len(anos):
            # Coluna que aparece num bloco CONTÍNUO de anos foi introduzida ou
            # aposentada — é a vida normal de um layout que muda. O defeito é a
            # coluna que some e VOLTA: aí faltou construir alguma coisa.
            #
            # A regra antiga perdoava só quando o primeiro ano faltante era uma
            # ponta, e por isso acusava as colunas que o MTE aposentou em 2023
            # (`ind_simples`, `cnae_20_classe`) de "buraco no meio" — três anos
            # de cauda, não um. Sete falsos positivos por tabela transformam a
            # auditoria em ruído, que é como um achado real passa despercebido.
            posicoes = [anos.index(a) for a in presentes]
            contiguo = posicoes == list(range(posicoes[0], posicoes[-1] + 1))
            if not contiguo:
                buracos = [anos[i] for i in range(posicoes[0], posicoes[-1])
                           if i not in posicoes]
                problemas.append(f"{col}: some e volta — ausente em {buracos[:3]} "
                                 f"entre {presentes[0]} e {presentes[-1]}")
    return problemas


def traducao_vazia(con, bucket: str, tabela: str, prefixo: str = "ano_particao") -> list[str]:
    """Ano com coluna traduzida mas descrição 100% nula, ou sem tradução nenhuma."""
    anos = con.execute(f"""
        SELECT DISTINCT regexp_extract(file, 'ano_particao=(\\d{{4}})', 1) AS ano
        FROM glob('s3://{bucket}/{tabela}/**/*.parquet') ORDER BY 1
    """).fetchall()
    problemas = []
    for (ano,) in anos:
        if not ano:
            continue
        q = f"read_parquet('s3://{bucket}/{tabela}/{prefixo}={ano}/**/*.parquet')"
        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {q}").fetchall()]
        desc = [c for c in cols if c.endswith("_descricao")]
        if not desc:
            problemas.append(f"{ano}: NENHUMA coluna traduzida")
            continue
        sel = ", ".join(f'count("{d}")' for d in desc)
        r = con.execute(f"SELECT {sel} FROM {q}").fetchone()
        vazias = [d for d, n in zip(desc, r) if n == 0]

        # Descrição nula tem duas causas MUITO diferentes, e tratá-las igual
        # torna a auditoria inútil:
        #
        #   - a origem só traz o marcador de "não informado" — CNAE 1.0 em 2007
        #     é sempre '000-1', bairro em 2023+ é sempre '999997', aprendiz em
        #     2003 é sempre '9'. Não há tradução possível nem o que consertar.
        #   - a origem traz códigos de verdade e o join falhou — aí sim é
        #     defeito nosso, e foi o que aconteceu com as 33 colunas de 2023.
        #
        # O que separa os dois é a VARIEDADE na coluna de código: um único
        # valor distinto é marcador; vários são dado que deixou de ser traduzido.
        reais, marcadores = [], []
        for d in vazias:
            origem = d.removesuffix("_descricao")
            if origem not in cols:
                reais.append(d)
                continue
            distintos = con.execute(
                f'SELECT count(DISTINCT "{origem}") FROM {q}').fetchone()[0]
            (marcadores if distintos <= 1 else reais).append(d)

        if reais:
            problemas.append(f"{ano}: {len(reais)} descrição(ões) 100% nula(s) "
                             f"com código variado ({', '.join(reais[:3])})")
        if marcadores:
            print(f"      ℹ️  {tabela} {ano}: {len(marcadores)} coluna(s) sem tradução "
                  f"porque a origem só tem marcador de não informado "
                  f"({', '.join(marcadores[:3])})")
    return problemas


def main() -> int:
    p = argparse.ArgumentParser(description="Audita consistência das camadas.")
    p.add_argument("--bucket", default=BUCKET_SILVER_TI,
                   choices=(BUCKET_SILVER_TI, BUCKET_SILVER))
    p.add_argument("--tabela", nargs="+", choices=TABELAS, default=list(TABELAS))
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=2")
    fs = _fs()

    print(f"🔍 Auditoria de {args.bucket}\n")
    total = 0
    for tabela in args.tabela:
        if not fs.glob(f"{args.bucket}/{tabela}/**/*.parquet"):
            continue
        prefixo = _prefixo_particao(fs, args.bucket, tabela)
        achados = []
        for rotulo, fn in (("completude", lambda: completude(con, fs, args.bucket, tabela)),
                           ("schema", lambda: deriva_de_schema(con, args.bucket, tabela, prefixo)),
                           ("tradução", lambda: traducao_vazia(con, args.bucket, tabela, prefixo))):
            try:
                for msg in fn():
                    achados.append(f"      [{rotulo}] {msg}")
            except Exception as e:
                achados.append(f"      [{rotulo}] erro na checagem: {str(e)[:80]}")

        total += len(achados)
        print(f"   {'❌' if achados else '✅'} {tabela}"
              f"{'' if achados else '  sem inconsistências'}")
        for a in achados[:12]:
            print(a)
        if len(achados) > 12:
            print(f"      … mais {len(achados) - 12}")

    print(f"\n🏁 {total} inconsistência(s)")
    return 0 if total == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
