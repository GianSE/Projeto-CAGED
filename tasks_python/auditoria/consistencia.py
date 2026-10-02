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





def _fontes(con, arquivos: list[str]) -> dict[str, str]:
    """
    Caminho no FTP de cada origem presente, e um parquet que a contém.

    Lê uma coluna só, com valor constante por arquivo: o parquet guarda isso
    como dicionário, então o custo é praticamente o do rodapé.
    """
    if not arquivos:
        return {}
    lista = ", ".join(f"'s3://{c}'" for c in arquivos)
    linhas = con.execute(f"""
        SELECT DISTINCT caminho_fonte, filename
        FROM read_parquet([{lista}], union_by_name=true, filename=true,
                          hive_partitioning=false)
    """).fetchall()
    return {caminho: arq.removeprefix("s3://") for caminho, arq in linhas if caminho}


def completude(con, fs, bucket: str, tabela: str) -> list[str]:
    """
    Todo arquivo do FTP com linhas de tecnologia tem saída na camada?

    A identidade da origem é o CAMINHO NO FTP, que está em cada linha dos dois
    lados (`caminho_fonte`), e não o nome do arquivo: o nome muda quando a
    camada é reparticionada ou agrupada, e comparar nome com nome já acusou 28
    origens "sem saída" em caged_ajustes onde a contagem de linhas batia exata.
    """
    bronze = sorted(fs.glob(f"{BUCKET_BRONZE}/{tabela}/**/*.parquet"))
    camada = sorted(fs.glob(f"{bucket}/{tabela}/**/*.parquet"))
    if not bronze or not camada:
        return []

    esperado = _fontes(con, bronze)
    obtido = set(_fontes(con, camada))
    faltantes = sorted(set(esperado) - obtido)

    # Numa camada de TI, origem que não tem NENHUMA linha de tecnologia
    # legitimamente não gera saída. `caged_ajustes` em 2002 é o caso: o bronze
    # tem o arquivo, o recorte devolve zero linhas, e a silver não grava nada.
    # Acusar isso transformaria a única verificação de completude num alarme que
    # se aprende a ignorar.
    if bucket == BUCKET_SILVER_TI:
        vazias = [c for c in faltantes if _sem_tecnologia(con, tabela, esperado[c])]
        for c in vazias:
            print(f"      ℹ️  {tabela}: origem {c.split('/')[-1]} não tem nenhuma "
                  f"linha de tecnologia — nada a gravar")
        faltantes = [c for c in faltantes if c not in vazias]

    if not faltantes:
        return []

    # A mensagem agrupa por ano porque é assim que se procura o problema, mesmo
    # que a comparação em si não dependa de ano nenhum.
    por_ano = defaultdict(list)
    for caminho in faltantes:
        m = re.search(r"/((?:19|20)\d{2})/", caminho)
        por_ano[m.group(1) if m else "?"].append(caminho.split("/")[-1])
    return [f"{ano}: {len(nomes)} origem(ns) sem saída ({', '.join(sorted(nomes)[:3])})"
            for ano, nomes in sorted(por_ano.items())]



def pedacos_faltantes(con, fs, bucket: str, tabela: str) -> list[str]:
    """
    Todo pedaço de arquivo-fonte tem saída na camada?

    Arquivos grandes são processados em faixas de linhas ("pedaços"), e a
    completude por arquivo não enxerga isso: basta um pedaço chegar para o
    arquivo-fonte contar como presente. Esta verificação recalcula os pedaços
    esperados com a MESMA função que o construtor usa — reimplementá-la aqui
    criaria duas divisões que divergiriam na primeira mudança — e confronta com
    a procedência registrada na camada.

    Quando encontra ausência, conta no bronze quantas linhas do recorte havia
    naquela faixa: pedaço legitimamente sem nenhuma linha de tecnologia não gera
    saída, e acusá-lo seria alarme falso. Quando não encontra nada, não lê dado
    nenhum — só rodapés de metadados.
    """
    from silver_rais.construir_silver import _pedacos
    from silver_caged.construir_silver import origens_existentes

    presentes: dict[str, set[str]] = defaultdict(set)
    for ano, origens in origens_existentes(con, fs, bucket, tabela).items():
        if ano is not None:
            presentes[str(ano)] |= {re.sub(r"_parte90$", "", o) for o in origens}

    problemas = []
    for caminho in sorted(fs.glob(f"{BUCKET_BRONZE}/{tabela}/**/*.parquet")):
        m = re.search(r"ano=(\d{4})", caminho)
        if not m:
            continue
        ano, stem = m.group(1), caminho.split("/")[-1].removesuffix(".parquet")
        esperados = _pedacos(con, f"s3://{caminho}", stem)
        if len(esperados) == 1:
            continue                      # arquivo não dividido: já coberto acima
        for rotulo, faixa in esperados:
            if rotulo in presentes.get(ano, set()):
                continue
            n = _linhas_do_recorte(con, tabela, f"s3://{caminho}", faixa)
            if n > 0:
                problemas.append(f"{ano}: pedaço {rotulo} ausente "
                                 f"({n:,} linha(s) do recorte no bronze)")
    return problemas


def _linhas_do_recorte(con, tabela: str, origem_s3: str, faixa) -> int:
    """Quantas linhas daquela faixa do arquivo de origem estão no recorte."""
    from gold_caged import escopo_tecnologia as esc

    try:
        colunas = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{origem_s3}')").fetchall()]
        col_cnae, col_cbo = esc.colunas_da_tabela(tabela, colunas)
        predicado = esc.sql_filtro_tecnologia(
            f'"{col_cnae}"' if col_cnae else None,
            f'"{col_cbo}"' if col_cbo else None)
        if not predicado:
            return 0
        recorte = ""
        if faixa:
            inicio, fim = faixa
            recorte = (f" AND file_row_number >= {inicio} "
                       f"AND file_row_number < {fim}")
        return con.execute(
            f"SELECT count(*) FROM read_parquet('{origem_s3}', "
            f"file_row_number=true) WHERE {predicado}{recorte}").fetchone()[0]
    except Exception:
        # Na dúvida, reporta: falso alarme é melhor que dado sumido sem aviso.
        return 1


def _sem_tecnologia(con, tabela: str, arquivo_bronze: str) -> bool:
    """
    A origem tem zero linha dentro do recorte de tecnologia?

    Reaproveita o MESMO predicado que o construtor usa. Reescrevê-lo aqui
    criaria duas definições de "o que é TI" que divergiriam na primeira
    mudança — e a auditoria passaria a medir outra coisa.
    """
    from gold_caged import escopo_tecnologia as esc

    try:
        alvo = f"'s3://{arquivo_bronze}'"
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
        verificacoes = [
            ("completude", lambda: completude(con, fs, args.bucket, tabela)),
            ("schema", lambda: deriva_de_schema(con, args.bucket, tabela, prefixo)),
            ("tradução", lambda: traducao_vazia(con, args.bucket, tabela, prefixo)),
        ]
        # Só a RAIS é processada em pedaços; no CAGED cada arquivo do bronze sai
        # inteiro, e a completude por arquivo já responde.
        if tabela.startswith("rais"):
            verificacoes.append(
                ("pedaços", lambda: pedacos_faltantes(con, fs, args.bucket, tabela)))

        for rotulo, fn in verificacoes:
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
