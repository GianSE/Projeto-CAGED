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


def completude(fs, bucket: str, tabela: str) -> list[str]:
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
        if o != e:
            problemas.append(f"{ano}: {o} de {e} origens")
    return problemas


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
            faltam = [a for a in anos if a not in presentes]
            if faltam[0] not in (anos[0], anos[-1]):
                problemas.append(f"{col}: ausente em {faltam[:3]} (buraco no meio)")
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
        if vazias:
            problemas.append(f"{ano}: {len(vazias)} descrição(ões) 100% nula(s) "
                             f"({', '.join(vazias[:3])})")
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
        for rotulo, fn in (("completude", lambda: completude(fs, args.bucket, tabela)),
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
