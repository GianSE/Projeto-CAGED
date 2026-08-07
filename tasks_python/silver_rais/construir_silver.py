"""
Constrói a camada silver da RAIS a partir da bronze + dicionários.

Mesmo princípio do silver_caged: LEFT JOIN nos dicionários (coluna nova
"<coluna>_descricao", código original preservado), tipagem de numéricos e
datas quando configurado em mapeamento.py.

Diferença de particionamento: a RAIS não tem competência mensal como o CAGED
— cada arquivo é ano inteiro (rais_estab) ou ano+UF/região (rais_vinc), então
a silver particiona só por ano_particao (ver catalogo.py: ItemTrabalho nunca
grava mes_particao para RAIS).

Uso (a partir de tasks_python, com o .venv ativo):

    python -m silver_rais.construir_silver --listar
    python -m silver_rais.construir_silver --tabela rais_estab rais_vinc
"""
import argparse
import sys
import time

from extracao_ftp.config_extracao import (
    BUCKET_BRONZE,
    BUCKET_SILVER,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)
from silver_caged.dicionarios import chave_normalizada, criar_view, existe
from silver_rais import mapeamento as mp

COLUNAS_TECNICAS = {
    "ano_particao", "mes_particao", "recorte_particao",
    "arquivo_fonte", "caminho_fonte", "data_ingestao", "ano", "mes",
}


def _fs_minio():
    import s3fs

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}", "region_name": MINIO_REGION},
    )


def _colunas_bronze(con, tabela: str) -> list[str]:
    caminho = f"s3://{BUCKET_BRONZE}/{tabela}/**/*.parquet"
    try:
        return [
            r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{caminho}') LIMIT 0"
            ).fetchall()
        ]
    except Exception as e:
        print(f"   ⚠️  Não consegui ler o schema de {tabela}: {str(e)[:200]}")
        return []


def _mapa_traducao(fs, colunas: list[str]) -> dict[str, dict]:
    """{coluna_bronze: spec} — casamento automático por nome + mapa manual conferido."""
    mapa = {}

    for col in colunas:
        if col in COLUNAS_TECNICAS or col in mp.ABAS_NAO_TRADUZIVEIS:
            continue
        if existe(fs, mp.NAMESPACE_DICIONARIO, col):
            mapa[col] = {"namespace": mp.NAMESPACE_DICIONARIO, "aba": col, "estilo": "colon"}

    for col, spec in mp.MAPA_MANUAL.items():
        if col in colunas and existe(fs, mp.NAMESPACE_DICIONARIO, spec["aba"]):
            mapa[col] = {"namespace": mp.NAMESPACE_DICIONARIO, **spec}

    return mapa


def _select_silver(fs, con, tabela: str, colunas: list[str]) -> str:
    numericos = {k: v for k, v in mp.NUMERICOS.items() if k in colunas}
    datas_aaaamm = [c for c in mp.DATAS_AAAAMM if c in colunas]
    mapa = _mapa_traducao(fs, colunas)

    if mapa:
        print(f"   📖 {len(mapa)} coluna(s) com tradução: {', '.join(sorted(mapa))}")
    else:
        print("   ⚠️  Nenhuma coluna casou com o dicionário automaticamente. "
              "Rode --listar e confira mapeamento.py (MAPA_MANUAL).")

    joins, expressoes = [], []

    for col in colunas:
        if col in COLUNAS_TECNICAS:
            continue

        if col in numericos:
            tipo = numericos[col]
            expressoes.append(f'try_cast(replace(trim(b."{col}"), \',\', \'.\') AS {tipo}) AS "{col}"')
        else:
            expressoes.append(f'b."{col}" AS "{col}"')

        if col in mapa:
            spec = dict(mapa[col])
            namespace, aba, estilo = spec.pop("namespace"), spec.pop("aba"), spec.pop("estilo")
            nome_view = f"dic_{tabela}_{col}"
            if criar_view(con, namespace, aba, estilo, nome_view, **spec):
                chave_fato = chave_normalizada(f'b."{col}"')
                joins.append(
                    f'LEFT JOIN {nome_view} AS "{nome_view}" '
                    f'ON {chave_fato} = "{nome_view}".codigo_norm'
                )
                expressoes.append(f'"{nome_view}".descricao AS "{col}_descricao"')

        if col in datas_aaaamm:
            expressoes.append(f'try_strptime(trim(b."{col}"), \'%Y%m\')::DATE AS "{col}_data"')

    for col in COLUNAS_TECNICAS & set(colunas):
        expressoes.append(f'b."{col}" AS "{col}"')

    select = ",\n            ".join(expressoes)
    join_sql = "\n            ".join(joins)
    caminho_bronze = f"s3://{BUCKET_BRONZE}/{tabela}/**/*.parquet"

    return f"""
        SELECT
            {select}
        FROM read_parquet('{caminho_bronze}') AS b
        {join_sql}
    """


def construir(con, fs, tabela: str) -> bool:
    print(f"\n{'=' * 70}\n  🔨 SILVER: {tabela}\n{'=' * 70}")
    inicio = time.time()

    colunas = _colunas_bronze(con, tabela)
    if not colunas:
        print("   ⏭️  Sem dados em bronze para esta tabela, pulando.")
        return False

    query = _select_silver(fs, con, tabela, colunas)
    destino = f"s3://{BUCKET_SILVER}/{tabela}"
    print(f"   📤 Gravando -> {destino}  (partição ano)")

    try:
        con.execute(f"""
            COPY ({query}) TO '{destino}' (
                FORMAT PARQUET,
                PARTITION_BY (ano_particao),
                COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
                OVERWRITE_OR_IGNORE true
            );
        """)
    except Exception as e:
        print(f"   ❌ Falha ao construir a silver de {tabela}: {str(e)[:300]}")
        return False

    linhas = con.execute(f"SELECT count(*) FROM read_parquet('{destino}/**/*.parquet')").fetchone()[0]
    print(f"   ✅ {linhas:,} linhas em {time.time() - inicio:.0f}s")
    return True


def _argumentos():
    p = argparse.ArgumentParser(description="Constrói a camada silver da RAIS.")
    p.add_argument("--tabela", nargs="+", choices=mp.TABELAS_RAIS, default=list(mp.TABELAS_RAIS))
    p.add_argument("--listar", action="store_true", help="Só mostra o mapeamento de tradução e sai")
    return p.parse_args()


def main() -> int:
    args = _argumentos()
    con = conectar_duckdb()
    fs = _fs_minio()

    if args.listar:
        for tabela in args.tabela:
            colunas = _colunas_bronze(con, tabela)
            if not colunas:
                print(f"{tabela}: sem dados em bronze ainda")
                continue
            mapa = _mapa_traducao(fs, colunas)
            print(f"\n{tabela} ({len(colunas)} colunas, {len(mapa)} traduzíveis):")
            for col in colunas:
                marca = "✅" if col in mapa else "  "
                extra = f" -> {mapa[col]['namespace']}/{mapa[col]['aba']}" if col in mapa else ""
                print(f"   {marca} {col}{extra}")
        return 0

    sucesso = 0
    for tabela in args.tabela:
        if construir(con, fs, tabela):
            sucesso += 1

    print(f"\n🏁 {sucesso}/{len(args.tabela)} tabela(s) construída(s) na silver.")
    return 0 if sucesso == len(args.tabela) else 2


if __name__ == "__main__":
    sys.exit(main())
