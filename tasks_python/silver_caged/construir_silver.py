"""
Constrói a camada silver do CAGED a partir da bronze + dicionários.

Para cada tabela (caged_mov, caged_for, caged_exc, caged_old, caged_ajustes):

  1. Lê todo o bronze da tabela (glob com particionamento Hive).
  2. Para cada coluna codificada com dicionário disponível, faz LEFT JOIN e
     acrescenta uma coluna "<coluna>_descricao" com o texto legível — o código
     original é mantido, nada é substituído.
  3. Tipa os campos numéricos (vírgula decimal -> ponto, cast) e monta uma
     coluna de data a partir da competência AAAAMM.
  4. Grava em s3://silver/<tabela>, particionado por ano/mês, ZSTD-3.

Uso (a partir de tasks_python, com o .venv ativo):

    python -m silver_caged.construir_silver --listar
    python -m silver_caged.construir_silver --tabela caged_mov caged_for caged_exc
    python -m silver_caged.construir_silver --tabela caged_old --ano-inicio 2015
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
from silver_caged import mapeamento as mp
from silver_caged.dicionarios import criar_view, existe

# Colunas que nunca são candidatas a tradução/tipagem (linhagem e partição)
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
    # Sem hive_partitioning: nem toda tabela tem o mesmo esquema de pastas
    # (caged_ajustes mistura arquivo anual "ano=2002/arq.parquet" com mensal
    # "ano=2010/mes=1/arq.parquet" — hive_partitioning=1 exige partição
    # uniforme e quebra nessa mistura). ano_particao/mes_particao já vêm como
    # colunas de verdade em cada linha, gravadas na ingestão bronze; não
    # precisa reconstruir a partição a partir do caminho.
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


def _mapa_traducao(con, fs, tabela: str, colunas: list[str]) -> dict[str, dict]:
    """
    Devolve {coluna_bronze: spec} só para colunas com dicionário de fato
    disponível no MinIO. `spec` é um dict com pelo menos "namespace", "aba",
    "estilo" — e "coluna"/"campo" quando o estilo exigir (ver dicionarios.py).
    """
    geracao = mp.geracao(tabela)
    mapa = {}

    if geracao == "novo":
        for col in colunas:
            if col in COLUNAS_TECNICAS or col in mp.ABAS_NAO_TRADUZIVEIS:
                continue
            if existe(fs, mp.BUCKET_SILVER_NAMESPACE_NOVO, col):
                mapa[col] = {
                    "namespace": mp.BUCKET_SILVER_NAMESPACE_NOVO,
                    "aba": col,
                    "estilo": "2col",
                }
    else:
        for col, spec in mp.MAPA_CAGED_ANTIGO.items():
            if col not in colunas:
                continue
            if not existe(fs, mp.BUCKET_DICT_NAMESPACE_ANTIGO, spec["aba"]):
                continue
            mapa[col] = {"namespace": mp.BUCKET_DICT_NAMESPACE_ANTIGO, **spec}

    return mapa


def _select_silver(con, fs, tabela: str, colunas: list[str]) -> str:
    geracao = mp.geracao(tabela)
    numericos = mp.NUMERICOS_NOVO_CAGED if geracao == "novo" else mp.NUMERICOS_CAGED_ANTIGO
    datas_aaaamm = mp.DATAS_AAAAMM_NOVO_CAGED if geracao == "novo" else mp.DATAS_AAAAMM_CAGED_ANTIGO
    numericos = {k: v for k, v in numericos.items() if k in colunas}
    datas_aaaamm = [c for c in datas_aaaamm if c in colunas]

    mapa = _mapa_traducao(con, fs, tabela, colunas)
    if mapa:
        print(f"   📖 {len(mapa)} coluna(s) com tradução: {', '.join(sorted(mapa))}")
    else:
        print("   ⚠️  Nenhuma coluna com dicionário disponível — silver sairá só tipada.")

    joins = []
    expressoes = []

    for col in colunas:
        if col in COLUNAS_TECNICAS:
            continue

        if col in numericos:
            tipo = numericos[col]
            expressoes.append(
                f'try_cast(replace(trim(b."{col}"), \',\', \'.\') AS {tipo}) AS "{col}"'
            )
        else:
            expressoes.append(f'b."{col}" AS "{col}"')

        if col in mapa:
            spec = dict(mapa[col])
            namespace, aba, estilo = spec.pop("namespace"), spec.pop("aba"), spec.pop("estilo")
            nome_view = f"dic_{tabela}_{col}"
            if criar_view(con, namespace, aba, estilo, nome_view, **spec):
                # O CAGED antigo grava código curto com zero à esquerda
                # ("02", "07"), mas o dicionário do layout traz o código sem
                # padding ("2", "7"). Casa por valor numérico quando os dois
                # lados são numéricos (tira o zero à esquerda e o sinal de
                # "-1" nessa comparação); cai para igualdade de texto quando
                # não são (códigos alfabéticos, como a seção do CNAE: A, B, C).
                joins.append(
                    f'LEFT JOIN {nome_view} AS "{nome_view}" ON '
                    f'(try_cast(trim(b."{col}") AS BIGINT) IS NOT NULL '
                    f'  AND try_cast(trim(b."{col}") AS BIGINT) = try_cast("{nome_view}".codigo AS BIGINT)) '
                    f'OR trim(b."{col}") = "{nome_view}".codigo'
                )
                expressoes.append(f'"{nome_view}".descricao AS "{col}_descricao"')

        if col in datas_aaaamm:
            expressoes.append(
                f'try_strptime(trim(b."{col}"), \'%Y%m\')::DATE AS "{col}_data"'
            )

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


def construir(con, fs, tabela: str, ano_inicio: int, ano_fim: int) -> bool:
    print(f"\n{'=' * 70}\n  🔨 SILVER: {tabela}\n{'=' * 70}")
    inicio = time.time()

    colunas = _colunas_bronze(con, tabela)
    if not colunas:
        print("   ⏭️  Sem dados em bronze para esta tabela, pulando.")
        return False

    query = _select_silver(con, fs, tabela, colunas)
    if ano_inicio or ano_fim:
        query += f" WHERE ano_particao BETWEEN {ano_inicio} AND {ano_fim}"

    destino = f"s3://{BUCKET_SILVER}/{tabela}"
    print(f"   📤 Gravando -> {destino}  (partição ano/mês)")

    try:
        con.execute(f"""
            COPY ({query}) TO '{destino}' (
                FORMAT PARQUET,
                PARTITION_BY (ano_particao, mes_particao),
                COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
                OVERWRITE_OR_IGNORE true
            );
        """)
    except Exception as e:
        print(f"   ❌ Falha ao construir a silver de {tabela}: {str(e)[:300]}")
        return False

    linhas = con.execute(
        f"SELECT count(*) FROM read_parquet('{destino}/**/*.parquet')"
    ).fetchone()[0]
    print(f"   ✅ {linhas:,} linhas em {time.time() - inicio:.0f}s")
    return True


def _argumentos():
    p = argparse.ArgumentParser(description="Constrói a camada silver do CAGED.")
    p.add_argument("--tabela", nargs="+", choices=mp.TODAS_TABELAS, default=list(mp.TODAS_TABELAS))
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
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
            mapa = _mapa_traducao(con, fs, tabela, colunas)
            print(f"\n{tabela} ({len(colunas)} colunas, {len(mapa)} traduzíveis):")
            for col in colunas:
                if col in mapa:
                    spec = mapa[col]
                    extra = spec.get("campo") or spec.get("coluna") or ""
                    sufixo = f" [{extra}]" if extra else ""
                    print(f"   ✅ {col}  ->  {spec['namespace']}/{spec['aba']} ({spec['estilo']}){sufixo}")
        return 0

    sucesso = 0
    for tabela in args.tabela:
        if construir(con, fs, tabela, args.ano_inicio, args.ano_fim):
            sucesso += 1

    print(f"\n🏁 {sucesso}/{len(args.tabela)} tabela(s) construída(s) na silver.")
    return 0 if sucesso == len(args.tabela) else 2


if __name__ == "__main__":
    sys.exit(main())
