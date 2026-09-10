"""
Agregados territoriais das DUAS bases, para o mapa do dashboard.

POR QUE JUNTAR AS DUAS NO MESMO MAPA
------------------------------------
Cada base pinta metade do quadro, e a metade que falta é sempre a que muda a
conclusão:

    RAIS  -> ESTOQUE. Onde o emprego de TI ESTÁ hoje.
    CAGED -> FLUXO.   Onde ele está SE MOVENDO agora.

São Paulo domina o estoque e vai dominar em qualquer mapa de nível — o que não
diz nada que já não se soubesse. O interessante é onde as duas leituras
DIVERGEM: um estado com estoque pequeno e fluxo forte está crescendo rápido; um
com estoque grande e fluxo negativo está encolhendo em silêncio. Nenhuma das
duas bases sozinha mostra isso.

A CHAVE COMUM
-------------
A RAIS não tem coluna de UF e o CAGED tem — mas as duas gravam
`municipio_descricao` no mesmo formato, com a sigla no prefixo ('Sp-Campinas',
'Es-Serra'). Derivar a UF do prefixo nas DUAS é o que garante que estão
falando da mesma coisa; usar `uf_descricao` num lado e o prefixo no outro
abriria espaço para divergência silenciosa em município com nome ambíguo.

Uso:
    python -m gold_unificado.mapa
"""
import argparse
import sys
import time

from extracao_ftp.config_extracao import (
    BUCKET_GOLD,
    BUCKET_SILVER_TI,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)
from gold_caged import escopo_tecnologia as esc

UF = "upper(split_part(municipio_descricao, '-', 1))"
MUNICIPIO = "municipio_descricao"
# As TRÊS tabelas (caged_mov, caged_old, rais_vinc) gravam o código IBGE de 6
# dígitos na mesma coluna. Levá-lo adiante é o que permite juntar com as
# coordenadas sem casar por nome — e nome de município é chave ruim: existem 21
# "Bom Jesus" no Brasil, e a base do MTE grava sem acento.
CODIGO = "municipio"

# O CAGED de tecnologia, nas duas gerações, com o território normalizado.
CAGED = f"""
    SELECT year(competenciamov_data) AS ano,
           {UF} AS uf, {MUNICIPIO} AS municipio, {CODIGO} AS cod_municipio,
           saldomovimentacao AS saldo
    FROM read_parquet('s3://{BUCKET_SILVER_TI}/caged_mov/**/*.parquet',
                      hive_partitioning=true)
    WHERE competenciamov_data IS NOT NULL AND municipio_descricao IS NOT NULL
    UNION ALL
    SELECT year(competencia_declarada_data), {UF}, {MUNICIPIO}, {CODIGO}, saldo_mov
    FROM read_parquet('s3://{BUCKET_SILVER_TI}/caged_old/**/*.parquet',
                      hive_partitioning=true)
    WHERE competencia_declarada_data IS NOT NULL AND municipio_descricao IS NOT NULL
"""

RAIS = f"""
    SELECT ano_particao AS ano, {UF} AS uf, {MUNICIPIO} AS municipio,
           {CODIGO} AS cod_municipio,
           vinculo_ativo_3112, vl_remun_media_sm,
           {esc.sql_classificacao('cnae_20_subclasse', 'cbo_ocupacao_2002')}
    FROM read_parquet('s3://{BUCKET_SILVER_TI}/rais_vinc/**/*.parquet',
                      hive_partitioning=true)
    WHERE municipio_descricao IS NOT NULL
"""

# Teto de plausibilidade da remuneração — ver gold_rais.construir_gold: a fonte
# inverte a conversão para salário mínimo em alguns registros a partir de 2023.
REMUN = ("CASE WHEN vinculo_ativo_3112 = '1' AND vl_remun_media_sm > 0 "
         "AND vl_remun_media_sm <= 500 THEN vl_remun_media_sm END")


def _sql(nivel: str) -> str:
    """
    O mesmo agregado em dois níveis territoriais.

    FULL OUTER JOIN de propósito: um município pode ter movimentação no CAGED
    sem estoque na RAIS naquele ano (empresa nova) ou o contrário (estoque
    parado, sem contratação). INNER perderia justamente esses casos, que são
    os mais informativos.
    """
    chave = "uf" if nivel == "uf" else "uf, municipio, cod_municipio"
    return f"""
        WITH fluxo AS (
            SELECT ano, {chave},
                   sum(saldo)                            AS saldo,
                   count(*) FILTER (WHERE saldo = 1)     AS admissoes,
                   count(*) FILTER (WHERE saldo = -1)    AS desligamentos
            FROM ({CAGED}) GROUP BY ALL
        ),
        estoque AS (
            SELECT ano, {chave},
                   count(*) FILTER (WHERE vinculo_ativo_3112 = '1') AS estoque,
                   round(avg({REMUN}), 2)                           AS remuneracao_sm,
                   round(median({REMUN}), 2)                        AS remuneracao_sm_mediana
            FROM ({RAIS}) WHERE setor_ti OR ocupacao_ti GROUP BY ALL
        )
        SELECT coalesce(f.ano, e.ano)                     AS ano,
               coalesce(f.uf, e.uf)                       AS uf,
               {"coalesce(f.municipio, e.municipio) AS municipio, coalesce(f.cod_municipio, e.cod_municipio) AS cod_municipio," if nivel != "uf" else ""}
               coalesce(e.estoque, 0)                     AS estoque,
               e.remuneracao_sm,
               e.remuneracao_sm_mediana,
               coalesce(f.saldo, 0)                       AS saldo,
               coalesce(f.admissoes, 0)                   AS admissoes,
               coalesce(f.desligamentos, 0)               AS desligamentos
        FROM fluxo f FULL OUTER JOIN estoque e
          ON f.ano = e.ano AND f.uf = e.uf
         {"AND f.cod_municipio = e.cod_municipio" if nivel != "uf" else ""}
        WHERE coalesce(f.uf, e.uf) IS NOT NULL
        ORDER BY 1, 2
    """


TABELAS = {"mapa_uf": "uf", "mapa_municipio": "municipio"}


def construir(con, nome: str) -> bool:
    destino = f"s3://{BUCKET_GOLD}/{nome}.parquet"
    print(f"\n🔨 {nome}")
    inicio = time.time()
    try:
        con.execute(f"""
            COPY ({_sql(TABELAS[nome])}) TO '{destino}' (
                FORMAT PARQUET,
                COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL}
            );
        """)
        linhas = con.execute(f"SELECT count(*) FROM read_parquet('{destino}')").fetchone()[0]
        print(f"   ✅ {linhas:,} linhas em {time.time() - inicio:.0f}s -> {destino}")
        return True
    except Exception as e:
        print(f"   ❌ falhou: {str(e)[:300]}")
        return False


def main() -> int:
    p = argparse.ArgumentParser(description="Agregados territoriais das duas bases.")
    p.add_argument("--tabela", nargs="+", choices=list(TABELAS), default=list(TABELAS))
    p.add_argument("--threads", type=int, default=4)
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute(f"SET threads={args.threads}")
    con.execute("SET enable_progress_bar=false")
    print("=" * 70)
    print("  GOLD — território: estoque (RAIS) e fluxo (CAGED) no mesmo lugar")
    print("=" * 70)

    ok = sum(construir(con, nome) for nome in args.tabela)
    print(f"\n🏁 {ok}/{len(args.tabela)} tabela(s) construída(s).")
    return 0 if ok == len(args.tabela) else 2


if __name__ == "__main__":
    sys.exit(main())
