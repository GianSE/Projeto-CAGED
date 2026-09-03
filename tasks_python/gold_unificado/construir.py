"""
Tabelas gold do mercado de tecnologia, unindo CAGED e RAIS.

POR QUE AGREGADAS, E NÃO MICRODADO
-----------------------------------
O dashboard roda no Streamlit Cloud e lê por HTTPS. Mandar ele varrer 12 GB de
CAGED mais 10 GB de RAIS a cada filtro seria inviável — e desnecessário: todo
gráfico do painel responde a uma pergunta agregada. Cada tabela aqui é o
resultado já somado de um recorte, com poucos MB.

POR QUE VÁRIAS TABELAS, E NÃO UM CUBO ÚNICO
-------------------------------------------
Um cubo com todas as dimensões cruzadas (ano × mês × UF × CBO × sexo × raça ×
escolaridade) explode em cardinalidade e fica maior que a soma das partes, com
a maioria das células vazia. Cada tabela abaixo cruza só as dimensões que a sua
pergunta precisa.

FLUXO E ESTOQUE NÃO SE SOMAM
----------------------------
O CAGED mede FLUXO (admissões e desligamentos do mês). A RAIS mede ESTOQUE
(vínculos ativos em 31/12). Somar os dois não significa nada. O que faz sentido
é usar um como denominador do outro — é o que `rotatividade` faz.

Uso:
    python -m gold_unificado.construir
    python -m gold_unificado.construir --so caged
"""
import argparse
import sys
import time
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_SILVER,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)
from gold_caged import escopo_tecnologia as esc
from gold_unificado.dicionario_canonico import sql_canonico

DIR_SAIDA = Path(__file__).resolve().parents[2] / "publicacao" / "gold"

# Colunas de cada base, para o recorte de TI e para o vocabulário canônico.
FONTES = {
    "caged_mov": {
        "cnae": "subclasse", "cbo": "cbo2002ocupacao", "uf": "uf_descricao",
        "sexo": "sexo_descricao", "raca": "racacor_descricao",
        "escol": "graudeinstrucao_descricao",
        "ano": "ano_particao", "mes": "mes_particao",
        "saldo": "saldomovimentacao", "salario": "salario",
    },
    "caged_old": {
        "cnae": "cnae_20_subclas", "cbo": "cbo_2002_ocupacao", "uf": "uf_descricao",
        "sexo": "sexo_descricao", "raca": "raca_cor_descricao",
        "escol": "grau_instrucao_descricao",
        "ano": "ano_particao", "mes": "mes_particao",
        "saldo": "saldo_mov", "salario": "salario_mensal",
    },
}

RAIS = {
    "cnae": "cnae_20_subclasse", "cbo": "cbo_ocupacao_2002", "uf": "municipio_descricao",
    "sexo": "sexo_trabalhador_descricao", "raca": "raca_cor_descricao",
    "escol": "escolaridade_apos_2005_descricao",
}


def _fonte(tabela: str) -> str:
    return f"s3://{BUCKET_SILVER}/{tabela}/**/*.parquet"


def _sql_caged(tabela: str) -> str:
    """SELECT do CAGED já recortado em TI e com o vocabulário canônico."""
    c = FONTES[tabela]
    filtro = esc.sql_filtro_tecnologia(f'"{c["cnae"]}"', f'"{c["cbo"]}"')
    return f"""
        SELECT
            {c['ano']}                                  AS ano,
            {c['mes']}                                  AS mes,
            "{c['uf']}"                                 AS uf,
            "{c['cbo']}"                                AS cbo,
            {esc.sql_area_ti(f'"{c["cbo"]}"')}          AS area_ti,
            CASE WHEN {esc.sql_filtro_cnae(f'"{c["cnae"]}"')} THEN true ELSE false END AS setor_ti,
            CASE WHEN {esc.sql_filtro_cbo(f'"{c["cbo"]}"')}  THEN true ELSE false END AS ocupacao_ti,
            {sql_canonico('sexo', f'"{c["sexo"]}"')}          AS sexo,
            {sql_canonico('raca_cor', f'"{c["raca"]}"')}      AS raca_cor,
            {sql_canonico('escolaridade', f'"{c["escol"]}"')} AS escolaridade,
            try_cast("{c['saldo']}" AS INTEGER)         AS saldo,
            try_cast("{c['salario']}" AS DOUBLE)        AS salario
        FROM read_parquet('{_fonte(tabela)}')
        WHERE {filtro}
    """


def _sql_rais() -> str:
    filtro = esc.sql_filtro_tecnologia(f'"{RAIS["cnae"]}"', f'"{RAIS["cbo"]}"')
    return f"""
        SELECT
            ano_particao                                AS ano,
            "{RAIS['cbo']}"                             AS cbo,
            {esc.sql_area_ti(f'"{RAIS["cbo"]}"')}       AS area_ti,
            CASE WHEN {esc.sql_filtro_cnae(f'"{RAIS["cnae"]}"')} THEN true ELSE false END AS setor_ti,
            CASE WHEN {esc.sql_filtro_cbo(f'"{RAIS["cbo"]}"')}  THEN true ELSE false END AS ocupacao_ti,
            {sql_canonico('sexo', f'"{RAIS["sexo"]}"')}          AS sexo,
            {sql_canonico('raca_cor', f'"{RAIS["raca"]}"')}      AS raca_cor,
            {sql_canonico('escolaridade', f'"{RAIS["escol"]}"')} AS escolaridade,
            try_cast(vinculo_ativo_3112 AS VARCHAR)     AS ativo_3112,
            try_cast(vl_remun_media_sm AS DOUBLE)       AS remun_sm,
            try_cast(tempo_emprego AS DOUBLE)           AS tempo_emprego
        FROM read_parquet('{_fonte('rais_vinc')}')
        WHERE {filtro}
    """


# (nome, SELECT). Cada uma responde a uma pergunta do dashboard.
def tabelas_caged() -> dict[str, str]:
    uniao = f"({_sql_caged('caged_mov')}) UNION ALL BY NAME ({_sql_caged('caged_old')})"
    return {
        # A série temporal: o gráfico de abertura.
        "fluxo_mensal": f"""
            SELECT ano, mes,
                   count(*) FILTER (WHERE saldo = 1)  AS admissoes,
                   count(*) FILTER (WHERE saldo = -1) AS desligamentos,
                   sum(saldo)                          AS saldo,
                   median(salario) FILTER (WHERE saldo = 1 AND salario > 0) AS salario_admissao
            FROM ({uniao}) GROUP BY 1, 2
        """,
        # Onde o trabalho acontece.
        "fluxo_uf": f"""
            SELECT ano, uf,
                   count(*) FILTER (WHERE saldo = 1) AS admissoes,
                   sum(saldo)                         AS saldo
            FROM ({uniao}) GROUP BY 1, 2
        """,
        # Quais áreas crescem, e a divisão setor x ocupação.
        "fluxo_area": f"""
            SELECT ano, area_ti, setor_ti, ocupacao_ti,
                   count(*) FILTER (WHERE saldo = 1) AS admissoes,
                   sum(saldo)                         AS saldo,
                   median(salario) FILTER (WHERE saldo = 1 AND salario > 0) AS salario_admissao
            FROM ({uniao}) GROUP BY 1, 2, 3, 4
        """,
        # Quem é contratado.
        "fluxo_perfil": f"""
            SELECT ano, sexo, raca_cor, escolaridade,
                   count(*) FILTER (WHERE saldo = 1) AS admissoes,
                   sum(saldo)                         AS saldo,
                   median(salario) FILTER (WHERE saldo = 1 AND salario > 0) AS salario_admissao
            FROM ({uniao}) GROUP BY 1, 2, 3, 4
        """,
    }


def tabelas_rais() -> dict[str, str]:
    base = _sql_rais()
    # O estoque é quem está ATIVO em 31/12 — a RAIS traz também os vínculos
    # encerrados no ano, e contá-los inflaria o estoque.
    ativo = "upper(trim(ativo_3112)) IN ('1', 'SIM')"
    return {
        "estoque_area": f"""
            SELECT ano, area_ti, setor_ti, ocupacao_ti,
                   count(*) FILTER (WHERE {ativo})                      AS vinculos,
                   median(remun_sm) FILTER (WHERE {ativo})              AS remun_sm_mediana,
                   median(tempo_emprego) FILTER (WHERE {ativo})         AS tempo_emprego_mediano
            FROM ({base}) GROUP BY 1, 2, 3, 4
        """,
        "estoque_perfil": f"""
            SELECT ano, sexo, raca_cor, escolaridade,
                   count(*) FILTER (WHERE {ativo})         AS vinculos,
                   median(remun_sm) FILTER (WHERE {ativo}) AS remun_sm_mediana
            FROM ({base}) GROUP BY 1, 2, 3, 4
        """,
        "estoque_ocupacao": f"""
            SELECT ano, cbo, area_ti,
                   count(*) FILTER (WHERE {ativo})                 AS vinculos,
                   median(remun_sm) FILTER (WHERE {ativo})         AS remun_sm_mediana,
                   median(tempo_emprego) FILTER (WHERE {ativo})    AS tempo_emprego_mediano
            FROM ({base}) GROUP BY 1, 2, 3
        """,
    }


def gravar(con, nome: str, sql: str) -> int:
    destino = DIR_SAIDA / f"{nome}.parquet"
    con.execute(f"""
        COPY ({sql}) TO '{destino.as_posix()}' (
            FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL});
    """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{destino.as_posix()}')").fetchone()[0]
    print(f"   ✅ {nome:<20} {n:>8,} linhas · {destino.stat().st_size / 1e6:6.2f} MB")
    return n


def main() -> int:
    p = argparse.ArgumentParser(description="Constrói as tabelas gold unificadas.")
    p.add_argument("--so", choices=("caged", "rais"), default=None)
    args = p.parse_args()

    DIR_SAIDA.mkdir(parents=True, exist_ok=True)
    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")

    alvos = {}
    if args.so in (None, "caged"):
        alvos |= tabelas_caged()
    if args.so in (None, "rais"):
        alvos |= tabelas_rais()

    inicio = time.time()
    print(f"🏗️  {len(alvos)} tabela(s) gold -> {DIR_SAIDA}\n")
    falhas = []
    for nome, sql in alvos.items():
        try:
            gravar(con, nome, sql)
        except Exception as e:
            falhas.append(nome)
            print(f"   ❌ {nome}: {str(e)[:200]}")

    print(f"\n🏁 {len(alvos) - len(falhas)}/{len(alvos)} em {(time.time() - inicio) / 60:.1f} min")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
