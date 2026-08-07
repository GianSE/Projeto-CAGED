"""
Consultas do dashboard — DuckDB lendo a silver direto.

POR QUE SEM CAMADA GOLD
-----------------------
A silver é recortada em tecnologia: ~4,5 milhões de movimentações, não os 725
milhões do mercado inteiro. Nessa escala o DuckDB agrega em menos de um
segundo, então uma camada intermediária de agregados só acrescentaria um
passo de build e mais uma cópia para manter sincronizada.

As agregações abaixo fazem o papel da gold, e o cache do Streamlit as
materializa em memória na primeira execução — o efeito prático é o mesmo,
sem bucket extra.

Se a silver voltar a crescer muito (mercado completo, ou RAIS inteira), o
caminho é reintroduzir a gold: as consultas daqui viram os agregados de lá
praticamente sem alteração.
"""
import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from extracao_ftp.config_extracao import (
    BUCKET_SILVER,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
)

# Origem dos dados detalhados, resolvida por ambiente:
#   DADOS_URL_BASE definido  -> parquet consolidado servido por HTTPS
#                               (Supabase Storage, R2, HF — o que estiver
#                               publicado). O DuckDB usa range request e baixa
#                               só os row groups que a consulta precisa.
#   sem a variável           -> silver particionada no MinIO local
#
# O glob (**/*.parquet) só existe no caminho local: sobre HTTPS não há
# listagem de diretório, por isso a versão publicada é UM arquivo por tabela
# (ver gold_caged/consolidar.py).
URL_BASE = os.getenv("DADOS_URL_BASE", "").rstrip("/")
FONTE = (f"{URL_BASE}/caged_mov.parquet" if URL_BASE
         else f"s3://{BUCKET_SILVER}/caged_mov/**/*.parquet")

# saldomovimentacao vale +1 na admissão e -1 no desligamento: é a definição
# oficial do saldo do CAGED (geração líquida de emprego formal).
# O salário médio considera só admissões com valor informado — no
# desligamento o salário reflete o histórico do vínculo, não o mercado atual,
# e os zeros (sem informação) afundariam a média.
METRICAS = """
    count(*) FILTER (WHERE saldomovimentacao = 1)  AS admissoes,
    count(*) FILTER (WHERE saldomovimentacao = -1) AS desligamentos,
    sum(saldomovimentacao)                          AS saldo,
    round(avg(CASE WHEN saldomovimentacao = 1 AND salario > 0
                   THEN salario END), 2)            AS salario_medio,
    round(avg(CASE WHEN saldomovimentacao = 1 THEN idade END), 1) AS idade_media
"""


# Agregados publicados (gerados por gold_caged/publicar.py). Existem para o
# app rodar onde NÃO há MinIO — Streamlit Cloud, por exemplo: são poucas
# centenas de KB e vão versionados no próprio repositório.
DIR_PUBLICADO = Path(__file__).resolve().parents[2] / "dados_publicados"


@st.cache_resource
def conectar():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_region='{MINIO_REGION}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con


@st.cache_resource
def _modo_publicado() -> bool:
    """
    Decide de onde vêm os dados, testando a fonte detalhada uma única vez.

    Se ela responde (MinIO local ou URL publicada), usa-a — é o caminho que
    permite filtro arbitrário. Se não responde e existem agregados no repo,
    cai para eles: o app continua de pé, só com as dimensões pré-calculadas.
    """
    try:
        conectar().execute(f"SELECT 1 FROM read_parquet('{FONTE}') LIMIT 1").fetchone()
        return False
    except Exception:
        return DIR_PUBLICADO.exists()


@st.cache_data(ttl=900, show_spinner="Consultando os dados…")
def _consultar(sql: str) -> pd.DataFrame:
    try:
        return conectar().execute(sql).df()
    except Exception as e:
        st.error(f"Falha na consulta: {str(e)[:300]}")
        return pd.DataFrame()


@st.cache_data(ttl=900, show_spinner=False)
def _publicado(nome: str) -> pd.DataFrame:
    caminho = DIR_PUBLICADO / f"{nome}.parquet"
    if not caminho.exists():
        return pd.DataFrame()
    return duckdb.sql(f"SELECT * FROM read_parquet('{caminho.as_posix()}')").df()


def tem_dados() -> bool:
    if _modo_publicado():
        return not _publicado("mensal").empty
    df = _consultar(f"SELECT count(*) AS n FROM read_parquet('{FONTE}')")
    return not df.empty and df["n"].iloc[0] > 0


def _sql_lentes() -> str:
    """
    Cruzamento das duas lentes do recorte.

    Responde quanto do trabalho de TI acontece FORA das empresas de
    tecnologia — o desenvolvedor do banco, da rede de varejo, do hospital.
    Classificar em SQL (e não em Python) mantém a lógica junto da definição
    do recorte e evita trazer as linhas cruas para a memória.
    """
    from gold_caged import escopo_tecnologia as esc

    return f"""
        SELECT ano_particao AS ano,
               CASE
                 WHEN {esc.sql_filtro_cnae()} AND {esc.sql_filtro_cbo()}
                   THEN 'Profissional de TI em empresa de TI'
                 WHEN {esc.sql_filtro_cbo()}
                   THEN 'Profissional de TI fora do setor de TI'
                 ELSE 'Outra ocupação em empresa de TI'
               END AS categoria,
               secao_descricao AS setor_empresa,
               {METRICAS}
        FROM read_parquet('{FONTE}')
        GROUP BY 1, 2, 3 ORDER BY 1
    """


# nome do agregado publicado -> SQL equivalente sobre a silver. As duas
# fontes produzem exatamente as mesmas colunas: publicar.py usa estas mesmas
# consultas, então o que é publicado nunca diverge do que o app calcula.
def _sql(nome: str) -> str:
    return {
        "mensal": f"""
            SELECT competenciamov_data AS competencia, {METRICAS}
            FROM read_parquet('{FONTE}')
            WHERE competenciamov_data IS NOT NULL GROUP BY 1 ORDER BY 1
        """,
        "mensal_uf": f"""
            SELECT competenciamov_data AS competencia, uf_descricao AS uf,
                   regiao_descricao AS regiao, {METRICAS}
            FROM read_parquet('{FONTE}')
            WHERE competenciamov_data IS NOT NULL AND uf_descricao IS NOT NULL
            GROUP BY 1, 2, 3 ORDER BY 1
        """,
        "setor": f"""
            SELECT ano_particao AS ano, secao_descricao AS setor, {METRICAS}
            FROM read_parquet('{FONTE}')
            WHERE secao_descricao IS NOT NULL GROUP BY 1, 2 ORDER BY 1
        """,
        "ocupacao": f"""
            SELECT ano_particao AS ano, cbo2002ocupacao_descricao AS ocupacao, {METRICAS}
            FROM read_parquet('{FONTE}')
            WHERE cbo2002ocupacao_descricao IS NOT NULL
            GROUP BY 1, 2 HAVING count(*) >= 50 ORDER BY 1
        """,
        "demografia": f"""
            SELECT ano_particao AS ano, sexo_descricao AS sexo,
                   racacor_descricao AS raca_cor,
                   graudeinstrucao_descricao AS escolaridade, {METRICAS}
            FROM read_parquet('{FONTE}') GROUP BY 1, 2, 3, 4 ORDER BY 1
        """,
        "lentes": _sql_lentes(),
    }[nome]


def _obter(nome: str) -> pd.DataFrame:
    """Lê do agregado publicado ou calcula na silver, conforme o ambiente."""
    if _modo_publicado():
        return _publicado(nome)
    return _consultar(_sql(nome))


def mensal() -> pd.DataFrame:
    return _obter("mensal")


def mensal_por_uf() -> pd.DataFrame:
    return _obter("mensal_uf")


def por_setor() -> pd.DataFrame:
    """Setor da EMPRESA que contrata — mostra onde o profissional de TI trabalha."""
    return _obter("setor")


def por_ocupacao() -> pd.DataFrame:
    return _obter("ocupacao")


def demografia() -> pd.DataFrame:
    return _obter("demografia")


def setor_ti_vs_ocupacao_ti() -> pd.DataFrame:
    return _obter("lentes")


def fonte_atual() -> str:
    """Rótulo da origem dos dados, para o rodapé do dashboard."""
    if _modo_publicado():
        return "agregados pré-calculados"
    return "parquet publicado (HTTPS)" if URL_BASE else "data lake local (MinIO)"
