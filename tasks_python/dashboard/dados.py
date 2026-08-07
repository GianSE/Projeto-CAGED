"""
Acesso à camada gold via DuckDB.

Toda leitura passa por aqui e é cacheada pelo Streamlit: os agregados da gold
são pequenos (milhares de linhas), então cabem em memória e o dashboard
responde a filtros sem nova ida ao MinIO. É esse cache — somado à
pré-agregação da gold — que faz a diferença entre um clique instantâneo e um
clique que varre 725 milhões de linhas na silver.
"""
import duckdb
import pandas as pd
import streamlit as st

from extracao_ftp.config_extracao import (
    BUCKET_GOLD,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
)


@st.cache_resource
def conectar():
    """Conexão DuckDB apontada para o MinIO (uma por sessão do Streamlit)."""
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


@st.cache_data(ttl=600, show_spinner=False)
def carregar(nome: str) -> pd.DataFrame:
    """Lê um agregado da gold inteiro para memória. Vazio se ainda não existir."""
    try:
        return conectar().execute(
            f"SELECT * FROM read_parquet('s3://{BUCKET_GOLD}/{nome}.parquet')"
        ).df()
    except Exception:
        return pd.DataFrame()


def agregados_disponiveis() -> dict[str, bool]:
    """Quais agregados já foram construídos — o dashboard degrada em vez de quebrar."""
    nomes = ["saldo_mensal", "saldo_uf", "saldo_setor",
             "perfil_demografico", "ocupacoes", "saldo_municipio"]
    return {n: not carregar(n).empty for n in nomes}
