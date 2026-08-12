"""
Configuração derivada da extração FTP.

Os valores vêm todos de `_settings/config.py` (fonte única de verdade do
projeto). Aqui ficam só as coisas específicas da extração: a árvore de
diretórios de staging e a conexão DuckDB já ajustada para arquivos grandes.
"""
import os
from pathlib import Path

from _settings.config import (  # noqa: F401  (re-exportados de propósito)
    BUCKET_BRONZE,
    BUCKET_GOLD,
    BUCKET_SILVER,
    BUCKET_SILVER_TI,
    bucket_silver,
    FTP_CONFIG,
    MINIO_CONFIG,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    STAGING_DIR,
)

# --- FTP do Ministério do Trabalho e Emprego (PDET) ---
FTP_HOST = FTP_CONFIG["host"]
FTP_BASE = FTP_CONFIG["base"]
FTP_TIMEOUT = FTP_CONFIG["timeout"]
FTP_MAX_RETRIES = FTP_CONFIG["max_retries"]
FTP_ENCODING = FTP_CONFIG["encoding"]

# --- MinIO / S3 ---
MINIO_ENDPOINT = MINIO_CONFIG["endpoint"]
MINIO_ACCESS_KEY = MINIO_CONFIG["access_key"]
MINIO_SECRET_KEY = MINIO_CONFIG["secret_key"]
MINIO_REGION = MINIO_CONFIG["region"]

# --- Staging local (download + descompactação temporária) ---
DIR_DOWNLOAD = STAGING_DIR / "download"
DIR_EXTRAIDO = STAGING_DIR / "extraido"
DIR_SPILL = STAGING_DIR / "duckdb_spill"
DIR_LOGS = STAGING_DIR / "logs"

# --- Parquet ---
PARQUET_ROW_GROUP_SIZE = int(os.getenv("PARQUET_ROW_GROUP_SIZE", "250000"))

# --- DuckDB ---
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")
DUCKDB_THREADS = os.getenv("DUCKDB_THREADS")  # None = deixa o DuckDB decidir


def preparar_diretorios() -> None:
    """Cria a árvore de staging local, se ainda não existir."""
    for d in (DIR_DOWNLOAD, DIR_EXTRAIDO, DIR_SPILL, DIR_LOGS):
        d.mkdir(parents=True, exist_ok=True)


def conectar_duckdb():
    """
    Conexão DuckDB para a ingestão: credenciais do MinIO + ajustes para
    arquivos grandes (a RAIS passa de 10 GB descompactada).
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # CREATE SECRET é DDL e não aceita parâmetros vinculados (?), então os valores
    # entram interpolados — com aspas simples escapadas para não quebrar o SQL.
    def esc(v: str) -> str:
        return str(v).replace("'", "''")

    con.execute(
        f"""
        CREATE OR REPLACE SECRET secret_minio (
            TYPE S3,
            KEY_ID '{esc(MINIO_ACCESS_KEY)}',
            SECRET '{esc(MINIO_SECRET_KEY)}',
            REGION '{esc(MINIO_REGION)}',
            ENDPOINT '{esc(MINIO_ENDPOINT)}',
            URL_STYLE 'path',
            USE_SSL false
        );
        """
    )

    DIR_SPILL.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET temp_directory='{DIR_SPILL.as_posix()}';")
    # Arquivos gigantes cabem melhor na memória sem preservar a ordem original
    con.execute("SET preserve_insertion_order=false;")
    if DUCKDB_THREADS:
        con.execute(f"SET threads={int(DUCKDB_THREADS)};")

    return con
