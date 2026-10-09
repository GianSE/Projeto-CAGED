"""
Configuração derivada da extração FTP — árvore de diretórios de staging.
A conexão DuckDB (com autenticação do Hub e ajustes de memória) mora em
`_utils/nuvem.py::conectar_duckdb()`, compartilhada com silver e gold.
"""
import os

from _settings.config import FTP_CONFIG, PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL, STAGING_DIR  # noqa: F401

FTP_HOST = FTP_CONFIG["host"]
FTP_BASE = FTP_CONFIG["base"]
FTP_TIMEOUT = FTP_CONFIG["timeout"]
FTP_MAX_RETRIES = FTP_CONFIG["max_retries"]
FTP_ENCODING = FTP_CONFIG["encoding"]

DIR_DOWNLOAD = STAGING_DIR / "download"
DIR_EXTRAIDO = STAGING_DIR / "extraido"
DIR_LOGS = STAGING_DIR / "logs"

PARQUET_ROW_GROUP_SIZE = int(os.getenv("PARQUET_ROW_GROUP_SIZE", "250000"))


def preparar_diretorios() -> None:
    """Cria a árvore de staging local, se ainda não existir."""
    for d in (DIR_DOWNLOAD, DIR_EXTRAIDO, DIR_LOGS):
        d.mkdir(parents=True, exist_ok=True)
