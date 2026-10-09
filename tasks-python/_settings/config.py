"""
Configuração central do projeto — pipeline 100% em nuvem (GitHub Actions +
Hugging Face Hub). Os nomes de repositório ficam em `_utils/nuvem.py`; aqui
só o que é específico da extração do FTP e do formato dos arquivos.
"""
import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv

_RAIZ_PROJETO = Path(__file__).resolve().parents[2]
load_dotenv(_RAIZ_PROJETO / ".env")
load_dotenv()  # fallback: .env na pasta atual

# --- Extração FTP (MTE / PDET) ---
FTP_CONFIG = {
    "host": os.getenv("FTP_HOST", "ftp.mtps.gov.br"),
    "base": os.getenv("FTP_BASE", "/pdet/microdados"),
    "timeout": int(os.getenv("FTP_TIMEOUT", "180")),
    "max_retries": int(os.getenv("FTP_MAX_RETRIES", "5")),
    # O servidor do MTE devolve nomes de arquivo em latin-1, não UTF-8.
    "encoding": "latin-1",
}

# --- Staging (download + descompactação temporária) ---
STAGING_DIR = Path(os.getenv("STAGING_DIR") or tempfile.gettempdir())

# --- Parquet ---
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "zstd")
PARQUET_COMPRESSION_LEVEL = int(os.getenv("PARQUET_COMPRESSION_LEVEL", "3"))
