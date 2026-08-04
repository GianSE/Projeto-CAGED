# Arquivo: config.py
"""
Configuração central do projeto.

Arquitetura: lakehouse 100% parquet sobre MinIO (S3), sem banco relacional.
Camadas -> bronze (bruto, tudo texto) / silver (limpo e tipado) / gold (agregados).
"""
import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv

# Carrega o .env da raiz do projeto (sobe a partir deste arquivo)
_RAIZ_PROJETO = Path(__file__).resolve().parents[2]
load_dotenv(_RAIZ_PROJETO / ".env")
load_dotenv()  # fallback: .env na pasta atual

# --- 1. DATA LAKE (MINIO) ---
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "region": os.getenv("MINIO_REGION", "us-east-1"),
}

BUCKET_BRONZE = os.getenv("BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("BUCKET_SILVER", "silver")
BUCKET_GOLD = os.getenv("BUCKET_GOLD", "gold")

# Opções no formato aceito por s3fs / polars storage_options
S3_STORAGE_OPTIONS = {
    "aws_access_key_id": MINIO_CONFIG["access_key"],
    "aws_secret_access_key": MINIO_CONFIG["secret_key"],
    "endpoint_url": f"http://{MINIO_CONFIG['endpoint']}",
    "aws_region": MINIO_CONFIG["region"],
}

# --- 2. EXTRAÇÃO FTP (MTE / PDET) ---
FTP_CONFIG = {
    "host": os.getenv("FTP_HOST", "ftp.mtps.gov.br"),
    "base": os.getenv("FTP_BASE", "/pdet/microdados"),
    "timeout": int(os.getenv("FTP_TIMEOUT", "180")),
    "max_retries": int(os.getenv("FTP_MAX_RETRIES", "5")),
    # O servidor do MTE devolve nomes de arquivo em latin-1, não UTF-8.
    "encoding": "latin-1",
}

# --- 3. STAGING (download + descompactação temporária) ---
STAGING_DIR = Path(os.getenv("STAGING_DIR") or tempfile.gettempdir())

# --- 4. PARQUET ---
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "zstd")
PARQUET_COMPRESSION_LEVEL = int(os.getenv("PARQUET_COMPRESSION_LEVEL", "3"))


# --- 5. FUNÇÕES UTILITÁRIAS DE AMBIENTE ---
def setup_minio_env():
    """Configura as variáveis de ambiente que DuckDB/Boto3 leem para acessar o S3/MinIO."""
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_CONFIG["access_key"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_CONFIG["secret_key"]
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    os.environ["AWS_DEFAULT_REGION"] = MINIO_CONFIG["region"]


def conectar_duckdb(memory_limit="4GB", temp_dir=None):
    """
    Devolve uma conexão DuckDB já apontada para o MinIO.

    Centraliza o boilerplate de httpfs + credenciais S3 que antes era repetido
    em cada script de ingestão.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_CONFIG["endpoint"]}';
        SET s3_access_key_id='{MINIO_CONFIG["access_key"]}';
        SET s3_secret_access_key='{MINIO_CONFIG["secret_key"]}';
        SET s3_region='{MINIO_CONFIG["region"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET memory_limit='{memory_limit}';
    """)
    if temp_dir:
        Path(temp_dir).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{str(temp_dir).replace(chr(92), '/')}';")
    return con


def get_temp_csv_caminho(filename="carga_temp.csv"):
    # Correção crítica para Windows e DuckDB
    temp_dir = tempfile.gettempdir()
    full_path = os.path.join(temp_dir, filename)
    return full_path.replace("\\", "/")
