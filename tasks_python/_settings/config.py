# Arquivo: config.py
import os
import tempfile
from dotenv import load_dotenv

# 1. Carrega as variáveis do arquivo .env para a memória
# (Ele procura o arquivo .env na pasta atual ou nas pastas pai)
load_dotenv()

# --- 1. CONFIGURAÇÕES DO BANCO DE DADOS (MariaDB) ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "local_infile": True
}

# --- 2. CONFIGURAÇÕES DO MINIO (Data Lake) ---
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY"),
    "region": os.getenv("MINIO_REGION")
}

# String pronta para configurar o DuckDB
DUCKDB_SECRET_SQL = f"""
    CREATE SECRET secret_minio (
        TYPE S3, 
        KEY_ID '{MINIO_CONFIG['access_key']}', 
        SECRET '{MINIO_CONFIG['secret_key']}',
        REGION '{MINIO_CONFIG['region']}', 
        ENDPOINT '{MINIO_CONFIG['endpoint']}',
        URL_STYLE 'path', 
        USE_SSL false
    );
"""

# --- 3. FUNÇÕES UTILITÁRIAS DE AMBIENTE ---
def setup_minio_env():
    """Configura as variáveis de ambiente necessárias para o DuckDB/Boto3 lerem o S3/MinIO."""
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_CONFIG["access_key"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_CONFIG["secret_key"]
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    os.environ["AWS_DEFAULT_REGION"] = MINIO_CONFIG["region"]

def get_temp_csv_caminho(filename="carga_temp.csv"):
    # Correção crítica para Windows e DuckDB/MariaDB
    temp_dir = tempfile.gettempdir()
    full_path = os.path.join(temp_dir, filename)
    return full_path.replace("\\", "/")