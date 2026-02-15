import os
import duckdb
from pathlib import Path

# --- CONFIGURAÇÕES (Mesmas do seu script original) ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

PATH_BASE_RAIS = Path("/data/caged/RAIS")
OUTPUT_FILE = "arquivos_faltantes.txt"

# --- CONEXÃO DUCKDB (Apenas para listar o S3) ---
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

def get_existing_s3_files():
    """Retorna um SET com os caminhos completos dos arquivos parquet já existentes no MinIO"""
    print("🔍 Consultando MinIO para listar arquivos já processados...")
    try:
        # Usa o GLOB do DuckDB para listar tudo na pasta rais
        query = f"SELECT file FROM glob('s3://{MINIO_BUCKET}/rais/**/*.parquet')"
        result = con.execute(query).fetchall()
        
        # Cria um set para busca rápida (O(1))
        # Exemplo de item no set: 's3://bronze/rais/ano=2020/nome_arquivo.parquet'
        existing_files = {row[0] for row in result}
        print(f"✅ Encontrados {len(existing_files)} arquivos no Bucket '{MINIO_BUCKET}'.")
        return existing_files
    except Exception as e:
        print(f"⚠️ Erro ao listar S3 (pode estar vazio ou inacessível): {e}")
        return set()

def find_missing():
    existing_s3 = get_existing_s3_files()
    missing_files = []

    print(f"\n🚀 Verificando arquivos locais em: {PATH_BASE_RAIS}")
    
    # Percorre todos os anos (pastas) dentro do path base
    # Estou usando iterdir() para pegar qualquer ano que tiver lá, não apenas o range hardcoded
    if not PATH_BASE_RAIS.exists():
        print("❌ Diretório base não encontrado.")
        return

    # Ordena pastas para ficar bonitinho no log
    years = sorted([p for p in PATH_BASE_RAIS.iterdir() if p.is_dir()])

    for year_path in years:
        year = year_path.name
        
        # Pega .7z e .zip
        archives = list(year_path.glob("*.7z")) + list(year_path.glob("*.zip"))
        
        if not archives:
            continue

        print(f"📂 Verificando ano {year} ({len(archives)} arquivos)...")

        for archive_path in archives:
            # --- REPLICA A LÓGICA DE NOMEAÇÃO DO SEU SCRIPT ORIGINAL ---
            # 1. Nome base limpo
            source_name = archive_path.stem.lower().replace(" ", "_")
            target_name = f"{source_name}.parquet"
            
            # 2. Caminho esperado no S3
            # Nota: Seu script usa 'ano={year}' na estrutura
            expected_s3_path = f"s3://{MINIO_BUCKET}/rais/ano={year}/{target_name}"

            # 3. Verificação
            if expected_s3_path not in existing_s3:
                print(f"    ❌ Faltando: {archive_path.name}")
                missing_files.append(str(archive_path))

    # --- GERA O RELATÓRIO ---
    print(f"\n📊 Resumo:")
    print(f"   Total Faltante: {len(missing_files)}")
    
    if missing_files:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for item in missing_files:
                f.write(f"{item}\n")
        print(f"📝 Lista salva em: {os.path.abspath(OUTPUT_FILE)}")
    else:
        print("✨ Tudo parece estar carregado! Nenhum arquivo faltando.")

if __name__ == "__main__":
    find_missing()