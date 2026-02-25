import os
import subprocess
import duckdb
import gc
import shutil
import re  # Adicionado para extrair o mês do nome do arquivo
from pathlib import Path
from datetime import datetime

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

PATH_BASE_AJUSTES = Path("/data/caged/CAGED_AJUSTES") 
TEMP_EXTRACT_PATH = Path("/tmp/extract_ajustes")
LOG_FILE = Path("/app/tasks_python/bronze/erros_ingestao_ajustes.txt")

# --- CONEXÃO DUCKDB ---
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET memory_limit='3GB'; 
    SET temp_directory='/tmp/duckdb_spill_ajustes';
""")

def scrub_and_fix_file(filepath):
    temp_clean = filepath.with_name(filepath.name + ".clean")
    try:
        with open(filepath, 'r', encoding='latin-1', errors='replace') as f_in, \
             open(temp_clean, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                if '\0' in line or len(line.strip()) < 5:
                    continue
                f_out.write(line)
        os.remove(filepath)
        os.rename(temp_clean, filepath)
        return True
    except Exception as e:
        print(f"   ⚠️ Erro na limpeza de {filepath.name}: {e}")
        return False

def process_ajustes():
    if TEMP_EXTRACT_PATH.exists():
        for item in TEMP_EXTRACT_PATH.iterdir():
            try:
                if item.is_file(): item.unlink()
                elif item.is_dir(): shutil.rmtree(item)
            except: pass
    TEMP_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)

    print(f"\n🚀 INICIANDO INGESTÃO CAGED AJUSTES (2010-2020)")

    for year in range(2010, 2021):
        year_path = PATH_BASE_AJUSTES / str(year)
        if not year_path.exists():
            continue

        print(f"\n📂 --- ANO AJUSTE: {year} ---")

        archives = list(year_path.glob("*.7z")) + list(year_path.glob("*.zip"))

        for archive_path in archives:
            # --- LÓGICA DE EXTRAÇÃO DO MÊS ---
            # O padrão é CAGEDEST_MMYYYY.7z. Pegamos os dígitos do nome.
            # Ex: CAGEDEST_012015 -> mes_match será '01'
            numbers = re.findall(r'\d+', archive_path.stem)
            if numbers:
                # O nome costuma ser MMYYYY, então o mês são os 2 primeiros dígitos do número encontrado
                full_str = numbers[0]
                str_month = full_str[:2] if len(full_str) >= 6 else "00"
                int_month = int(str_month)
            else:
                str_month = "00"
                int_month = 0

            print(f"📦 Extraindo: {archive_path.name} (Mês Detectado: {str_month})")
            
            for f in TEMP_EXTRACT_PATH.glob("*"): 
                try: os.remove(f)
                except: pass

            subprocess.run(["7z", "e", str(archive_path), f"-o{str(TEMP_EXTRACT_PATH)}", "-y"], 
                           capture_output=True, check=False)

            candidates = list(TEMP_EXTRACT_PATH.glob("*.txt")) + list(TEMP_EXTRACT_PATH.glob("*.csv"))

            for local_file in candidates:
                if local_file.stat().st_size < 10240: continue 

                print(f"   📄 Processando: {local_file.name}")
                
                if scrub_and_fix_file(local_file):
                    # --- CAMINHO PARTICIONADO NO S3 ---
                    target_name = f"{archive_path.stem.lower()}.parquet"
                    s3_path = f"s3://{MINIO_BUCKET}/caged_ajustes/ano={year}/mes={int_month}/{target_name}"

                    try:
                        print(f"   🔄 Enviando para MinIO...")
                        con.execute(f"""
                            COPY (
                                SELECT 
                                    *,
                                    {year}::INT as ano_particao,
                                    {int_month}::INT as mes_particao,
                                    '{archive_path.name}' as arquivo_fonte
                                FROM read_csv_auto(
                                    '{str(local_file)}',
                                    delim=';',
                                    header=True,
                                    all_varchar=True,
                                    encoding='utf-8',
                                    ignore_errors=True,
                                    null_padding=True,
                                    quote='"'
                                )
                            ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
                        """)
                        print(f"   ✅ OK!")
                    except Exception as e:
                        print(f"   ❌ Erro DuckDB: {e}")
                    
                    if local_file.exists(): os.remove(local_file)
            
            gc.collect()

    print("\n✨ Processamento de Ajustes concluído!")

if __name__ == "__main__":
    process_ajustes()