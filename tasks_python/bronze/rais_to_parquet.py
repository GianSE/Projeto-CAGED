import os
import subprocess
import duckdb
import gc
import shutil
from pathlib import Path

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

PATH_BASE_RAIS = Path("/data/caged/RAIS") 
TEMP_EXTRACT_PATH = Path("/tmp/extract_rais")

# --- CONEXÃO DUCKDB ---
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET memory_limit='4GB'; 
    SET temp_directory='/tmp/duckdb_spill_rais';
""")

def scrub_and_fix_file(filepath):
    """ 
    Converte Latin-1 para UTF-8, remove bytes nulos e
    faz o TRIM manual de espaços em cada campo da linha.
    """
    print(f"    🧹 Iniciando cirurgia no arquivo (Scrubbing): {filepath.name}")
    temp_clean = filepath.with_name(filepath.name + ".clean")
    
    lines_kept = 0
    lines_dropped = 0
    
    try:
        with open(filepath, 'r', encoding='latin-1', errors='replace') as f_in, \
             open(temp_clean, 'w', encoding='utf-8') as f_out:
            
            for line in f_in:
                # 1. Ignora linhas com Null Bytes
                if '\0' in line:
                    lines_dropped += 1
                    continue
                
                # 2. LIMPEZA DE ESPAÇOS (HEX 20)
                # Quebramos a linha, limpamos cada campo e juntamos de novo
                # Isso garante que o Hex 20 suma de todos os campos
                parts = line.split(';')
                clean_parts = [p.strip() for p in parts]
                clean_line = ";".join(clean_parts)
                
                # 3. Verifica conteúdo mínimo
                if len(clean_line) < 2:
                    lines_dropped += 1
                    continue

                f_out.write(clean_line + "\n")
                lines_kept += 1
        
        os.remove(filepath)
        os.rename(temp_clean, filepath)
        
        print(f"    ✨ Sucesso! Mantidas: {lines_kept} | Lixo: {lines_dropped}")
        return True
    except Exception as e:
        print(f"    ⚠️ Erro crítico na limpeza: {e}")
        return False

def process_rais():
    # Limpeza preventiva
    if TEMP_EXTRACT_PATH.exists():
        for item in TEMP_EXTRACT_PATH.iterdir():
            try:
                if item.is_file(): item.unlink()
                elif item.is_dir(): shutil.rmtree(item)
            except: pass
    TEMP_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)

    print(f"\n🚀 INICIANDO INGESTÃO RAIS (2020-2024)")

    for year in range(2022, 1984, -1):
        year_path = PATH_BASE_RAIS / str(year)
        if not year_path.exists():
            continue

        print(f"\n📂 --- ANO: {year} ---")
        archives = list(year_path.glob("*.7z")) + list(year_path.glob("*.zip"))

        for archive_path in archives:
            print(f"📦 Extraindo: {archive_path.name}")
            
            # Limpa temp antes da nova extração
            for f in TEMP_EXTRACT_PATH.glob("*"): 
                try: os.remove(f)
                except: pass

            subprocess.run(["7z", "e", str(archive_path), f"-o{str(TEMP_EXTRACT_PATH)}", "-y"], 
                           capture_output=True, check=False)

            candidates = []
            for ext in ["*.txt", "*.csv", "*.comt", "*.TXT", "*.CSV", "*.COMT"]:
                candidates.extend(list(TEMP_EXTRACT_PATH.glob(ext)))

            for local_file in candidates:
                if local_file.stat().st_size < 10240: continue 

                print(f"    📄 Processando: {local_file.name}")
                
                if scrub_and_fix_file(local_file):
                    source_name = archive_path.stem.lower().replace(" ", "_")
                    target_name = f"{source_name}_{local_file.stem.lower()}.parquet"
                    s3_path = f"s3://{MINIO_BUCKET}/rais/ano={year}/{target_name}"

                    try:
                        print(f"    🔄 DuckDB -> S3 ({s3_path})")
                        con.execute(f"""
                            COPY (
                                SELECT 
                                    *,
                                    {year}::INT as ano_particao,
                                    '{archive_path.name}' as arquivo_origem
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
                        print(f"    ✅ Sucesso!")
                    except Exception as e:
                        print(f"    ❌ Erro no DuckDB: {e}")
                    
                    if local_file.exists(): 
                        os.remove(local_file)
                        print(f"    🧹 Arquivo temporário removido: {local_file.name}")
            
            gc.collect()

    print("\n✨ Processamento RAIS finalizado!")

if __name__ == "__main__":
    process_rais()