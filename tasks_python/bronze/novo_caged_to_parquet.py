import os
import py7zr
import duckdb
from pathlib import Path
import zipfile
import shutil

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000") 
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

# Caminhos
BASE_PATH = Path("/data/caged/NOVO CAGED") 
TEMP_EXTRACT_PATH = Path("/tmp/extract")

# Conexão DuckDB
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_region='us-east-1'; 
""")

def process_caged():
    print(f"🔍 Iniciando varredura em: {BASE_PATH}")
    
    if not TEMP_EXTRACT_PATH.exists():
        TEMP_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)
    else:
        for item in TEMP_EXTRACT_PATH.iterdir():
            try:
                if item.is_file() or item.is_symlink():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                print(f"⚠️ Não foi possível limpar {item}: {e}")

    type_map = {'EXC': 'caged_exc', 'FOR': 'caged_for', 'MOV': 'caged_mov'}
    
    pastas_encontradas = 0

    for year in range(2020, 2026):
        for month in range(1, 13):
            str_month = f"{month:02d}"
            str_year_month = f"{year}{str_month}"
            
            path_direto = BASE_PATH / str_year_month
            path_aninhado = BASE_PATH / str(year) / str_year_month
            
            folder_path = None
            if path_direto.exists():
                folder_path = path_direto
            elif path_aninhado.exists():
                folder_path = path_aninhado
            
            if not folder_path:
                continue
                
            pastas_encontradas += 1
            print(f"\n--- 🚀 Processando competência: {str_year_month} ---")

            for f_sigla, f_tabela in type_map.items():
                possiveis_nomes = [
                    f"CAGED{f_sigla}{str_year_month}.7z",
                    f"CAGED{f_sigla}{str_year_month}.zip",
                    f"CAGED{f_sigla}{str_year_month}.txt.7z",
                    f"caged{f_sigla}{str_year_month}.7z"
                ]
                
                archive_full_path = None
                for nome in possiveis_nomes:
                    if (folder_path / nome).exists():
                        archive_full_path = folder_path / nome
                        break
                
                if archive_full_path:
                    try:
                        print(f"📦 Extraindo {archive_full_path.name}...")
                        
                        for f in TEMP_EXTRACT_PATH.glob("*"):
                            try: os.remove(f) 
                            except: pass

                        target_txt_path = None

                        if archive_full_path.suffix == '.7z':
                            with py7zr.SevenZipFile(archive_full_path, mode='r') as z:
                                all_names = z.getnames()
                                candidates = [f for f in all_names if f.lower().endswith(('.txt', '.csv'))]
                                if candidates:
                                    z.extract(targets=[candidates[0]], path=TEMP_EXTRACT_PATH)
                                    target_txt_path = TEMP_EXTRACT_PATH / candidates[0]
                        else:
                            with zipfile.ZipFile(archive_full_path, 'r') as z:
                                all_names = z.namelist()
                                candidates = [f for f in all_names if f.lower().endswith(('.txt', '.csv'))]
                                if candidates:
                                    z.extract(candidates[0], TEMP_EXTRACT_PATH)
                                    target_txt_path = TEMP_EXTRACT_PATH / candidates[0]

                        if not target_txt_path or not target_txt_path.exists():
                            print(f"⚠️ Arquivo de dados não encontrado dentro do compactado.")
                            continue

                        s3_target_path = (
                            f"s3://{MINIO_BUCKET}/{f_tabela}/" 
                            f"ano={year}/mes={month}/"     
                            f"{f_tabela}_{str_year_month}.parquet"
                        )
                        
                        print(f"📤 Uploading -> {s3_target_path}")
                        
                        # --- ADICIONADO: arquivo_fonte no SELECT ---
                        con.execute(f"""
                            COPY (
                                SELECT 
                                    *,
                                    {year}::INT as ano_particao,
                                    {month}::INT as mes_particao,
                                    '{archive_full_path.name}' as arquivo_fonte
                                FROM read_csv_auto(
                                    '{str(target_txt_path)}', 
                                    delim=';',
                                    header=True, 
                                    normalize_names=True,
                                    all_varchar=True,
                                    encoding='latin-1', 
                                    ignore_errors=True,
                                    null_padding=True,
                                    quote='"'
                                )
                            ) TO '{s3_target_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
                        """)

                        print(f"✅ Sucesso!")

                        if target_txt_path.exists():
                            target_txt_path.unlink()
                            print(f"🧹 Limpeza imediata: {target_txt_path.name} apagado.")

                    except Exception as e:
                        print(f"❌ Erro ao processar {archive_full_path.name}: {e}")
                        if 'target_txt_path' in locals() and target_txt_path and target_txt_path.exists():
                            target_txt_path.unlink()
    
    if pastas_encontradas == 0:
        print("\n⚠️ Nenhuma pasta encontrada. Verifique o BASE_PATH.")

if __name__ == "__main__":
    process_caged()