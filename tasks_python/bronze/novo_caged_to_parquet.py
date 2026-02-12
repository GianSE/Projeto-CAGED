import os
import py7zr
import duckdb
from pathlib import Path
import zipfile

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

# Caminhos (Ajuste conforme seu volume Docker)
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
""")

def process_caged():
    print(f"🔍 Iniciando varredura em: {BASE_PATH}")
    if not BASE_PATH.exists():
        print(f"❌ ERRO CRÍTICO: A pasta raiz {BASE_PATH} não existe!")
        return

    TEMP_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)
    type_map = {'EXC': 'caged-exc', 'FOR': 'caged-for', 'MOV': 'caged-mov'}
    
    pastas_encontradas = 0

    for year in range(2020, 2026):
        for month in range(1, 13):
            str_month = f"{month:02d}"
            int_month = month
            str_year_month = f"{year}{str_month}"
            
            # --- LÓGICA HÍBRIDA: Tenta achar a pasta de qualquer jeito ---
            path_direto = BASE_PATH / str_year_month             # Ex: .../202001
            path_aninhado = BASE_PATH / str(year) / str_year_month # Ex: .../2020/202001
            
            folder_path = None
            if path_direto.exists():
                folder_path = path_direto
            elif path_aninhado.exists():
                folder_path = path_aninhado
            
            if not folder_path:
                # Comente o print abaixo se quiser silenciar os erros de pasta não encontrada
                # print(f"⚠️ Pasta não encontrada: {str_year_month} (Tentado: {path_direto})")
                continue
                
            pastas_encontradas += 1
            print(f"\n--- 🚀 Processando: {str_year_month} (em {folder_path.name}) ---")

            for f_sigla, f_tabela in type_map.items():
                # Tenta .7z e .zip
                archive_filename_7z = f"CAGED{f_sigla}{str_year_month}.7z"
                archive_filename_zip = f"CAGED{f_sigla}{str_year_month}.zip"
                
                archive_full_path = folder_path / archive_filename_7z
                
                if not archive_full_path.exists():
                    archive_full_path = folder_path / archive_filename_zip
                
                if not archive_full_path.exists():
                    # Tenta minúsculo também (Linux é case sensitive)
                    archive_full_path = folder_path / archive_filename_7z.lower()

                if archive_full_path.exists():
                    try:
                        print(f"📦 Extraindo {archive_full_path.name}...")
                        inner_txt_name = None
                        
                        # Extração
                        if archive_full_path.suffix == '.7z':
                            with py7zr.SevenZipFile(archive_full_path, mode='r') as z:
                                all_files = z.getnames()
                                txt_files = [f for f in all_files if f.lower().endswith(('.txt', '.csv'))]
                                if txt_files:
                                    inner_txt_name = txt_files[0]
                                    z.extract(targets=[inner_txt_name], path=TEMP_EXTRACT_PATH)
                        else: # zip
                            with zipfile.ZipFile(archive_full_path, 'r') as z:
                                all_files = z.namelist()
                                txt_files = [f for f in all_files if f.lower().endswith(('.txt', '.csv'))]
                                if txt_files:
                                    inner_txt_name = txt_files[0]
                                    z.extract(inner_txt_name, TEMP_EXTRACT_PATH)

                        if not inner_txt_name:
                            print(f"⚠️  Arquivo compactado vazio ou sem txt: {archive_full_path.name}")
                            continue

                        local_txt_path = TEMP_EXTRACT_PATH / inner_txt_name

                        # Caminho S3 (Particionamento INT correto)
                        s3_target_path = (
                            f"s3://{MINIO_BUCKET}/{f_tabela}/" # Aqui ele cria a pasta da tabela (exc, for ou mov)
                            f"ano={year}/mes={int_month}/"     # Particionamento Hive
                            f"{f_tabela}_{str_year_month}.parquet"
                        )
                        
                        print(f"📤 Convertendo -> {s3_target_path}")
                        
                        con.execute(f"""
                            COPY (
                                SELECT 
                                    *,
                                    {year}::INT as ano,
                                    {int_month}::INT as mes
                                FROM read_csv_auto(
                                    '{str(local_txt_path)}', 
                                    delim=';', 
                                    encoding='utf-8', 
                                    normalize_names=True,
                                    ignore_errors=True
                                )
                            ) TO '{s3_target_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
                        """)

                        if local_txt_path.exists(): os.remove(local_txt_path)
                        print(f"✅ Sucesso: {f_tabela}")

                    except Exception as e:
                        print(f"❌ Erro ao processar {archive_full_path.name}: {e}")
                        if 'local_txt_path' in locals() and local_txt_path.exists(): os.remove(local_txt_path)
    
    if pastas_encontradas == 0:
        print("\n⚠️ AVISO FINAL: Nenhuma pasta de competência foi encontrada.")
        print("   Verifique se o volume do Docker está montado corretamente e se os nomes batem (ex: 202001).")

if __name__ == "__main__":
    process_caged()