import os
import subprocess
import duckdb
import gc
import shutil
from pathlib import Path
from datetime import datetime

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

PATH_BASE_OLD = Path("/data/caged/CAGED") 
TEMP_EXTRACT_PATH = Path("/tmp/extract_old")
LOG_FILE = Path("/app/tasks_python/bronze/erros_ingestao_old.txt")

# Garante criação dos diretórios
if not TEMP_EXTRACT_PATH.exists():
    TEMP_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)
if not LOG_FILE.parent.exists():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# --- CONEXÃO ---
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET memory_limit='3GB'; 
    SET temp_directory='/tmp/duckdb_spill';
""")

def log_error(contexto, error_msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {contexto} | ERRO: {error_msg}\n")
    print(f"❌ {error_msg}")

def extract_brute_force(archive_path, output_dir):
    """ Tenta extrair ignorando CRC usando 7zip """
    # -y: assume Yes para tudo (sobrescrever)
    cmd = ["7z", "e", str(archive_path), f"-o{str(output_dir)}", "-y"]
    subprocess.run(cmd, capture_output=True, text=True, check=False)
    return True

def scrub_and_fix_file(filepath):
    """
    Lê o arquivo binário corrompido e reescreve apenas as partes de texto válidas.
    Converte Latin-1 -> UTF-8 para o DuckDB.
    """
    print(f"   🧹 Iniciando cirurgia no arquivo (Scrubbing)...")
    temp_clean = filepath.with_name(filepath.name + ".clean")
    
    lines_kept = 0
    lines_dropped = 0
    
    try:
        # Abre in com 'replace' para não travar em byte inválido
        with open(filepath, 'r', encoding='latin-1', errors='replace') as f_in, \
             open(temp_clean, 'w', encoding='utf-8') as f_out:
            
            for line in f_in:
                # Remove linhas com null bytes ou muito curtas (lixo de fim de arquivo)
                if '\0' in line or len(line.strip()) < 5:
                    if lines_dropped < 5: # Loga só os primeiros erros pra não poluir
                        print(f"🗑️ Lixo descartado (amostra): {repr(line[:50])}")
                    lines_dropped += 1
                    continue

                f_out.write(line)
                lines_kept += 1
        
        # Troca o arquivo sujo pelo limpo
        os.remove(filepath)
        os.rename(temp_clean, filepath)
        
        print(f"   ✨ Arquivo limpo! Linhas: {lines_kept} | Lixo: {lines_dropped}")
        return True
    except Exception as e:
        print(f"   ⚠️ Erro crítico na limpeza: {e}")
        return False

def process_caged_old():
    # --- CORREÇÃO 1: Limpeza segura da pasta temp (sem rmtree na raiz) ---
    print("🧹 Limpando diretório temporário inicial...")
    if TEMP_EXTRACT_PATH.exists():
        for item in TEMP_EXTRACT_PATH.iterdir():
            try:
                if item.is_file(): item.unlink()
                elif item.is_dir(): shutil.rmtree(item)
            except Exception as e:
                print(f"⚠️ Erro ao limpar temp: {e}")
    
    print(f"\n🚀 INICIANDO INGESTÃO (MODO CIRURGIA: CLEANER -> DUCKDB)")
    
    for year in range(2007, 2014):
        year_path = PATH_BASE_OLD / str(year)
        # Fallback se os arquivos estiverem na raiz
        if not year_path.exists(): year_path = PATH_BASE_OLD

        print(f"\n📂 Processando Ano: {year}")

        for month in range(1, 13):
            mm = f"{month:02d}"
            filename_7z = f"CAGEDEST_{mm}{year}.7z"
            file_path = year_path / filename_7z
            
            if not file_path.exists():
                # Tenta procurar na raiz se não achou na pasta do ano
                file_path = PATH_BASE_OLD / filename_7z
                if not file_path.exists(): continue

            # --- CORREÇÃO 2: Padronização de nomes (ano_particao) ---
            s3_path = f"s3://{MINIO_BUCKET}/caged_old/ano={year}/mes={month}/caged_old_{year}{mm}.parquet"
            local_txt = None
            
            try:
                # 1. Limpeza Temp (Garante pasta vazia para extração)
                for f in TEMP_EXTRACT_PATH.glob("*"):
                    try: os.remove(f)
                    except: pass

                # 2. Extração
                print(f"📦 Extraindo: {filename_7z}")
                extract_brute_force(file_path, TEMP_EXTRACT_PATH)
                
                candidates = list(TEMP_EXTRACT_PATH.glob("*.txt")) + list(TEMP_EXTRACT_PATH.glob("*.csv"))
                if not candidates:
                    print("   ⚠️ Nada extraído (arquivo vazio ou nome inesperado).")
                    continue

                # Pega o maior arquivo (ignora leia-me.txt pequenos)
                local_txt = max(candidates, key=lambda p: p.stat().st_size)
                
                if local_txt.stat().st_size < 1024 * 1024: # < 1MB
                    print("   ⚠️ Arquivo muito pequeno/vazio.")
                    continue

                # 3. Limpeza Python (Latin1 -> UTF8)
                if not scrub_and_fix_file(local_txt):
                    continue

                # 4. Ingestão DuckDB
                print(f"🔄 Convertendo para Parquet...")
                
                # Usando read_csv_auto pois agora o arquivo está limpo (UTF-8)
                # Adicionado ano_particao e mes_particao
                con.execute(f"""
                    COPY (
                        SELECT 
                            *,
                            {year}::INT as ano_particao,
                            {month}::INT as mes_particao,
                            '{filename_7z}' as arquivo_origem  -- <-- ADICIONADO AQUI
                        FROM read_csv_auto(
                            '{str(local_txt)}',
                            delim=';',
                            header=True,
                            quote='"',
                            escape='"',
                            all_varchar=True,
                            normalize_names=True,
                            encoding='utf-8',     
                            ignore_errors=True,
                            null_padding=True
                        )
                    ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
                """)
                
                print(f"✅ Sucesso: {mm}/{year}")

            except Exception as e:
                log_error(f"{year}-{mm}", str(e))
            
            finally:
                # 5. Limpeza Final (Importante para liberar disco)
                print(f"🧹 Limpando temporários de {mm}/{year}...")
                
                if local_txt and local_txt.exists():
                    try: os.remove(local_txt)
                    except: pass

                # Remove qualquer sobra (.clean, etc)
                for f in TEMP_EXTRACT_PATH.glob("*"):
                    try: 
                        if f.is_file(): os.remove(f)
                    except: pass
                
                gc.collect()

if __name__ == "__main__":
    process_caged_old()