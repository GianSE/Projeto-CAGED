import os
import subprocess
import duckdb
import gc
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

TEMP_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)
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
    """ Tenta extrair ignorando CRC """
    cmd = ["7z", "e", str(archive_path), f"-o{str(output_dir)}", "-y"]
    subprocess.run(cmd, capture_output=True, text=True, check=False)
    return True

def scrub_and_fix_file(filepath):
    """
    Lê o arquivo binário corrompido e reescreve apenas as partes de texto válidas.
    Converte tudo para UTF-8 limpo para o DuckDB não reclamar.
    """
    print(f"   🧹 Iniciando cirurgia no arquivo (Scrubbing)...")
    temp_clean = filepath.with_name(filepath.name + ".clean")
    
    lines_kept = 0
    lines_dropped = 0
    
    try:
        # Abre o arquivo 'sujo' em modo binário ignorando erros de decode
        # Abre o arquivo 'limpo' em UTF-8 pronto para o DuckDB
        with open(filepath, 'r', encoding='latin-1', errors='replace') as f_in, \
             open(temp_clean, 'w', encoding='utf-8') as f_out:
            
            for line in f_in:
                # Se a linha tiver caracteres nulos (lixo binário), ignoramos
                # Trecho alterado para "espiar" o lixo
                if '\0' in line or len(line) < 5:
                    # Imprime só os primeiros 50 caracteres do lixo para não sujar o terminal
                    print(f"🗑️ Lixo descartado: {repr(line[:50])}")
                    lines_dropped += 1
                    continue

                # Escreve a linha limpa
                f_out.write(line)
                lines_kept += 1
            
            
        
        # Substitui o arquivo original pelo limpo
        os.remove(filepath)
        os.rename(temp_clean, filepath)
        
        print(f"   ✨ Arquivo limpo! Linhas mantidas: {lines_kept} | Lixo descartado: {lines_dropped}")
        return True
    except Exception as e:
        print(f"   ⚠️ Erro crítico na limpeza: {e}")
        return False

def process_caged_old():
    # Limpeza preventiva total antes de começar
    if TEMP_EXTRACT_PATH.exists():
        import shutil
        shutil.rmtree(TEMP_EXTRACT_PATH)
    TEMP_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)
    
    print(f"\n🚀 INICIANDO INGESTÃO (MODO CIRURGIA: CLEANER -> DUCKDB)")
    
    for year in range(2014, 2020):
        year_path = PATH_BASE_OLD / str(year)
        if not year_path.exists(): year_path = PATH_BASE_OLD

        print(f"\n📂 Processando Ano: {year}")

        for month in range(1, 13):
            mm = f"{month:02d}"
            filename_7z = f"CAGEDEST_{mm}{year}.7z"
            file_path = year_path / filename_7z
            
            if not file_path.exists():
                file_path = PATH_BASE_OLD / filename_7z
                if not file_path.exists(): continue

            s3_path = f"s3://{MINIO_BUCKET}/caged-old/ano={year}/mes={month}/data.parquet"
            local_txt = None
            
            try:
                # 1. Limpeza Temp
                for f in TEMP_EXTRACT_PATH.glob("*"):
                    try: os.remove(f)
                    except: pass

                # 2. Extração
                print(f"📦 Extraindo: {filename_7z}")
                extract_brute_force(file_path, TEMP_EXTRACT_PATH)
                
                candidates = list(TEMP_EXTRACT_PATH.glob("*.txt")) + list(TEMP_EXTRACT_PATH.glob("*.csv"))
                if not candidates:
                    print("   ⚠️ Nada extraído.")
                    continue

                local_txt = max(candidates, key=lambda p: p.stat().st_size)
                
                if local_txt.stat().st_size < 1024 * 1024:
                    print("   ⚠️ Arquivo muito pequeno (vazio).")
                    continue

                # 3. O PASSO MÁGICO: LIMPEZA PYTHON
                # Isso converte de Latin-1 "sujo" para UTF-8 "limpo"
                if not scrub_and_fix_file(local_txt):
                    continue

                # 4. Ingestão (Agora o arquivo é UTF-8 Limpo!)
                print(f"🔄 Convertendo para Parquet...")
                
                con.execute(f"""
                    COPY (
                        SELECT 
                            *,
                            {year}::INT as ano,
                            {month}::INT as mes
                        FROM read_csv(
                            '{str(local_txt)}',
                            delim=';',
                            header=True,
                            quote='"',
                            escape='"',
                            all_varchar=True,
                            encoding='utf-8',     -- AGORA É UTF-8 (Pois o Python converteu)
                            ignore_errors=True,
                            strict_mode=False,
                            null_padding=True
                        )
                    ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
                """)
                
                # Validação
                count = con.execute(f"SELECT count(*) FROM read_parquet('{s3_path}')").fetchone()[0]
                print(f"✅ Sucesso: {mm}/{year} | Linhas Salvas: {count}")

            except Exception as e:
                log_error(f"{year}-{mm}", str(e))
            
            finally:
                # --- LIMPEZA ROBUSTA ---
                # Garante que o arquivo extraído gigante seja deletado IMEDIATAMENTE
                # para liberar espaço para o próximo mês.
                print(f"🧹 Limpando temporários de {mm}/{year}...")
                
                # 1. Tenta deletar o arquivo txt local se a variável existir
                if local_txt and local_txt.exists():
                    try:
                        os.remove(local_txt)
                    except Exception as e:
                        print(f"⚠️ Falha ao deletar {local_txt.name}: {e}")

                # 2. Varredura geral na pasta temp para garantir que não sobrou lixo (ex: arquivos .clean)
                for f in TEMP_EXTRACT_PATH.glob("*"):
                    try:
                        if f.is_file():
                            os.remove(f)
                    except Exception as e:
                        print(f"⚠️ Falha ao limpar temp: {e}")

                # 3. Força a liberação de RAM
                gc.collect()

if __name__ == "__main__":
    process_caged_old()