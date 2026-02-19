import os
import subprocess
import duckdb
import gc
import shutil
import re
from pathlib import Path

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

PATH_FALTANTES = Path(r"D:\lakehouse\minio\minio_data\bronze\faltantes")
TEMP_EXTRACT_PATH = Path(r"D:\lakehouse\minio\minio_data\bronze\temp_resgate")
SEVEN_ZIP_EXE = r"C:\Program Files\7-Zip\7z.exe"
DELIMITADOR = ';' 

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET memory_limit='10GB'; 
""")

def extrair_resgate_stream(arquivo_7z, arquivo_destino):
    print(f"   🚑 Extraindo (Modo Resgate): {arquivo_7z.name}")
    cmd = [SEVEN_ZIP_EXE, "e", str(arquivo_7z), "-so"]
    try:
        processo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        bytes_salvos = 0
        with open(arquivo_destino, 'wb') as f_out:
            while True:
                chunk = processo.stdout.read(1024 * 1024)
                if not chunk: break
                f_out.write(chunk)
                bytes_salvos += len(chunk)
        print(f"   📦 Dados recuperados: {bytes_salvos / (1024**2):.2f} MB")
        return True
    except Exception as e:
        print(f"   ❌ Erro na extração: {e}")
        return False

def scrub_and_fix_file(filepath):
    print(f"   🧹 Limpando arquivo: {filepath.name}")
    temp_clean = filepath.with_name(filepath.stem + "_clean.txt")
    lines_kept = 0
    lines_dropped = 0
    try:
        with open(filepath, 'r', encoding='latin-1', errors='replace') as f_in, \
             open(temp_clean, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                if '\0' in line:
                    lines_dropped += 1
                    continue
                parts = line.split(DELIMITADOR)
                if len(parts) < 3:
                    lines_dropped += 1
                    continue
                clean_parts = [p.strip() for p in parts]
                clean_line = DELIMITADOR.join(clean_parts)
                f_out.write(clean_line + "\n")
                lines_kept += 1
        
        # Remove última linha quebrada
        with open(temp_clean, 'rb+') as f:
            f.seek(0, os.SEEK_END)
            if f.tell() > 0:
                pos = f.tell() - 1
                while pos > 0 and f.read(1) != b'\n':
                    pos -= 1
                    f.seek(pos, os.SEEK_SET)
                if pos > 0:
                    f.seek(pos, os.SEEK_SET)
                    f.truncate()

        if filepath.exists(): os.remove(filepath)
        if temp_clean.exists(): os.rename(temp_clean, filepath)
        print(f"   ✨ Limpeza OK! Linhas: {lines_kept} | Lixo: {lines_dropped}")
        return True
    except Exception as e:
        print(f"   ⚠️ Erro crítico na limpeza: {e}")
        return False

def process_faltantes():
    if TEMP_EXTRACT_PATH.exists(): shutil.rmtree(TEMP_EXTRACT_PATH)
    TEMP_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)
    
    arquivos_7z = list(PATH_FALTANTES.glob("*.7z"))
    print(f"🚀 Iniciando resgate de {len(arquivos_7z)} arquivos em {PATH_FALTANTES}")

    for arquivo_7z in arquivos_7z:
        print(f"\n📂 Processando: {arquivo_7z.name}")
        
        # --- CORREÇÃO DO ANO AQUI ---
        match = re.search(r'(19|20)\d{2}', arquivo_7z.name)
        ano = match.group(0) if match else "indefinido"
        print(f"   📅 Ano detectado: {ano}")
        # ----------------------------

        nome_txt = arquivo_7z.stem + ".txt"
        caminho_txt = TEMP_EXTRACT_PATH / nome_txt

        if not extrair_resgate_stream(arquivo_7z, caminho_txt): continue
        if not scrub_and_fix_file(caminho_txt): continue

        s3_path = f"s3://{MINIO_BUCKET}/rais/ano={ano}/{arquivo_7z.stem}.parquet"
        path_leitura = str(caminho_txt).replace("\\", "/")
        
        print(f"   🔄 Convertendo para Parquet -> {s3_path}")
        try:
            con.execute(f"""
                COPY (
                    SELECT *, '{ano}' as ano_particao, '{arquivo_7z.name}' as arquivo_origem
                    FROM read_csv_auto('{path_leitura}', delim='{DELIMITADOR}', header=True, all_varchar=True, encoding='utf-8', ignore_errors=True, null_padding=True, quote='"')
                ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
            """)
            print(f"   ✅ Sucesso no upload!")
        except Exception as e:
            print(f"   ❌ Erro no DuckDB: {e}")

        try:
            if caminho_txt.exists(): os.remove(caminho_txt)
        except: pass
        gc.collect()

    try: shutil.rmtree(TEMP_EXTRACT_PATH)
    except: pass
    print("\n✨ Processamento de Faltantes Finalizado!")

if __name__ == "__main__":
    process_faltantes()