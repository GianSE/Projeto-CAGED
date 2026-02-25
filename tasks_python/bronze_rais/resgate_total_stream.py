import os
import subprocess
import duckdb
import gc
import shutil
from pathlib import Path

# --- CONFIGURAÇÕES ---
PATH_FALTANTES = Path(r"D:\lakehouse\minio\minio_data\bronze\faltantes")
PASTA_TEMP = Path(r"D:\lakehouse\minio\minio_data\bronze\temp_desespero")

# Caminho do 7-Zip
SEVEN_ZIP_EXE = r"C:\Program Files\7-Zip\7z.exe"

# DuckDB / MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

DELIMITADOR = ';'

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

def tentar_extrair_na_marra(arquivo_7z, arquivo_destino):
    print(f"   🔨 Marretando: {arquivo_7z.name}")
    
    # Lista de "truques" para enganar o 7-Zip
    # -t* : Tenta detectar qualquer formato automaticamente (recursive)
    # -tzip, -tgzip : Força formatos específicos caso a extensão esteja mentindo
    modos = [
        ["-t*"],       # Modo "Descubra você mesmo" (Scan profundo)
        ["-t7z"],      # Força 7z
        ["-tzip"],     # Força Zip (comum arquivos virem errados)
        ["-tgzip"],    # Força Gzip
        ["-txz"]       # Força XZ
    ]
    
    for i, flags in enumerate(modos):
        cmd = [SEVEN_ZIP_EXE, "e", str(arquivo_7z), "-so"] + flags
        
        try:
            processo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Tenta ler o primeiro pedaço pra ver se vem dado
            bytes_salvos = 0
            with open(arquivo_destino, 'wb') as f_out:
                while True:
                    chunk = processo.stdout.read(1024 * 1024)
                    if not chunk: break
                    f_out.write(chunk)
                    bytes_salvos += len(chunk)
            
            # Se extraiu mais de 10KB, consideramos uma vitória e paramos de tentar outros modos
            if bytes_salvos > 10240:
                print(f"   ✅ SUCESSO com modo {flags}! Recuperado: {bytes_salvos / (1024**2):.2f} MB")
                return True
            
        except:
            pass
            
    print("   ❌ Falha: Nenhum modo conseguiu extrair dados úteis.")
    return False

def limpar_e_salvar(filepath, nome_original):
    print(f"   🧹 Filtrando o que sobrou...")
    temp_clean = filepath.with_name(filepath.stem + "_clean.txt")
    
    lines_kept = 0
    
    try:
        # Lê tentando recuperar texto legível
        with open(filepath, 'r', encoding='latin-1', errors='replace') as f_in, \
             open(temp_clean, 'w', encoding='utf-8') as f_out:
            
            for line in f_in:
                if '\0' in line: continue
                # Só salva se tiver pelo menos 3 colunas (evita HTML e lixo binário)
                if line.count(DELIMITADOR) >= 3:
                    parts = [p.strip() for p in line.split(DELIMITADOR)]
                    f_out.write(DELIMITADOR.join(parts) + "\n")
                    lines_kept += 1
        
        if lines_kept == 0:
            print("   ⚠️ Arquivo contém dados, mas não parece CSV de RAIS (0 linhas válidas).")
            # Debug: Mostra o que tem dentro
            with open(filepath, 'r', errors='ignore') as f:
                print(f"   👀 Conteúdo (Primeiros 100 chars): {f.read(100)}")
            return False

        # DuckDB -> Parquet
        ano = "".join(filter(str.isdigit, nome_original)) or "indefinido"
        s3_path = f"s3://{MINIO_BUCKET}/rais/ano={ano}/{Path(nome_original).stem}.parquet"
        
        print(f"   🔄 Salvando {lines_kept} linhas em -> {s3_path}")
        path_leitura = str(temp_clean).replace("\\", "/")
        
        con.execute(f"""
            COPY (
                SELECT *, '{ano}' as ano_particao, '{nome_original}' as arquivo_origem
                FROM read_csv_auto('{path_leitura}', delim='{DELIMITADOR}', header=True, all_varchar=True, ignore_errors=True)
            ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """)
        return True

    except Exception as e:
        print(f"   ❌ Erro processando: {e}")
        return False

def main():
    if PASTA_TEMP.exists(): shutil.rmtree(PASTA_TEMP)
    PASTA_TEMP.mkdir(parents=True, exist_ok=True)
    
    arquivos = list(PATH_FALTANTES.glob("*.*")) # Pega tudo, não só .7z
    print(f"💀 Iniciando tentativa final em {len(arquivos)} arquivos.")

    for arq in arquivos:
        # Pula arquivos muito pequenos (menos de 1KB é certeza que é lixo)
        if arq.stat().st_size < 100: continue
        
        nome_txt = arq.stem + ".txt"
        caminho_txt = PASTA_TEMP / nome_txt
        
        if tentar_extrair_na_marra(arq, caminho_txt):
            limpar_e_salvar(caminho_txt, arq.name)
        
        # Limpeza rápida
        try: os.remove(caminho_txt)
        except: pass
        print("-" * 30)

    try: shutil.rmtree(PASTA_TEMP)
    except: pass
    print("\n🏁 Fim da operação.")

if __name__ == "__main__":
    main()