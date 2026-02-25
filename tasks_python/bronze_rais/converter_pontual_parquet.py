import os
import duckdb
import gc
from pathlib import Path
import time

# --- CONFIGURAÇÕES DE ENTRADA (EDITE AQUI) ---
# Caminho do arquivo que você recuperou/quer converter
ARQUIVO_ORIGEM = Path(r"D:\lakehouse\minio\minio_data\bronze\rais\ano=2013\resgate\BA2013_resgatado.txt")

# Ano de referência para a partição
ANO_ALVO = 2013

# Configurações do MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000") # Se rodar no Windows local, use localhost
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

# --- CONEXÃO DUCKDB ---
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET memory_limit='8GB'; -- Aumentei um pouco pois arquivo único exige menos overhead
""")

def limpar_arquivo(filepath):
    """ 
    Lê o arquivo original (Latin-1), remove Nulos e Espaços extras,
    e salva um novo arquivo temporário UTF-8 pronto para o DuckDB.
    """
    print(f"🧹 Iniciando limpeza (Scrubbing) em: {filepath.name}")
    temp_clean = filepath.with_name(filepath.stem + "_clean.txt")
    
    lines_kept = 0
    lines_dropped = 0
    start_time = time.time()
    
    try:
        # RAIS antiga costuma ser Latin-1. Se falhar, tente 'cp1252'.
        with open(filepath, 'r', encoding='latin-1', errors='replace') as f_in, \
             open(temp_clean, 'w', encoding='utf-8') as f_out:
            
            for i, line in enumerate(f_in):
                # Feedback visual a cada 500k linhas
                if i % 500000 == 0 and i > 0:
                    print(f"   ... processadas {i/1000000:.1f}M linhas ...")

                # 1. Ignora linhas com Null Bytes (comum em corrupção)
                if '\0' in line:
                    lines_dropped += 1
                    continue
                
                # 2. Limpeza de espaços e verificação básica
                parts = line.split(';')
                
                # Se a linha estiver muito quebrada (menos colunas que o esperado), pode filtrar aqui
                # Ex: RAIS costuma ter muitas colunas, se tiver só 1 ou 2 é lixo
                if len(parts) < 5: 
                    lines_dropped += 1
                    continue

                clean_parts = [p.strip() for p in parts]
                clean_line = ";".join(clean_parts)
                
                f_out.write(clean_line + "\n")
                lines_kept += 1
        
        print(f"✨ Limpeza concluída em {time.time() - start_time:.1f}s")
        print(f"✅ Linhas Mantidas: {lines_kept} | 🗑️ Linhas Descartadas: {lines_dropped}")
        return temp_clean

    except Exception as e:
        print(f"⚠️ Erro crítico na limpeza: {e}")
        return None

def processar_arquivo_unico():
    if not ARQUIVO_ORIGEM.exists():
        print(f"❌ Arquivo não encontrado: {ARQUIVO_ORIGEM}")
        return

    print(f"\n🚀 PROCESSANDO ARQUIVO ÚNICO: {ARQUIVO_ORIGEM.name}")
    print(f"📅 Ano Referência: {ANO_ALVO}")

    # 1. Limpeza
    arquivo_limpo = limpar_arquivo(ARQUIVO_ORIGEM)
    
    if not arquivo_limpo:
        print("❌ Abortando devido a erro na limpeza.")
        return

    # 2. Definição do Caminho S3
    nome_destino = ARQUIVO_ORIGEM.stem.lower().replace(" ", "_").replace("_resgatado", "") + ".parquet"
    s3_path = f"s3://{MINIO_BUCKET}/rais/ano={ANO_ALVO}/{nome_destino}"
    
    # Ajuste de caminho para o DuckDB (Windows precisa de / mesmo sendo local)
    path_leitura = str(arquivo_limpo).replace("\\", "/")

    print(f"🔄 Convertendo DuckDB -> S3: {s3_path}")

    try:
        query = f"""
            COPY (
                SELECT 
                    *,
                    {ANO_ALVO}::INT as ano_particao,
                    '{ARQUIVO_ORIGEM.name}' as arquivo_origem
                FROM read_csv_auto(
                    '{path_leitura}',
                    delim=';',
                    header=True,
                    all_varchar=True, -- Importante para evitar erro de tipo em coluna suja
                    encoding='utf-8', -- Já limpamos para UTF-8
                    ignore_errors=True,
                    null_padding=True,
                    quote='"'
                )
            ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
        con.execute(query)
        print(f"✅ SUCESSO! Arquivo salvo no MinIO.")
    
    except Exception as e:
        print(f"❌ Erro no DuckDB: {e}")
    
    finally:
        # 3. Limpeza do arquivo temporário _clean
        if arquivo_limpo.exists():
            try:
                os.remove(arquivo_limpo)
                print(f"🧹 Arquivo temporário removido: {arquivo_limpo.name}")
            except:
                pass
        gc.collect()

if __name__ == "__main__":
    processar_arquivo_unico()