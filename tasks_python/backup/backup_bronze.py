import os
import s3fs
import polars as pl
import concurrent.futures

# --- CONFIGURAÇÕES ---
PREFIXO_NUVEM = "caged_mov"
CONTAINER = "bronze"
PASTA_BACKUP_LOCAL = "./tcc_backup_bronze" # Nome da pasta que será criada no seu PC
MAX_WORKERS = 6 # Como é pro disco local (SSD/HD), costuma ser mais rápido. Pode testar com 6 ou 8.

# --- CONEXÃO MINIO ---
S3_OPTIONS = {
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "endpoint_url": "http://localhost:9000",
}
fs_minio = s3fs.S3FileSystem(
    key=S3_OPTIONS["aws_access_key_id"],
    secret=S3_OPTIONS["aws_secret_access_key"],
    client_kwargs={"endpoint_url": S3_OPTIONS["endpoint_url"]}
)

# --- FUNÇÃO DA THREAD ---
def processar_backup_local(caminho_relativo):
    # Ex: caminho_relativo = bronze/caged_mov/ano_hive=2020/mes_hive=01/arquivo.parquet
    caminho_origem = f"s3://{caminho_relativo}"
    
    # Monta o caminho exato onde o arquivo vai ficar no seu PC
    # os.sep garante que vai usar a barra certa do seu sistema operacional (\ no Windows, / no Linux)
    caminho_destino_local = os.path.join(PASTA_BACKUP_LOCAL, caminho_relativo.replace("/", os.sep))
    
    # Cria as pastas e subpastas locais (se elas não existirem ainda)
    os.makedirs(os.path.dirname(caminho_destino_local), exist_ok=True)

    # Se o arquivo já existir no seu PC, ele pula (excelente caso você precise pausar e recomeçar o script)
    if os.path.exists(caminho_destino_local):
        print(f"   ⏭️ Já baixado: {caminho_relativo.split('/')[-1]}")
        return

    print(f"   -> Baixando e processando: {caminho_relativo.split('/')[-1]}")
    
    try:
        # Puxa via Streaming (Lazy) para economizar RAM
        lf = pl.scan_parquet(caminho_origem, storage_options=S3_OPTIONS)
        
        # Limpa as colunas de partição (igual foi feito na Azure)
        colunas_schema = lf.collect_schema().names()
        if "ano_hive" in colunas_schema:
            lf = lf.drop("ano_hive")
        if "mes_hive" in colunas_schema:
            lf = lf.drop("mes_hive")

        # Salva o arquivo no seu disco local (note que aqui não tem AZURE_OPTIONS, pois é local)
        lf.sink_parquet(
            caminho_destino_local,
            compression="zstd"
        )
        print(f"   ✅ Salvo no PC: {caminho_relativo.split('/')[-1]}")
        
    except Exception as e:
        print(f"   ❌ Erro em {caminho_relativo}: {e}")

# --- LOOP PRINCIPAL ---
def gerar_backup():
    print(f"📦 Iniciando criação do clone local em '{PASTA_BACKUP_LOCAL}'...\n")
    
    pasta_base = f"{CONTAINER}/{PREFIXO_NUVEM}"
    
    print("🔍 Varrendo o MinIO para encontrar os arquivos (aguarde)...")
    caminhos_minio = fs_minio.glob(f"{pasta_base}/**/*.parquet")
    
    if not caminhos_minio:
        print("⚠️ Nenhum arquivo encontrado no MinIO.")
        return

    print(f"📊 Encontrados {len(caminhos_minio)} arquivos. Iniciando download multithread...\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Passamos a lista de arquivos para as threads trabalharem juntas
        executor.map(processar_backup_local, caminhos_minio)

    print(f"\n🏁 Backup 100% finalizado! A pasta '{PASTA_BACKUP_LOCAL}' está pronta.")

if __name__ == "__main__":
    gerar_backup()