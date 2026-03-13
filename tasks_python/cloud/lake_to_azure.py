import s3fs
import adlfs
import polars as pl
import concurrent.futures
from env import string_azure

# --- CONFIGURAÇÕES ---
PREFIXO_NUVEM = "caged_old"
CONTAINER = "bronze"
ANOS_COM_ERRO = range(2007, 2020) # Ajuste a sua lista de anos aqui
MAX_WORKERS = 4

# --- CREDENCIAIS ---
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

_azure_parts = dict(part.split("=", 1) for part in string_azure.split(";") if "=" in part)
AZURE_OPTIONS = {
    "account_name": _azure_parts["AccountName"],
    "account_key": _azure_parts["AccountKey"],
}
fs_azure = adlfs.AzureBlobFileSystem(
    account_name=AZURE_OPTIONS["account_name"],
    account_key=AZURE_OPTIONS["account_key"]
)

# --- FUNÇÃO DA THREAD ---
def transferir_arquivo(caminho_relativo):
    print(f"   -> Subindo: {caminho_relativo.split('/')[-1]} (Mês: {caminho_relativo.split('/')[-2]})")
    
    # Reconstrói os caminhos absolutos
    caminho_origem = f"s3://{CONTAINER}/{caminho_relativo}"
    caminho_destino = f"az://{CONTAINER}/{caminho_relativo}"

    try:
        lf = pl.scan_parquet(caminho_origem, storage_options=S3_OPTIONS)
        
        # Como as partições ano_hive e mes_hive já estão na estrutura de pastas, 
        # é boa prática remover do parquet se estiverem lá dentro.
        colunas_schema = lf.collect_schema().names()
        if "ano_hive" in colunas_schema:
            lf = lf.drop("ano_hive")
        if "mes_hive" in colunas_schema:
            lf = lf.drop("mes_hive")

        lf.sink_parquet(
            caminho_destino,
            compression="zstd",
            storage_options=AZURE_OPTIONS,
        )
        print(f"   ✅ Sucesso: {caminho_relativo.split('/')[-1]}")
    except Exception as e:
        print(f"   ❌ Erro em {caminho_relativo}: {e}")

# --- LOOP PRINCIPAL ---
def curar_arquivos():
    print("🩹 Iniciando migração MULTITHREAD corrigida...\n")
    
    for ano in ANOS_COM_ERRO:
        pasta_base = f"{CONTAINER}/{PREFIXO_NUVEM}/ano_hive={ano}"

        # Usando ** para buscar dentro dos meses
        caminhos_minio = fs_minio.glob(f"{pasta_base}/**/*.parquet")
        caminhos_azure = fs_azure.glob(f"{pasta_base}/**/*.parquet")

        # Extrai o caminho relativo (ex: caged_mov/ano_hive=2007/mes_hive=01/arquivo.parquet)
        # Isso garante que ele compare a pasta exata e o nome do arquivo.
        relativos_minio = set([c.split(f"{CONTAINER}/")[1] for c in caminhos_minio])
        relativos_azure = set([c.split(f"{CONTAINER}/")[1] for c in caminhos_azure])

        arquivos_faltantes = relativos_minio - relativos_azure

        if arquivos_faltantes:
            print(f"\n⚠️ {ano}: Faltam {len(arquivos_faltantes)} arquivos. Iniciando upload em paralelo...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Passa o caminho relativo exato para a thread
                executor.map(transferir_arquivo, arquivos_faltantes)
                
        else:
            print(f"✅ {ano}: 100% sincronizado.")

    print("\n🏁 Processo finalizado!")

if __name__ == "__main__":
    curar_arquivos()