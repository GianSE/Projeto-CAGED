import s3fs
import adlfs
from env import string_azure

# --- CONFIGURAÇÕES ---
PREFIXO_NUVEM = "caged_mov"
CONTAINER = "bronze"
ANOS_PARA_CHECAR = range(2020, 2026) # Ajuste até o ano final que você tem

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

# --- CONEXÃO AZURE ---
_azure_parts = dict(part.split("=", 1) for part in string_azure.split(";") if "=" in part)
fs_azure = adlfs.AzureBlobFileSystem(
    account_name=_azure_parts["AccountName"],
    account_key=_azure_parts["AccountKey"]
)

def auditar_migracao():
    print(f"🔍 Iniciando auditoria da pasta '{PREFIXO_NUVEM}'...\n")
    
    total_faltando = 0
    anos_com_erro = []

    for ano in ANOS_PARA_CHECAR:
        pasta_minio = f"{CONTAINER}/{PREFIXO_NUVEM}/ano_hive={ano}"
        pasta_azure = f"{CONTAINER}/{PREFIXO_NUVEM}/ano_hive={ano}"

        # 1. Lista arquivos no MinIO e pega apenas o nome final do arquivo
        caminhos_minio = fs_minio.glob(f"{pasta_minio}/**/*.parquet")
        arquivos_minio = set([caminho.split("/")[-1] for caminho in caminhos_minio])

        # 2. Lista arquivos no Azure e pega apenas o nome final do arquivo
        caminhos_azure = fs_azure.glob(f"{pasta_azure}/**/*.parquet")
        arquivos_azure = set([caminho.split("/")[-1] for caminho in caminhos_azure])

        # 3. Compara os dois lados
        if not arquivos_minio:
            print(f"⏭️ {ano}: Nenhum arquivo encontrado no MinIO.")
            continue

        # Subtrai o conjunto do Azure do conjunto do MinIO. O que sobrar, é o que falta subir.
        arquivos_faltantes = arquivos_minio - arquivos_azure

        if arquivos_faltantes:
            qtd_falta = len(arquivos_faltantes)
            total_faltando += qtd_falta
            anos_com_erro.append(ano)
            print(f"❌ {ano}: Faltam {qtd_falta} arquivos no Azure! Ex: {list(arquivos_faltantes)[:3]}")
        else:
            print(f"✅ {ano}: Sincronizado perfeitamente ({len(arquivos_minio)} arquivos).")

    # --- RESUMO FINAL ---
    print("-" * 40)
    if total_faltando == 0:
        print("🎉 AUDITORIA CONCLUÍDA: Todos os arquivos do MinIO estão no Azure!")
    else:
        print(f"⚠️ ATENÇÃO: Faltam {total_faltando} arquivos no total.")
        print(f"Anos com pendências: {anos_com_erro}")

if __name__ == "__main__":
    auditar_migracao()