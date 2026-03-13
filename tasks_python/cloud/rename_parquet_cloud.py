import s3fs
import adlfs
from env import string_azure

# --- CONFIGURAÇÕES ---
PREFIXO_NUVEM = "caged_ajustes"
CONTAINER = "bronze"
ANOS_PARA_CHECAR = range(2010, 2020)

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

def renomear_arquivos_nuvem():
    print(f"🚀 Iniciando alinhamento de nomes na pasta '{PREFIXO_NUVEM}'...\n")
    
    arquivos_renomeados = 0

    for ano in ANOS_PARA_CHECAR:
        pasta_base_minio = f"{CONTAINER}/{PREFIXO_NUVEM}/ano_hive={ano}"
        
        # Pega todos os arquivos parquet desse ano no MinIO (todas as subpastas mes_hive)
        caminhos_minio = fs_minio.glob(f"{pasta_base_minio}/**/*.parquet")
        
        if not caminhos_minio:
            print(f"⏭️ {ano}: Nenhum arquivo no MinIO para mapear.")
            continue

        for caminho_minio in caminhos_minio:
            # Quebra o caminho para isolar a pasta exata e o nome do arquivo correto
            partes_caminho = caminho_minio.split("/")
            nome_correto = partes_caminho[-1]
            pasta_exata = "/".join(partes_caminho[:-1]) # Ex: bronze/caged_mov/ano_hive=2020/mes_hive=1

            # Procura o arquivo equivalente na mesma pasta lá no Azure
            caminhos_azure = fs_azure.glob(f"{pasta_exata}/*.parquet")

            if len(caminhos_azure) == 1:
                caminho_azure_atual = caminhos_azure[0]
                nome_atual_azure = caminho_azure_atual.split("/")[-1]

                # Se o nome estiver diferente, manda renomear!
                if nome_atual_azure != nome_correto:
                    novo_caminho_azure = f"{pasta_exata}/{nome_correto}"
                    print(f" 🔄 Renomeando em {pasta_exata.split('caged_mov/')[-1]}: \n    De: {nome_atual_azure} \n    Para: {nome_correto}")
                    
                    try:
                        # O comando rename do fsspec resolve a mágica na nuvem
                        fs_azure.rename(caminho_azure_atual, novo_caminho_azure)
                        arquivos_renomeados += 1
                    except Exception as e:
                        print(f"    ❌ Erro ao renomear: {e}")
            
            elif len(caminhos_azure) == 0:
                print(f" ⚠️ ALERTA: A pasta {pasta_exata} está VAZIA no Azure! O arquivo não subiu.")
            else:
                print(f" ⚠️ ALERTA: A pasta {pasta_exata} tem MAIS DE UM arquivo no Azure. Verifique manualmente.")

    # --- RESUMO FINAL ---
    print("-" * 40)
    print(f"🎉 PROCESSO CONCLUÍDO! Total de arquivos renomeados na nuvem: {arquivos_renomeados}")

if __name__ == "__main__":
    renomear_arquivos_nuvem()