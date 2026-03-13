import os
import shutil
import s3fs
import polars as pl
import concurrent.futures

# --- CONFIGURAÇÕES ---
# Coloque aqui todas as tabelas que você quer fazer backup
TABELAS = ["caged_ajustes", "caged_exc", "caged_for", "caged_mov", "caged_old", "rais_estab", "rais"] 
CONTAINER = "bronze"
PASTA_BACKUP_BASE = "./bkp_temporario" # Pasta onde os parquets vão ficar antes de zipar
MAX_WORKERS = 6 

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
    caminho_origem = f"s3://{caminho_relativo}"
    
    # Caminho no PC: ./bkp_temporario/bronze/nome_da_tabela/ano_hive=...
    caminho_destino_local = os.path.join(PASTA_BACKUP_BASE, caminho_relativo.replace("/", os.sep))
    
    os.makedirs(os.path.dirname(caminho_destino_local), exist_ok=True)

    if os.path.exists(caminho_destino_local):
        print(f"   ⏭️ Já baixado: {caminho_relativo.split('/')[-1]}")
        return

    try:
        lf = pl.scan_parquet(caminho_origem, storage_options=S3_OPTIONS)
        
        colunas_schema = lf.collect_schema().names()
        if "ano_hive" in colunas_schema:
            lf = lf.drop("ano_hive")
        if "mes_hive" in colunas_schema:
            lf = lf.drop("mes_hive")

        lf.sink_parquet(
            caminho_destino_local,
            compression="zstd",
            row_group_size=250_000
        )
        print(f"   ✅ Salvo: {caminho_relativo.split('/')[-1]}")
        
    except Exception as e:
        print(f"   ❌ Erro em {caminho_relativo}: {e}")

# --- LOOP PRINCIPAL ---
def gerar_backups_modulares():
    print("📦 Iniciando extração e compactação por tabela...\n")
    
    for tabela in TABELAS:
        print("=" * 50)
        print(f"🚀 INICIANDO TABELA: {tabela.upper()}")
        print("=" * 50)
        
        pasta_base_minio = f"{CONTAINER}/{tabela}"
        caminhos_minio = fs_minio.glob(f"{pasta_base_minio}/**/*.parquet")
        
        if not caminhos_minio:
            print(f"⚠️ Nenhum arquivo encontrado para '{tabela}'. Pulando...\n")
            continue

        print(f"📊 {len(caminhos_minio)} arquivos encontrados. Baixando e limpando...")

        # 1. Baixa os arquivos da tabela atual
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(processar_backup_local, caminhos_minio)

        # 2. Prepara os caminhos para o Zip
        # A pasta que queremos zipar é: ./bkp_temporario/bronze/nome_da_tabela
        pasta_da_tabela_local = os.path.join(PASTA_BACKUP_BASE, CONTAINER, tabela)
        nome_do_zip = f"./backup_{tabela}" # Vai gerar backup_caged_mov.zip, backup_rais.zip, etc.

        print(f"\n🗜️ Todos os downloads de '{tabela}' concluídos! Gerando o arquivo .zip...")
        
        # 3. Compacta a tabela atual
        shutil.make_archive(
            base_name=nome_do_zip, 
            format="zip", 
            root_dir=pasta_da_tabela_local
        )

        print(f"🎉 Zip da tabela '{tabela}' gerado com sucesso: {nome_do_zip}.zip\n")

    print("-" * 50)
    print("🏁 PROCESSO DE BACKUP 100% FINALIZADO!")
    print(f"Você já pode pegar os arquivos .zip gerados e subir no Google Drive.")

if __name__ == "__main__":
    gerar_backups_modulares()