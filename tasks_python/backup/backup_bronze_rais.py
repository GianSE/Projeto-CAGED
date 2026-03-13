import os
import shutil
import s3fs
import polars as pl
import concurrent.futures
import zipfile

# --- CONFIGURAÇÕES ---
TABELAS = ["rais"] 
CONTAINER = "bronze"

PASTA_BACKUP_BASE = "./bkp_temporario" 
PASTA_DESTINO_ZIPS = "./tcc_backup_bronze" 

MAX_WORKERS = 6 
LIMITE_ZIP_GB = 4 # Tamanho máximo de cada parte do Zip em Gigabytes

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

# --- FUNÇÃO DA THREAD (DOWNLOAD) ---
def processar_backup_local(caminho_relativo):
    caminho_origem = f"s3://{caminho_relativo}"
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

# --- FUNÇÃO INTELIGENTE DE ZIP ---
def zipar_em_partes(tabela, pasta_origem):
    limite_bytes = LIMITE_ZIP_GB * 1024 * 1024 * 1024
    
    # Mapeia todos os arquivos que acabamos de baixar
    arquivos_para_zipar = []
    for root, _, files in os.walk(pasta_origem):
        for file in files:
            arquivos_para_zipar.append(os.path.join(root, file))
            
    if not arquivos_para_zipar:
        return

    parte = 1
    tamanho_atual = 0
    zip_atual = None

    def abrir_novo_zip(num_parte):
        nome_zip = os.path.join(PASTA_DESTINO_ZIPS, f"backup_{tabela}_parte{num_parte}.zip")
        # ZIP_STORED: Apenas empacota (muito rápido), pois o parquet já está comprimido
        return zipfile.ZipFile(nome_zip, 'w', zipfile.ZIP_STORED)

    zip_atual = abrir_novo_zip(parte)

    for filepath in arquivos_para_zipar:
        tamanho_arquivo = os.path.getsize(filepath)

        # Se colocar esse arquivo vai passar de 5GB, fecha o zip atual e abre o próximo
        if (tamanho_atual + tamanho_arquivo) > limite_bytes and tamanho_atual > 0:
            zip_atual.close()
            print(f"   💾 Parte {parte} concluída! Iniciando parte {parte + 1}...")
            parte += 1
            zip_atual = abrir_novo_zip(parte)
            tamanho_atual = 0

        # Calcula o caminho relativo para manter a estrutura de pastas bonitinha dentro do Zip
        caminho_interno_zip = os.path.relpath(filepath, pasta_origem)
        zip_atual.write(filepath, caminho_interno_zip)
        tamanho_atual += tamanho_arquivo

    if zip_atual:
        zip_atual.close()
        print(f"   💾 Parte {parte} concluída!")

# --- LOOP PRINCIPAL ---
def gerar_backups_modulares():
    print(f"📦 Iniciando extração com limite de {LIMITE_ZIP_GB}GB por Zip...\n")
    os.makedirs(PASTA_DESTINO_ZIPS, exist_ok=True)
    
    for tabela in TABELAS:
        print("\n" + "=" * 50)
        print(f"🚀 INICIANDO TABELA: {tabela.upper()}")
        print("=" * 50)
        
        pasta_base_minio = f"{CONTAINER}/{tabela}"
        caminhos_minio = fs_minio.glob(f"{pasta_base_minio}/**/*.parquet")
        
        if not caminhos_minio:
            print(f"⚠️ Nenhum arquivo encontrado para '{tabela}'. Pulando...")
            continue

        print(f"📊 {len(caminhos_minio)} arquivos encontrados. Baixando para o HD...\n")

        # 1. Baixa tudo
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(processar_backup_local, caminhos_minio)

        pasta_da_tabela_local = os.path.join(PASTA_BACKUP_BASE, CONTAINER, tabela)

        print(f"\n🗜️ Downloads concluídos! Empacotando em fatias de {LIMITE_ZIP_GB}GB...")
        
        # 2. Chama a nova função que corta em partes
        zipar_em_partes(tabela, pasta_da_tabela_local)

        print(f"🎉 Zips da tabela '{tabela}' gerados com sucesso!")

    # --- LIMPEZA FINAL ---
    print("\n" + "-" * 50)
    print("🧹 Esvaziando a pasta temporária do HD...")
    if os.path.exists(PASTA_BACKUP_BASE):
        shutil.rmtree(PASTA_BACKUP_BASE)
        
    print("🏁 PROCESSO 100% FINALIZADO!")
    print(f"Seus Zips fatiados estão na pasta: {PASTA_DESTINO_ZIPS}")

if __name__ == "__main__":
    gerar_backups_modulares()