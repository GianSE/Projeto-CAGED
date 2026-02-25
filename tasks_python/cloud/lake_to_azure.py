import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient
from env import string_azure

# 1. Configurações
AZURE_CONNECTION_STRING = string_azure
CONTAINER_NAME = "bronze"

# O caminho exato da sua pasta bronze local
LOCAL_BASE_DIR = r"D:\lakehouse\minio\minio_data\bronze"

# As pastas que você quer pular
PASTAS_PARA_PULAR = ["caged_ajustes", "caged_for", "caged_exc", "caged_mov", "menor_preco", "rais", "rais_estab"] 

# --- A MÁGICA DA VELOCIDADE ---
# Quantos arquivos subir AO MESMO TEMPO (Recomendo 10 ou 20 para internet residencial)
MAX_WORKERS = 10 

# 2. A Função do "Trabalhador" (O que cada caixa do supermercado faz)
def upload_single_file(arquivo_local, caminho_base, blob_service_client):
    caminho_relativo = arquivo_local.relative_to(caminho_base)
    destination_blob_path = caminho_relativo.as_posix()
    
    # 1º Filtro: Pastas ignoradas
    if caminho_relativo.parts[0] in PASTAS_PARA_PULAR:
        return "ignorado_pasta"

    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME, 
        blob=destination_blob_path
    )
    
    # 2º Filtro: Já existe na nuvem?
    if blob_client.exists():
        return "ignorado_nuvem"
        
    print(f"⬆️ Subindo: {destination_blob_path}")
    
    # 3º Ação: Faz o upload com Retry (Teimosia)
    max_tentativas = 5
    for tentativa in range(max_tentativas):
        try:
            with open(arquivo_local, "rb") as data:
                # Timeout de 120s para dar fôlego à conexão
                blob_client.upload_blob(data, overwrite=False, connection_timeout=120)
            return "sucesso"
        except Exception as e:
            if tentativa < max_tentativas - 1:
                time.sleep(5) # Espera 5s e tenta de novo
            else:
                print(f"❌ Falha definitiva no {destination_blob_path}. Erro: {e}")
                return "erro"

# 3. O Gerenciador (Quem organiza a fila)
def upload_all_to_datalake():
    try:
        print("Conectando ao Azure...")
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        caminho_base = Path(LOCAL_BASE_DIR)
        
        todos_arquivos = [f for f in caminho_base.rglob("*") if f.is_file()]
        print(f"Encontrados {len(todos_arquivos)} arquivos. Ligando o motor turbo com {MAX_WORKERS} conexões simultâneas...\n")
        
        # Dicionário para guardar o placar final
        placar = {"sucesso": 0, "ignorado_pasta": 0, "ignorado_nuvem": 0, "erro": 0}

        # --- A EXECUÇÃO EM PARALELO ---
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Entrega a lista de arquivos para os trabalhadores
            futuros = [
                executor.submit(upload_single_file, arquivo, caminho_base, blob_service_client) 
                for arquivo in todos_arquivos
            ]
            
            # Conforme os trabalhadores vão terminando, a gente anota no placar
            for futuro in as_completed(futuros):
                resultado = futuro.result()
                placar[resultado] += 1
                
        print("\n✅ Sincronização Turbo concluída!")
        print(f"📊 Resumo: {placar['sucesso']} enviados | {placar['ignorado_nuvem']} já existiam na Azure.")
        print(f"🛡️ {placar['ignorado_pasta']} ignorados pela regra de pastas | {placar['erro']} falharam.")
        
    except Exception as e:
        print(f"❌ Deu erro crítico no script principal: {e}")

if __name__ == "__main__":
    upload_all_to_datalake()