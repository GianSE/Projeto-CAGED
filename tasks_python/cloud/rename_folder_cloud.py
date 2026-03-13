import re
import env
from azure.storage.filedatalake import DataLakeServiceClient

# Usa a mesma variável segura que você já criou!
AZURE_CONNECTION_STRING = env.string_azure
CONTAINER_NAME = "bronze"

def arrumar_ordem_dos_meses():
    try:
        print("Conectando ao Data Lake...")
        # Note que agora usamos o DataLakeServiceClient no lugar do BlobServiceClient
        service_client = DataLakeServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        file_system_client = service_client.get_file_system_client(CONTAINER_NAME)

        # Mapeia tudo o que existe na sua camada bronze
        paths = file_system_client.get_paths()
        
        pastas_renomeadas = 0
        for path in paths:
            # Filtra apenas o que for diretório (pasta)
            if path.is_directory:
                # Usa Regex para encontrar pastas que terminam EXATAMENTE de "mes_hive=1" até "mes_hive=9"
                match = re.search(r'(.*mes_hive=)([1-9])$', path.name)
                
                if match:
                    caminho_antigo = path.name
                    # Monta o novo nome enfiando o '0' no meio (ex: mes_hive= + 0 + 1)
                    caminho_novo = f"{match.group(1)}0{match.group(2)}"
                    
                    print(f"Ajustando: {caminho_antigo}  -->  {caminho_novo}")
                    
                    # Pega a pasta antiga e aplica o rename
                    dir_client = file_system_client.get_directory_client(caminho_antigo)
                    # A API exige passar o nome do container na frente do caminho novo
                    dir_client.rename_directory(new_name=f"{CONTAINER_NAME}/{caminho_novo}")
                    
                    pastas_renomeadas += 1

        print(f"\n✅ Perfeito! {pastas_renomeadas} pastas foram corrigidas. Agora tudo vai ficar na ordem certinha (01, 02... 10, 11).")
        
    except Exception as e:
        print(f"❌ Deu erro: {e}")

if __name__ == "__main__":
    arrumar_ordem_dos_meses()