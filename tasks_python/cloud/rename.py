import os
import re
from pathlib import Path

def renomear_meses_com_zero(diretorio_base):
    print(f"Iniciando varredura em: {diretorio_base}")
    pastas_renomeadas = 0

    # topdown=False garante que renomeamos as filhas antes dos pais
    for root, dirs, files in os.walk(diretorio_base, topdown=False):
        for nome_pasta in dirs:
            caminho_antigo = Path(root) / nome_pasta
            
            # O Regex ^mes_hive=([1-9])$ procura EXATAMENTE por pastas que 
            # comecem com 'mes_hive=' e terminem com um único número de 1 a 9
            match = re.match(r'^mes_hive=([1-9])$', nome_pasta)
            
            if match:
                # Extrai apenas o número (ex: '1', '2', etc.)
                numero_mes = match.group(1)
                
                # Monta o novo nome com o zero na frente
                novo_nome = f"mes_hive=0{numero_mes}"
                caminho_novo = Path(root) / novo_nome
                
                print(f"Ajustando localmente: {nome_pasta}  -->  {novo_nome}")
                
                # Aplica o rename no seu Windows
                caminho_antigo.rename(caminho_novo)
                pastas_renomeadas += 1

    print(f"\n✅ Sucesso! {pastas_renomeadas} pastas foram renomeadas localmente para o padrão 01, 02, etc.")

if __name__ == "__main__":
    # Mantive o caminho exato do seu volume do MinIO
    caminho_seu_bucket_local = "D:/lakehouse/minio/minio_data/bronze/"
    
    renomear_meses_com_zero(caminho_seu_bucket_local)