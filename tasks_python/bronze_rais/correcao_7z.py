import os
import subprocess

# --- CONFIGURAÇÕES ---
SEVEN_ZIP_EXE = r"C:\Program Files\7-Zip\7z.exe"
ARQUIVO_ALVO = r"D:\lakehouse\minio\minio_data\bronze\rais\ano=2013\BA2013.7z"
# Nome do arquivo de saída (como estamos lendo o fluxo, precisamos dar o nome manualmente)
ARQUIVO_FINAL = r"D:\lakehouse\minio\minio_data\bronze\rais\ano=2013\resgate\BA2013_resgatado.txt"

def resgatar_via_stream():
    if not os.path.exists(SEVEN_ZIP_EXE):
        print("❌ Erro: 7-Zip não encontrado.")
        return

    # Cria a pasta de destino
    os.makedirs(os.path.dirname(ARQUIVO_FINAL), exist_ok=True)

    print(f"🚑 Iniciando resgate via STREAM de: {ARQUIVO_ALVO}")
    print("⏳ O Python vai capturar os dados diretamente do 7-Zip...")

    # Comando: 'e' = extrair, '-so' = escrever no output padrão (stream)
    cmd = [SEVEN_ZIP_EXE, "e", ARQUIVO_ALVO, "-so"]

    try:
        # Abre o processo do 7-Zip
        processo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Abre nosso arquivo de destino para escrita binária
        bytes_salvos = 0
        with open(ARQUIVO_FINAL, 'wb') as f_out:
            while True:
                # Lê blocos de 1MB do 7-Zip
                chunk = processo.stdout.read(1024 * 1024)
                
                if not chunk:
                    break # Fim dos dados
                
                f_out.write(chunk)
                bytes_salvos += len(chunk)
                
                # Mostra progresso simples (em MB)
                print(f"\r📥 Baixados/Recuperados: {bytes_salvos / (1024*1024):.2f} MB", end="")

        print(f"\n\n✅ Processo finalizado!")
        print(f"📂 Arquivo salvo em: {ARQUIVO_FINAL}")
        
        # Verifica se o 7-Zip reclamou de algo (provavelmente sim)
        _, stderr = processo.communicate()
        if stderr:
            print("⚠️ O 7-Zip reportou erros (esperado):")
            print(stderr.decode('utf-8', errors='ignore'))
            
        # Limpeza da última linha (opcional, mas recomendado)
        limpar_ultima_linha(ARQUIVO_FINAL)

    except Exception as e:
        print(f"\n❌ Erro crítico no Python: {e}")

def limpar_ultima_linha(caminho_arquivo):
    print("🧹 Verificando a integridade da última linha...")
    try:
        with open(caminho_arquivo, 'rb+') as f:
            f.seek(0, os.SEEK_END)
            tamanho = f.tell()
            if tamanho == 0: return

            # Retrocede para achar o último \n
            pos = tamanho - 1
            while pos > 0 and f.read(1) != b'\n':
                pos -= 1
                f.seek(pos, os.SEEK_SET)
            
            if pos > 0:
                f.seek(pos, os.SEEK_SET)
                f.truncate()
                print("✨ Última linha (provavelmente quebrada) removida com sucesso.")
            else:
                print("⚠️ Arquivo muito curto ou sem quebras de linha.")
    except Exception as e:
        print(f"⚠️ Erro ao limpar linha: {e}")

if __name__ == "__main__":
    resgatar_via_stream()