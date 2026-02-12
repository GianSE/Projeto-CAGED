import shutil
import os
import subprocess
import sys

# --- CONFIGURAÇÃO DE CAMINHOS ---
# Pega o diretório onde ESTE script está (_ops)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Pega o diretório pai (raiz do projeto)
project_root = os.path.dirname(current_dir)

hooks_src = os.path.join(current_dir, "hooks")          # _ops/hooks
hooks_dst = os.path.join(project_root, ".git", "hooks") # .git/hooks

# Lista de hooks para ativar
hooks_to_sync = ["pre-commit"] 

print("🚀 Iniciando Setup do Ambiente de Desenvolvimento...\n")
print(f"📂 Raiz do projeto detectada: {project_root}")

# --- 1. CONFIGURAÇÃO DOS HOOKS ---
print("\n🔄 [1/3] Sincronizando Git Hooks...")

if not os.path.exists(hooks_dst):
    try: 
        os.makedirs(hooks_dst)
    except Exception: 
        pass

if not os.path.exists(hooks_src):
    print(f"   ⚠️  Pasta de hooks fonte não encontrada em: {hooks_src}")
    print("       Crie a pasta '_ops/hooks' e coloque o arquivo 'pre-commit' lá.")
else:
    for hook in ["pre-commit", "pre-push"]:
        src = os.path.join(hooks_src, hook)
        dst = os.path.join(hooks_dst, hook)
        
        if hook in hooks_to_sync:
            if os.path.exists(src):
                shutil.copy(src, dst)
                print(f"   ✅ {hook} atualizado.")
            else:
                print(f"   ⚠️  Arquivo fonte '{hook}' não encontrado em _ops/hooks.")
        elif os.path.exists(dst):
            try: 
                os.remove(dst) 
                print(f"   🗑️  {hook} removido (não listado para sync).")
            except Exception: 
                pass

# --- 2. CONFIGURAÇÃO DO GIT ---
print("\n⚙️  [2/3] Ajustando Git...")
try:
    subprocess.run(["git", "config", "core.safecrlf", "false"], check=True)
    subprocess.run(["git", "config", "core.autocrlf", "input"], check=True)
    print("   ✅ Git CRLF OK!")
except Exception: 
    print("   ⚠️  Falha ao configurar Git.")

# --- 3. CONFIGURAÇÃO DO ALIAS 'WORKER' ---
print("\n⌨️  [3/3] Configurando atalho 'worker'...")

try:
    if sys.platform == "win32":
        # Pede ao PowerShell onde fica o arquivo de perfil
        result = subprocess.run(
            ["powershell", "-Command", "echo $PROFILE"], 
            capture_output=True, text=True
        )
        profile_path = result.stdout.strip()

        if profile_path:
            os.makedirs(os.path.dirname(profile_path), exist_ok=True)
            
            # Função PowerShell para entrar no container
            worker_func = """
function worker {
    $id = docker ps --filter "name=worker" --format "{{.Names}}" | Select-Object -First 1
    if ($id) {
        Write-Host "🚀 Entrando em: $id" -ForegroundColor Cyan
        docker exec -it $id /bin/bash
    } else {
        Write-Host "❌ Nenhum worker rodando." -ForegroundColor Red
    }
}
"""
            # Lê o arquivo atual para evitar duplicatas
            content = ""
            if os.path.exists(profile_path):
                with open(profile_path, "r", encoding="utf-8-sig") as f:
                    content = f.read()

            if "function worker" not in content:
                with open(profile_path, "a", encoding="utf-8-sig") as f:
                    f.write("\n" + worker_func)
                print(f"   ✅ Alias gravado em: {profile_path}")
                print("      👉 Reinicie seu terminal para usar o comando 'worker'")
            else:
                print("   ⚡ Alias já existe.")
        else:
            print("   ⚠️  Não consegui localizar o perfil do PowerShell.")
    else:
        print("   ℹ️  Pulo configuração de alias (Linux/Mac).")

except Exception as e:
    print(f"   ⚠️  Erro ao configurar alias: {e}")

print("\n✨ Setup finalizado! 🛡️")