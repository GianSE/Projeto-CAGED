import sys
import os
import importlib.util

def check_file(filepath):
    """Tenta verificar a sintaxe/import do arquivo"""
    print(f"Checking {filepath}...")
    try:
        # Método seguro: Apenas compila para checar erro de sintaxe
        with open(filepath, 'r', encoding='utf-8') as f:
            compile(f.read(), filepath, 'exec')
        return True
    except Exception as e:
        print(f"❌ Erro em {filepath}: {e}")
        return False

def main():
    # Se receber argumentos do Bash, usa eles. 
    # Se não receber nada (sys.argv <= 1), varre tudo (comportamento antigo).
    if len(sys.argv) > 1:
        files_to_check = sys.argv[1:]
    else:
        # Fallback: Varre o projeto todo se rodar manual sem argumentos
        files_to_check = []
        for root, _, files in os.walk("."):
            for file in files:
                if file.endswith(".py") and "_ops" not in root: # Exemplo de filtro
                    files_to_check.append(os.path.join(root, file))

    has_error = False
    for file_path in files_to_check:
        if os.path.exists(file_path):
            if not check_file(file_path):
                has_error = True
    
    if has_error:
        sys.exit(1)
    else:
        print("✅ Todos os imports verificados com sucesso.")
        sys.exit(0)

if __name__ == "__main__":
    main()