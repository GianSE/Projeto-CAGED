import subprocess
import time
import sys
import os
import hashlib
import json
import tempfile
import ctypes
import re

# --- [CONFIGURAÇÃO DE PERSISTÊNCIA] ---
METADATA_DIR = r"C:\deploy_metadata"
HASH_STORAGE = os.path.join(METADATA_DIR, "worker_state.json")

if not os.path.exists(METADATA_DIR):
    os.makedirs(METADATA_DIR, exist_ok=True)

# --- [BYPASS DE CREDENCIAIS DOCKER] ---
os.environ["DOCKER_BUILDKIT"] = "1"
os.environ["COMPOSE_DOCKER_CLI_BUILD"] = "1"

def get_long_path(path):
    try:
        buf = ctypes.create_unicode_buffer(500)
        ctypes.windll.kernel32.GetLongPathNameW(path, buf, 500)
        return buf.value
    except: return path

fake_config_dir = get_long_path(tempfile.mkdtemp())
with open(os.path.join(fake_config_dir, "config.json"), "w") as f:
    json.dump({ "credsStore": "", "credsHelpers": {}, "auths": {} }, f)

os.environ["DOCKER_CONFIG"] = fake_config_dir
DOCKER_CMD = f'docker --config "{fake_config_dir}"'
# -----------------------------------------------------------

# Reduzi o timeout apenas por segurança, mas o Kill será imediato
TIMEOUT_DRAIN = 10 
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

FILES_TO_MONITOR = [
    os.path.join(project_root, "worker", "requirements.txt"),
    os.path.join(project_root, "worker", "Dockerfile")
]

def get_file_hash(path):
    if not os.path.exists(path): return None
    hasher = hashlib.md5()
    with open(path, 'rb') as f: hasher.update(f.read())
    return hasher.hexdigest()

def check_if_build_needed():
    current_hashes = {os.path.basename(p): get_file_hash(p) for p in FILES_TO_MONITOR}
    stored_hashes = {}
    if os.path.exists(HASH_STORAGE):
        try:
            with open(HASH_STORAGE, 'r') as f: stored_hashes = json.load(f)
        except: pass
    return current_hashes != stored_hashes, current_hashes

def save_new_hashes(hashes):
    with open(HASH_STORAGE, 'w') as f: json.dump(hashes, f, indent=4)

def run_command(command, description):
    print(f"[EXEC] {description}...")
    result = subprocess.run(command, shell=True, env=os.environ)
    return result.returncode == 0

def extract_image_name():
    try:
        # Busca a imagem no compose do worker
        with open(os.path.join(project_root, "worker", "docker-compose.yml"), "r") as f:
            match = re.search(r'image:\s+([^\s]+)', f.read())
            if match: return match.group(1)
    except: pass
    return "custom-worker:latest"

def force_kill_container(color, timeout=10):
    """Substitui a drenagem suave por um encerramento forçado (SIGKILL)."""
    try:
        cmd_ps = f'{DOCKER_CMD} ps --filter "name=worker-{color}" -q'
        ids = subprocess.run(cmd_ps, shell=True, capture_output=True, text=True).stdout.split()
    except: ids = []
    
    if not ids: return

    print(f"[KILL] Finalizando {color} imediatamente (SIGKILL)...")
    # Removido o --signal=SIGTERM para usar o padrão do docker kill (SIGKILL)
    subprocess.run(f"{DOCKER_CMD} kill {' '.join(ids)}", shell=True)
    
    # Pequena verificação para garantir que o Docker removeu o processo
    start = time.time()
    while True:
        rem = subprocess.run(cmd_ps, shell=True, capture_output=True, text=True).stdout.strip()
        if not rem or (time.time() - start) > timeout: break
        time.sleep(2)

def rebuild_blue_green():
    docker_dir = os.path.join(project_root, "worker")
    os.chdir(docker_dir)

    needs_build, new_hashes = check_if_build_needed()
    check_any = subprocess.run(f'{DOCKER_CMD} ps --filter "name=worker-" -q', shell=True, capture_output=True, text=True).stdout.strip()

    if not needs_build and "--force" not in sys.argv and check_any:
        print("[SKIP] Cache detectado. Pulando build.")
        return

    is_blue = subprocess.run(f'{DOCKER_CMD} ps --filter "name=worker-blue" -q', shell=True, capture_output=True, text=True).stdout.strip()
    new_color = "green" if is_blue else "blue"
    curr_color = "blue" if is_blue else "green"
    
    print(f"[INFO] Ciclo: {curr_color or 'Nenhum'} -> {new_color}")

    if needs_build or "--force" in sys.argv:
        image_name = extract_image_name()
        print(f"[BUILD] Gerando imagem: {image_name}")
        cmd_build = f"{DOCKER_CMD} build -t {image_name} -f Dockerfile .."
        if not run_command(cmd_build, "Docker Build"): sys.exit(1)
        save_new_hashes(new_hashes)

    cmd_up = f"{DOCKER_CMD} compose -p worker-{new_color} up -d --remove-orphans"
    if not run_command(cmd_up, f"Subindo {new_color}"): sys.exit(1)

    print("[WAIT] Aguardando estabilizacao (5s)...")
    time.sleep(5) # Reduzi o tempo de espera para dev

    if check_any and curr_color:
        # Chamada da nova função de Kill
        force_kill_container(curr_color)
        run_command(f"{DOCKER_CMD} compose -p worker-{curr_color} down", f"Limpando {curr_color}")

    subprocess.run(f"{DOCKER_CMD} image prune -f", shell=True, capture_output=True)
    print(f"[OK] {new_color.upper()} esta pronto!")

if __name__ == "__main__":
    rebuild_blue_green()