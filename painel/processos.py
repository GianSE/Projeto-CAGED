"""
Controle do processo de extração a partir do painel (botão play/stop).

Só permite UM processo pesado por vez — a lição da sessão anterior foi rodar
extração + build de silver concorrentes numa máquina com pouca RAM livre e
derrubar tudo (SIGSEGV por pressão de memória). O painel vira o único ponto de
disparo, então essa regra fica garantida em vez de depender de disciplina.

O subprocesso roda com PYTHONUNBUFFERED=1 e stdout/stderr redirecionados para
um arquivo de log — sem isso, no Windows, a saída fica bufferizada em bloco
até o processo terminar, e o log tail do painel ficaria sempre vazio.
"""
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from extracao_ftp.config_extracao import DIR_LOGS

RAIZ = Path(__file__).resolve().parents[1]
DIR_TASKS_PYTHON = RAIZ / "tasks_python"

# Interpretador que roda os JOBS PESADOS (extração, silver), não o painel.
#
# São ambientes diferentes de propósito: o painel só precisa de flask, duckdb
# e s3fs; os jobs precisam de py7zr, pyarrow, openpyxl e companhia. Usar
# `sys.executable` aqui apontaria para o venv do painel, que não tem essas
# dependências, e os botões falhariam na hora de disparar.
#
# PYTHON_JOBS permite apontar para outro lugar (útil se o venv da raiz mudar
# de caminho); o padrão é o .venv da raiz do projeto.
_PADRAO_JOBS = RAIZ / ".venv" / ("Scripts" if os.name == "nt" else "bin") / \
    ("python.exe" if os.name == "nt" else "python")
PYTHON_JOBS = os.getenv("PYTHON_JOBS") or str(_PADRAO_JOBS)

# Uma pasta com um arquivo por execução — nunca sobrescreve, dá pra voltar
# depois e ver exatamente o que aconteceu numa carga específica.
DIR_LOGS_EXECUCOES = DIR_LOGS / "execucoes"

_lock = threading.Lock()
_processo: subprocess.Popen | None = None
_comando: list[str] = []
_caminho_log: Path | None = None
_tipo: str = "extração"
_iniciado_em: float | None = None
_finalizado_em: float | None = None
_codigo_saida: int | None = None


ARQUIVO_JOB_ATIVO = DIR_LOGS / "job_ativo.json"


def _salvar_job_ativo(pid: int, comando: list[str], tipo: str, caminho_log: Path) -> None:
    """
    Registra o job em disco para que um restart do painel não perca o rastro.

    Sem isso, reiniciar o painel com uma carga em andamento fazia o card voltar
    a dizer "Ocioso" — o processo continuava rodando, mas o handle vivia só na
    memória do painel.
    """
    try:
        import psutil

        DIR_LOGS.mkdir(parents=True, exist_ok=True)
        ARQUIVO_JOB_ATIVO.write_text(json.dumps({
            "pid": pid,
            "comando": comando,
            "tipo": tipo,
            "log": str(caminho_log),
            "iniciado_em": time.time(),
            # Guarda o instante de criação do processo: PID no Windows é
            # reciclado, e sem isso um PID reaproveitado por outro programa
            # seria confundido com o nosso job.
            "criado_em": psutil.Process(pid).create_time(),
        }), encoding="utf-8")
    except Exception:
        pass


def _limpar_job_ativo() -> None:
    try:
        ARQUIVO_JOB_ATIVO.unlink(missing_ok=True)
    except Exception:
        pass


def _adotar_job_ativo() -> dict | None:
    """Reassume um job registrado em disco, se o processo ainda estiver vivo."""
    try:
        import psutil

        dados = json.loads(ARQUIVO_JOB_ATIVO.read_text(encoding="utf-8"))
        proc = psutil.Process(dados["pid"])
        # Compara o instante de criação para descartar PID reciclado.
        if abs(proc.create_time() - dados["criado_em"]) > 1:
            return None
        return dados
    except Exception:
        return None


def _caminho_log_mais_recente() -> Path | None:
    if not DIR_LOGS_EXECUCOES.exists():
        return None
    arquivos = sorted(DIR_LOGS_EXECUCOES.glob("*.log"))
    return arquivos[-1] if arquivos else None


def _montar_comando(dataset: list[str], ano_inicio: int, ano_fim: int | None = None,
                    tabelas: list[str] | None = None, forcar: bool = False) -> list[str]:
    comando = [
        PYTHON_JOBS, "-m", "extracao_ftp.run_extracao",
        "--dataset", *dataset,
        "--ano-inicio", str(ano_inicio),
    ]
    if ano_fim is not None:
        comando += ["--ano-fim", str(ano_fim)]
    if tabelas:
        comando += ["--tabela", *tabelas]
    if forcar:
        comando.append("--forcar")
    return comando


def _lancar(comando: list[str], rotulo: str, tipo: str) -> dict:
    """
    Sobe um subprocesso pesado. Recusa se já houver um rodando — a regra de
    "um job pesado por vez" vale para extração E construção de silver, já que
    ambos disputam a mesma memória (esta máquina já derrubou tudo por OOM
    rodando os dois juntos).
    """
    global _processo, _comando, _caminho_log, _tipo
    global _iniciado_em, _finalizado_em, _codigo_saida

    with _lock:
        if _processo is not None and _processo.poll() is None:
            return {"ok": False, "erro": f"Já existe um job em andamento ({_tipo})."}

        DIR_LOGS_EXECUCOES.mkdir(parents=True, exist_ok=True)
        carimbo = datetime.now().strftime("%Y%m%d_%H%M%S")
        caminho_log = DIR_LOGS_EXECUCOES / f"{carimbo}_{rotulo}.log"

        ambiente = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
        log = open(caminho_log, "w", encoding="utf-8", errors="replace")

        _processo = subprocess.Popen(
            comando, cwd=str(DIR_TASKS_PYTHON), env=ambiente,
            stdout=log, stderr=subprocess.STDOUT,
        )
        _comando = comando
        _caminho_log = caminho_log
        _tipo = tipo
        _iniciado_em = time.time()
        _finalizado_em = None
        _codigo_saida = None

        _salvar_job_ativo(_processo.pid, comando, tipo, caminho_log)
        return {"ok": True, "pid": _processo.pid, "comando": " ".join(comando)}


def iniciar(dataset: list[str], ano_inicio: int, ano_fim: int | None = None,
           tabelas: list[str] | None = None, forcar: bool = False,
           rotulo: str = "extracao") -> dict:
    """Sobe o subprocesso de extração (FTP -> bronze)."""
    comando = _montar_comando(dataset, ano_inicio, ano_fim, tabelas, forcar)
    return _lancar(comando, rotulo, "extração")


def iniciar_silver(tabelas: list[str], camada: str = "caged", forcar: bool = False) -> dict:
    """Sobe o subprocesso de construção da silver (bronze -> silver traduzida)."""
    modulo = "silver_caged.construir_silver" if camada == "caged" else "silver_rais.construir_silver"
    comando = [PYTHON_JOBS, "-m", modulo, "--tabela", *tabelas]
    if forcar:
        comando.append("--forcar")
    return _lancar(comando, f"silver-{'-'.join(tabelas)}"[:60], "silver")


def parar() -> dict:
    """Pede para o subprocesso terminar (SIGTERM); força depois de alguns segundos."""
    global _codigo_saida, _finalizado_em

    with _lock:
        # Job adotado após restart do painel: não há handle de subprocesso,
        # então encerra pelo PID registrado em disco.
        if _processo is None:
            adotado = _adotar_job_ativo()
            if not adotado:
                return {"ok": False, "erro": "Nenhum job rodando."}
            try:
                import psutil

                proc = psutil.Process(adotado["pid"])
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except psutil.TimeoutExpired:
                    proc.kill()
            except Exception as e:
                return {"ok": False, "erro": f"não consegui encerrar o job: {str(e)[:150]}"}
            _limpar_job_ativo()
            return {"ok": True, "codigo_saida": None}

        if _processo.poll() is not None:
            return {"ok": False, "erro": "Nenhum job rodando."}

        _processo.terminate()
        try:
            _processo.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _processo.kill()
            _processo.wait(timeout=10)

        _codigo_saida = _processo.returncode
        _finalizado_em = time.time()
        _limpar_job_ativo()
        return {"ok": True, "codigo_saida": _codigo_saida}


def _tail(caminho: Path, n_linhas: int = 40) -> list[str]:
    if not caminho.exists():
        return []
    try:
        with open(caminho, "r", encoding="utf-8", errors="replace") as f:
            linhas = f.readlines()
        return [l.rstrip("\n") for l in linhas[-n_linhas:]]
    except Exception:
        return []


def listar_execucoes() -> list[dict]:
    """Histórico de execuções (mais recente primeiro) — cada uma com seu log próprio."""
    if not DIR_LOGS_EXECUCOES.exists():
        return []
    arquivos = sorted(DIR_LOGS_EXECUCOES.glob("*.log"), reverse=True)
    return [
        {"nome": a.name, "tamanho_kb": round(a.stat().st_size / 1024, 1),
         "modificado_em": a.stat().st_mtime}
        for a in arquivos
    ]


def status() -> dict:
    """Estado atual do processo controlado pelo painel, para exibir no dashboard."""
    global _codigo_saida, _finalizado_em

    with _lock:
        rodando = _processo is not None and _processo.poll() is None
        if _processo is not None and not rodando and _codigo_saida is None:
            _codigo_saida = _processo.returncode
            _finalizado_em = time.time()
            _limpar_job_ativo()

        # Painel recém-reiniciado não tem handle do processo em memória, mas o
        # job pode continuar rodando — readota pelo registro em disco.
        if _processo is None:
            adotado = _adotar_job_ativo()
            if adotado:
                caminho_adotado = Path(adotado["log"])
                return {
                    "rodando": True,
                    "adotado": True,
                    "tipo": adotado.get("tipo", "job"),
                    "pid": adotado["pid"],
                    "comando": " ".join(adotado.get("comando", [])),
                    "log_arquivo": caminho_adotado.name,
                    "iniciado_em": adotado.get("iniciado_em"),
                    "finalizado_em": None,
                    "codigo_saida": None,
                    "log_tail": _tail(caminho_adotado, 60),
                }

        # Fora de uma execução ativa, ainda mostra a cauda do último log —
        # útil pra ver como uma carga terminou sem precisar abrir o arquivo.
        caminho = _caminho_log if _processo is not None else _caminho_log_mais_recente()

        return {
            "rodando": rodando,
            "tipo": _tipo,
            "pid": _processo.pid if _processo else None,
            "comando": " ".join(_comando) if _comando else None,
            "log_arquivo": caminho.name if caminho else None,
            "iniciado_em": _iniciado_em,
            "finalizado_em": _finalizado_em,
            "codigo_saida": _codigo_saida,
            "log_tail": _tail(caminho, 60) if caminho else [],
        }
