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
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from extracao_ftp.config_extracao import DIR_LOGS

DIR_TASKS_PYTHON = Path(__file__).resolve().parents[1]
# Uma pasta com um arquivo por execução — nunca sobrescreve, dá pra voltar
# depois e ver exatamente o que aconteceu numa carga específica.
DIR_LOGS_EXECUCOES = DIR_LOGS / "execucoes"

_lock = threading.Lock()
_processo: subprocess.Popen | None = None
_comando: list[str] = []
_caminho_log: Path | None = None
_iniciado_em: float | None = None
_finalizado_em: float | None = None
_codigo_saida: int | None = None


def _caminho_log_mais_recente() -> Path | None:
    if not DIR_LOGS_EXECUCOES.exists():
        return None
    arquivos = sorted(DIR_LOGS_EXECUCOES.glob("*.log"))
    return arquivos[-1] if arquivos else None


def _montar_comando(dataset: list[str], ano_inicio: int, ano_fim: int | None = None,
                    tabelas: list[str] | None = None, forcar: bool = False) -> list[str]:
    comando = [
        sys.executable, "-m", "extracao_ftp.run_extracao",
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


def iniciar(dataset: list[str], ano_inicio: int, ano_fim: int | None = None,
           tabelas: list[str] | None = None, forcar: bool = False,
           rotulo: str = "extracao") -> dict:
    """Sobe o subprocesso de extração. Recusa se já houver um rodando."""
    global _processo, _comando, _caminho_log, _iniciado_em, _finalizado_em, _codigo_saida

    with _lock:
        if _processo is not None and _processo.poll() is None:
            return {"ok": False, "erro": "Já existe uma extração em andamento."}

        DIR_LOGS_EXECUCOES.mkdir(parents=True, exist_ok=True)
        comando = _montar_comando(dataset, ano_inicio, ano_fim, tabelas, forcar)

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
        _iniciado_em = time.time()
        _finalizado_em = None
        _codigo_saida = None

        return {"ok": True, "pid": _processo.pid, "comando": " ".join(comando)}


def parar() -> dict:
    """Pede para o subprocesso terminar (SIGTERM); força depois de alguns segundos."""
    global _codigo_saida, _finalizado_em

    with _lock:
        if _processo is None or _processo.poll() is not None:
            return {"ok": False, "erro": "Nenhuma extração rodando."}

        _processo.terminate()
        try:
            _processo.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _processo.kill()
            _processo.wait(timeout=10)

        _codigo_saida = _processo.returncode
        _finalizado_em = time.time()
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

        # Fora de uma execução ativa, ainda mostra a cauda do último log —
        # útil pra ver como uma carga terminou sem precisar abrir o arquivo.
        caminho = _caminho_log if _processo is not None else _caminho_log_mais_recente()

        return {
            "rodando": rodando,
            "pid": _processo.pid if _processo else None,
            "comando": " ".join(_comando) if _comando else None,
            "log_arquivo": caminho.name if caminho else None,
            "iniciado_em": _iniciado_em,
            "finalizado_em": _finalizado_em,
            "codigo_saida": _codigo_saida,
            "log_tail": _tail(caminho, 60) if caminho else [],
        }
