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
import re
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


def iniciar_silver(tabelas: list[str], camada: str = "caged", forcar: bool = False,
                   mercado_completo: bool = False, hive: bool = False) -> dict:
    """
    Sobe o subprocesso de construção da silver (bronze -> silver traduzida).

    Os dois recortes vão para buckets diferentes (silver-ti e silver), então
    disparar um não atrapalha o outro. `hive` só faz sentido no mercado
    completo, que é o recorte destinado à publicação: é o formato particionado
    por ano/mês que o dataset publicado usa.
    """
    modulo = "silver_caged.construir_silver" if camada == "caged" else "silver_rais.construir_silver"
    comando = [PYTHON_JOBS, "-m", modulo, "--tabela", *tabelas]
    if forcar:
        comando.append("--forcar")
    if mercado_completo:
        comando.append("--mercado-completo")
    if hive:
        comando.append("--hive")

    # O tipo aparece no status ("● ... rodando"), então diz QUAL silver: os dois
    # recortes levam tempos muito diferentes, e "silver rodando" sozinho não
    # deixa claro se são minutos ou horas.
    recorte = "completo" if mercado_completo else "ti"
    tipo = "silver (mercado completo)" if mercado_completo else "silver (TI)"
    return _lancar(comando, f"silver-{recorte}-{'-'.join(tabelas)}"[:60], tipo)


def iniciar_publicacao(tabelas: list[str], repo: str) -> dict:
    """
    Sobe o subprocesso de publicação no Hugging Face (silver -> Hub).

    Passa pelo mesmo _lancar dos demais, e portanto pela mesma trava de "um job
    pesado por vez". Não é por memória — o upload é leve — e sim porque
    publicar uma tabela que ainda está sendo construída subiria um retrato
    parcial dela, e o dataset ficaria com meses faltando sem nenhum aviso.
    """
    comando = [PYTHON_JOBS, "-m", "silver_caged.publicar_hf",
               "--repo", repo, "--tabela", *tabelas]
    return _lancar(comando, f"hf-{'-'.join(tabelas)}"[:60], f"publicação → {repo}")


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
    # Mesma normalização do _linhas_log: sem ela a barra de progresso do upload
    # chega ao painel como uma única linha quilométrica.
    return [l for l in _linhas_log(caminho) if l.strip()][-n_linhas:]


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


_RE_ITEM = re.compile(r"\[(\d+)/(\d+)\]")
_RE_TABELA = re.compile(r"(?:SILVER|BRONZE|PUBLICANDO):\s*(\w+)")
_RE_ENVIO = re.compile(r"(\d+) arquivo\(s\), ([\d.]+) GB")


_RE_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

# Só o fim do arquivo interessa, e o upload do Hugging Face escreve barra de
# progresso do tqdm: dezenas de MB de redesenho num log só. Ler o arquivo
# inteiro a cada refresh de 5 s seria desperdício puro.
_MAX_BYTES_LOG = 2 * 1024 * 1024


def _linhas_log(caminho: Path, max_bytes: int = _MAX_BYTES_LOG) -> list[str]:
    """
    Cauda do log, normalizada em linhas de verdade.

    O tqdm (que o upload_large_folder usa) redesenha a mesma linha com retorno
    de carro em vez de quebra de linha, e colore com escapes ANSI. Sem separar
    também pelo retorno de carro, todo o relatório de envio vira UMA linha de
    megabytes — ilegível na tela e inútil para qualquer regex de progresso.
    """
    try:
        with open(caminho, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - max_bytes))
            bruto = f.read().decode("utf-8", errors="replace")
    except Exception:
        return []

    return [_RE_ANSI.sub("", l).rstrip() for l in re.split(r"[\r\n]", bruto)]


# Janela maior, só para achar o relatório de envio: ele sai a cada 60 s, e um
# log escrito com as barras de progresso ligadas empilha megabytes de redesenho
# entre um relatório e o outro. Publicações novas não precisam disso (o
# publicador desliga as barras), mas logs já gravados sim.
_MAX_BYTES_ENVIO = 32 * 1024 * 1024
_TTL_ENVIO = 20
_cache_envio: tuple[float, dict | None] = (0.0, None)


def _envio_do_log(caminho: Path) -> dict | None:
    """Progresso do upload, relido no máximo a cada _TTL_ENVIO segundos."""
    global _cache_envio

    agora = time.time()
    if agora - _cache_envio[0] < _TTL_ENVIO:
        return _cache_envio[1]

    achado = _progresso_envio(_linhas_log(caminho, _MAX_BYTES_ENVIO))
    _cache_envio = (agora, achado)
    return achado


# Relatório do upload_large_folder. "Processing Files (a / b)" traz o avanço
# real do envio inteiro; "New Data Upload" é só o lote em trânsito no momento.
_RE_ENVIO_TOTAL = re.compile(
    r"Processing Files\s*\((\d+)\s*/\s*(\d+)\)\s*:\s*(\d+)%.*?\|\s*"
    r"([\d.]+\s*\w+)\s*/\s*([\d.]+\s*\w+)(?:,\s*([\d.]+\s*\w+/s))?"
)


def _progresso_envio(linhas: list[str]) -> dict | None:
    """
    Avanço do upload para o Hugging Face.

    Precisa existir porque a barra "Hugging Face" de cada tabela mede o que já
    está COMMITADO no repositório, e o upload_large_folder transfere tudo antes
    de commitar. Medido num envio de 6,17 GB: com 4,78 GB já transferidos, o
    repositório continuava intocado (lastModified e contagem de commits
    inalterados). Ou seja, aquela barra fica em zero durante praticamente todo o
    envio e só então salta — sem esta aqui, a fase mais longa da publicação não
    teria retorno visível nenhum.

    O contador "(a / b)" do relatório é de arquivos ENVIADOS, não commitados —
    conferido contra a API do Hub, que não registrava commit algum enquanto ele
    já marcava 6.

    De trás para frente: interessa o último relatório impresso.
    """
    for linha in reversed(linhas):
        m = _RE_ENVIO_TOTAL.search(linha)
        if m:
            return {
                "arquivos": int(m.group(1)),
                "arquivos_total": int(m.group(2)),
                "pct": int(m.group(3)),
                "enviado": m.group(4).strip(),
                "tamanho_total": m.group(5).strip(),
                "velocidade": (m.group(6) or "").strip() or None,
            }
    return None


def _progresso(linhas: list[str], iniciado_em: float | None,
               caminho: Path | None = None) -> dict | None:
    """
    Progressão do job a partir do log que ele já imprime.

    O construtor da silver escreve "[49/78] ✅ arquivo: N linhas" por arquivo e
    "🔨 SILVER: caged_mov" ao trocar de tabela. Em vez de instrumentar o job
    para reportar progresso por outro canal (arquivo de estado, socket), o
    painel lê o que já está lá — o log é a fonte da verdade e continua legível
    para um humano.

    Varre de trás para frente: interessa a ÚLTIMA ocorrência, e os logs chegam
    a dezenas de milhares de linhas.
    """
    atual = total = None
    tabela = None

    for linha in reversed(linhas):
        if atual is None:
            m = _RE_ITEM.search(linha)
            if m:
                atual, total = int(m.group(1)), int(m.group(2))
        if tabela is None:
            m = _RE_TABELA.search(linha)
            if m:
                tabela = m.group(1)
        if atual is not None and tabela is not None:
            break

    # Com o caminho em mãos, a busca do relatório de envio usa a janela larga e
    # cacheada; sem ele, cai para o que já está nas linhas recebidas.
    envio = _envio_do_log(caminho) if caminho else _progresso_envio(linhas)

    if atual is None or not total:
        # Sem "[n/N]" ainda, mas já enviando: é o caso da publicação, cujo
        # espelhamento pode estar todo pulado (nada a baixar) e que passa
        # direto para o upload.
        return {"atual": None, "total": None, "pct": None, "tabela": None,
                "segundos_restantes": None, "envio": envio} if envio else None

    pct = min(100, round(atual / total * 100))

    # ETA pela média do que ESTA execução processou, não pelo índice do
    # arquivo. A construção é retomável e pula em silêncio o que já existe:
    # numa retomada em 42/78, dividir o tempo decorrido por 42 trataria 42
    # arquivos pulados em milissegundos como se tivessem sido processados, e a
    # estimativa saía ~5x otimista (medido: 3 min no lugar de 15).
    #
    # Cada arquivo processado imprime uma linha "[n/N]"; os pulados não
    # imprimem nada. Contar essas linhas dá o denominador certo.
    processados = sum(1 for l in linhas if _RE_ITEM.search(l))

    # Só a partir do terceiro: com um ou dois, a média ainda carrega o custo de
    # partida (conexão, materialização dos dicionários) e produziria uma
    # estimativa absurda logo na primeira olhada.
    segundos_restantes = None
    if iniciado_em and processados >= 3:
        decorrido = time.time() - iniciado_em
        segundos_restantes = round(decorrido / processados * (total - atual))

    return {"atual": atual, "total": total, "pct": pct, "tabela": tabela,
            "segundos_restantes": segundos_restantes, "envio": envio}


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
                # A progressão sai do log inteiro, não da cauda exibida: o
                # último "[n/N]" pode ter rolado para fora das 60 linhas quando
                # o job imprime muita coisa entre um arquivo e outro.
                cauda = _tail(caminho_adotado, 60)
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
                    "log_tail": cauda,
                    "progresso": _progresso(_linhas_log(caminho_adotado),
                                            adotado.get("iniciado_em"),
                                            caminho_adotado),
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
            # Só enquanto roda: depois de terminar, um "[78/78]" congelado na
            # tela pareceria um job ainda em andamento.
            "progresso": (_progresso(_linhas_log(caminho), _iniciado_em, caminho)
                          if rodando and caminho else None),
        }
