"""
Dispara os anos restantes da RAIS assim que o ano em teste fechar com sucesso.

Existe porque o primeiro ano foi rodado isolado, para validar o ciclo completo
(traduzir -> publicar -> apagar as duas cópias). Confirmado o ciclo, o resto é
mecânico — mas só deve começar se o teste tiver terminado LIMPO.

O que ele confere antes de disparar, e por quê:

  1. o processo do pipeline saiu           — a trava de job único recusaria
  2. o log diz "1/1 ano(s)" e não lista falha
  3. o espelho local do ano sumiu          — prova que a limpeza rodou
  4. a silver do ano sumiu do MinIO        — a limpeza que mais importa, é ela
                                             que enche o disco
  5. o repositório no Hub tem a tabela     — prova que o dado chegou

Qualquer uma falhando, ele NÃO dispara e explica o motivo. Publicar 18 anos em
cima de um ciclo quebrado seria multiplicar o erro por 18.
"""
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import painel  # noqa: F401  (bootstrap de caminho)

from painel import hf_status
from painel.processos import DIR_LOGS_EXECUCOES

ANO_TESTE = 2025
FAIXA_RESTANTE = (2007, 2024)
PAINEL = "http://127.0.0.1:8088"
INTERVALO = 60


def _log_mais_recente() -> Path | None:
    logs = sorted(DIR_LOGS_EXECUCOES.glob("*rais-pipeline*.log"),
                  key=lambda p: p.stat().st_mtime)
    return logs[-1] if logs else None


def _pipeline_rodando() -> bool:
    import psutil

    for p in psutil.process_iter(["cmdline"]):
        try:
            if "pipeline_ano" in " ".join(p.info["cmdline"] or []):
                return True
        except Exception:
            continue
    return False


def _conferir(log: Path) -> list[str]:
    """Devolve a lista de problemas. Vazia = pode disparar."""
    problemas = []
    texto = log.read_text(encoding="utf-8", errors="replace")

    if "anos com falha" in texto:
        m = re.search(r"anos com falha[^\n]*", texto)
        problemas.append(f"o log reporta falha: {m.group(0) if m else '?'}")
    if not re.search(r"🏁 1/1 ano", texto):
        problemas.append("o log não confirma '1/1 ano(s)' concluído")

    from silver_caged.publicar_hf import DIRS

    espelho = DIRS["rais"] / "rais_vinc" / f"ano_particao={ANO_TESTE}"
    if espelho.exists():
        problemas.append(f"o espelho local de {ANO_TESTE} não foi apagado")

    from painel.app import _fs
    from extracao_ftp.config_extracao import BUCKET_SILVER

    fs = _fs()
    if fs.exists(f"{BUCKET_SILVER}/rais_vinc/ano_particao={ANO_TESTE}"):
        problemas.append(f"a silver de {ANO_TESTE} não foi apagada do MinIO")

    estado = hf_status.ler(hf_status.REPOS["rais"])
    publicados = estado.get("por_tabela", {}).get("rais_vinc", {}).get("arquivos", 0)
    if not publicados:
        problemas.append("nenhum arquivo de rais_vinc no repositório do Hub")

    return problemas


def _disparar() -> None:
    corpo = json.dumps({
        "tabela": "rais_vinc",
        "ano_inicio": FAIXA_RESTANTE[0],
        "ano_fim": FAIXA_RESTANTE[1],
    }).encode()
    req = urllib.request.Request(f"{PAINEL}/api/rais/pipeline", data=corpo,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            print("DISPARADO:", json.load(r))
    except urllib.error.HTTPError as e:
        print("RECUSADO pelo painel:", e.code, json.load(e))


def main() -> int:
    print(f"⏳ aguardando o ciclo de {ANO_TESTE} terminar…", flush=True)
    while _pipeline_rodando():
        time.sleep(INTERVALO)

    log = _log_mais_recente()
    if log is None:
        print("❌ nenhum log do pipeline encontrado")
        return 1

    print(f"🔎 conferindo {log.name}", flush=True)
    problemas = _conferir(log)
    if problemas:
        print(f"🛑 {ANO_TESTE} NÃO fechou limpo — os {FAIXA_RESTANTE} anos não foram disparados:")
        for p in problemas:
            print(f"   • {p}")
        return 2

    print(f"✅ {ANO_TESTE} fechou limpo. Disparando "
          f"{FAIXA_RESTANTE[0]}–{FAIXA_RESTANTE[1]}.", flush=True)
    _disparar()
    return 0


if __name__ == "__main__":
    sys.exit(main())
