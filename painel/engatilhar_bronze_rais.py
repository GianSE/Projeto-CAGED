"""
Dispara o bronze da RAIS assim que o do CAGED terminar íntegro.

O CAGED é o ensaio: 7,78 GB em 8 lotes, contra 52,56 GB em 38 lotes da RAIS.
Só faz sentido soltar o grande depois que o pequeno provar que o publicador
funciona de ponta a ponta — incluindo lote, quebra de arquivo, envio e limpeza
do espelho.

O que confere antes de disparar:

  1. o processo saiu                  — a trava de job único recusaria
  2. o log fecha "8/8 lote(s)", sem falha
  3. o Hub tem os 514 parquets do CAGED
  4. um arquivo publicado ABRE pelo hf://

A checagem 4 é a que não dá para deduzir das outras: arquivo listado não é
arquivo legível. Um upload truncado aparece na árvore com o nome certo.
"""
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import painel  # noqa: F401

from painel import hf_status
from painel.processos import DIR_LOGS_EXECUCOES

PAINEL = "http://127.0.0.1:8088"
REPO_CAGED = "Gianpedro/bronze_caged"
INTERVALO = 60


def _rodando() -> bool:
    import psutil

    for p in psutil.process_iter(["cmdline"]):
        try:
            if "publicar_bronze" in " ".join(p.info["cmdline"] or []):
                return True
        except Exception:
            continue
    return False


def _um_arquivo_abre(repo: str) -> tuple[bool, str]:
    """Lê de fato um parquet publicado — listagem não prova legibilidade."""
    try:
        import duckdb

        url = f"https://huggingface.co/api/datasets/{repo}/tree/main?recursive=true"
        req = urllib.request.Request(url, headers={"User-Agent": "x"})
        with urllib.request.urlopen(req, timeout=60) as r:
            itens = json.load(r)
        alvos = [i["path"] for i in itens
                 if i["type"] == "file" and i["path"].endswith(".parquet")
                 and "/" in i["path"]]
        if not alvos:
            return False, "nenhum parquet de dados no repositório"

        base = f"https://huggingface.co/datasets/{repo}/resolve/main"
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs; SET enable_progress_bar=false;")
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{base}/{alvos[0]}')").fetchone()[0]
        return True, f"{alvos[0].split('/')[-1]} abriu com {n:,} linhas"
    except Exception as e:
        return False, f"não consegui ler: {str(e)[:90]}"


def _conferir() -> list[str]:
    problemas = []

    logs = sorted(DIR_LOGS_EXECUCOES.glob("*bronze-caged*.log"),
                  key=lambda p: p.stat().st_mtime)
    if not logs:
        return ["nenhum log do bronze do CAGED"]
    texto = logs[-1].read_text(encoding="utf-8", errors="replace")

    if "lotes com falha" in texto:
        m = re.search(r"lotes com falha[^\n]*", texto)
        problemas.append(f"log reporta falha: {m.group(0) if m else '?'}")
    if not re.search(r"🏁 (\d+)/\1 lote", texto):
        problemas.append("o log não confirma todos os lotes concluídos")

    estado = hf_status.ler(REPO_CAGED)
    publicados = sum(t.get("arquivos", 0) for t in estado.get("por_tabela", {}).values())
    if publicados < 500:
        problemas.append(f"só {publicados} arquivos no Hub (esperado ~514)")

    ok, detalhe = _um_arquivo_abre(REPO_CAGED)
    if not ok:
        problemas.append(detalhe)
    else:
        print(f"   ✅ leitura conferida: {detalhe}")

    return problemas


def main() -> int:
    print("⏳ aguardando o bronze do CAGED terminar…", flush=True)
    while _rodando():
        time.sleep(INTERVALO)

    print("🔎 conferindo o CAGED publicado", flush=True)
    problemas = _conferir()
    if problemas:
        print("🛑 CAGED não fechou íntegro — a RAIS NÃO foi disparada:")
        for p in problemas:
            print(f"   • {p}")
        return 2

    print("✅ CAGED íntegro. Disparando o bronze da RAIS (52,56 GB, 38 lotes).",
          flush=True)
    corpo = json.dumps({"camada": "rais", "repo": "Gianpedro/bronze_rais"}).encode()
    req = urllib.request.Request(f"{PAINEL}/api/bronze/publicar", data=corpo,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            print("DISPARADO:", json.load(r))
    except urllib.error.HTTPError as e:
        print("RECUSADO pelo painel:", e.code, json.load(e))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
