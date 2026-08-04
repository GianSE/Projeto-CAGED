"""
Painel web de observabilidade da extração CAGED/RAIS.

Mostra: o que está rodando agora (heartbeat), quantos arquivos cada tabela já
tem no bronze/silver comparado ao esperado, e a atividade recente do
manifesto. Roda separado dos jobs de carga — só lê arquivos e faz listagem
leve no S3 (nunca abre o DuckDB), então não compete por CPU com a extração ou
com a construção da silver.

Uso:
    python -m painel.app
    # abre em http://127.0.0.1:8088
"""
import csv
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from extracao_ftp import heartbeat
from extracao_ftp.catalogo import DATASETS
from extracao_ftp.config_extracao import (
    BUCKET_BRONZE,
    BUCKET_SILVER,
    DIR_LOGS,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
)
from painel import processos
from painel.totais_esperados import TOTAIS_BRONZE

REFRESH_SEGUNDOS = 5
PREFIXOS_BRONZE_ESPECIAIS = {"_layouts", "dicionarios"}
LINHAS_ATIVIDADE_RECENTE = 25

app = Flask(__name__)

_cache_lock = threading.Lock()
_cache: dict = {"pronto": False}


def _fs():
    import s3fs

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}", "region_name": MINIO_REGION},
    )


def _checar_minio(fs) -> dict:
    try:
        fs.ls(BUCKET_BRONZE)
        return {"ok": True, "endpoint": MINIO_ENDPOINT}
    except Exception as e:
        return {"ok": False, "endpoint": MINIO_ENDPOINT, "erro": str(e)[:200]}


def _listar_tabelas(fs, bucket: str) -> list[str]:
    try:
        nomes = [p.split("/")[-1] for p in fs.ls(bucket)]
    except Exception:
        return []
    return sorted(n for n in nomes if n and n not in PREFIXOS_BRONZE_ESPECIAIS)


def _contar_parquets(fs, bucket: str, tabela: str) -> int:
    try:
        return len(fs.glob(f"{bucket}/{tabela}/**/*.parquet"))
    except Exception:
        return 0


def _ler_manifesto() -> dict:
    """
    Lê o manifesto inteiro (é o registro permanente de tudo que a extração já
    processou, nunca é apagado). Devolve tanto os últimos N itens (atividade
    recente, mistura ok/erro) quanto TODOS os erros — os erros somem da lista
    "recente" assim que itens ok subsequentes empurram eles pra fora das
    últimas N linhas, mas continuam precisando aparecer em algum lugar pra
    dar pra tratar depois.
    """
    caminho = DIR_LOGS / "manifesto_extracao.csv"
    linhas: list[dict] = []
    erros: list[dict] = []

    if caminho.exists():
        try:
            with open(caminho, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    linhas.append(row)
                    if row.get("status") == "erro":
                        erros.append(row)
        except Exception:
            pass

    return {
        "total": len(linhas),
        "ok": len(linhas) - len(erros),
        "erro": len(erros),
        "recentes": list(reversed(linhas[-LINHAS_ATIVIDADE_RECENTE:])),
        "erros": list(reversed(erros)),
    }


def _montar_status() -> dict:
    fs = _fs()
    minio = _checar_minio(fs)

    tabelas_bronze = set(_listar_tabelas(fs, BUCKET_BRONZE)) | set(TOTAIS_BRONZE)
    tabelas_silver = set(_listar_tabelas(fs, BUCKET_SILVER))

    tabelas = []
    for nome in sorted(tabelas_bronze | tabelas_silver):
        n_bronze = _contar_parquets(fs, BUCKET_BRONZE, nome) if minio["ok"] else 0
        n_silver = _contar_parquets(fs, BUCKET_SILVER, nome) if minio["ok"] else 0
        esperado = TOTAIS_BRONZE.get(nome)
        pct = min(100, round(n_bronze / esperado * 100)) if esperado else None
        tabelas.append({
            "tabela": nome,
            "bronze": n_bronze,
            "bronze_esperado": esperado,
            "pct_bronze": pct,
            "silver": n_silver,
            "tem_silver": n_silver > 0,
        })

    return {
        "pronto": True,
        "gerado_em": time.time(),
        "minio": minio,
        "heartbeat": heartbeat.ler(),
        "tabelas": tabelas,
        "manifesto": _ler_manifesto(),
        "processo": processos.status(),
    }


def _loop_atualizacao():
    global _cache
    while True:
        try:
            novo = _montar_status()
        except Exception as e:
            novo = {"pronto": False, "erro": str(e)[:300]}
        with _cache_lock:
            _cache = novo
        time.sleep(REFRESH_SEGUNDOS)


@app.route("/")
def index():
    return render_template("index.html", refresh_ms=REFRESH_SEGUNDOS * 1000)


@app.route("/api/status")
def api_status():
    with _cache_lock:
        return jsonify(_cache)


@app.route("/api/extracao/iniciar", methods=["POST"])
def api_iniciar():
    corpo = request.get_json(silent=True) or {}
    dataset = corpo.get("dataset") or list(DATASETS)
    ano_inicio = int(corpo.get("ano_inicio", 1985))

    dataset_invalido = [d for d in dataset if d not in DATASETS]
    if dataset_invalido:
        return jsonify({"ok": False, "erro": f"dataset inválido: {dataset_invalido}"}), 400

    resultado = processos.iniciar(dataset, ano_inicio)
    return jsonify(resultado), (200 if resultado["ok"] else 409)


@app.route("/api/extracao/parar", methods=["POST"])
def api_parar():
    resultado = processos.parar()
    return jsonify(resultado), (200 if resultado["ok"] else 409)


@app.route("/api/execucoes")
def api_execucoes():
    return jsonify(processos.listar_execucoes())


@app.route("/api/execucoes/<nome>")
def api_execucao_log(nome):
    # Path(...).name descarta qualquer parte de diretório do parâmetro —
    # bloqueia tentativa de sair de DIR_LOGS_EXECUCOES via "../".
    nome_seguro = Path(nome).name
    caminho = processos.DIR_LOGS_EXECUCOES / nome_seguro
    if not caminho.is_file():
        return jsonify({"ok": False, "erro": "log não encontrado"}), 404
    try:
        conteudo = caminho.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)[:200]}), 500
    return jsonify({"ok": True, "nome": nome_seguro, "conteudo": conteudo})


def main():
    threading.Thread(target=_loop_atualizacao, daemon=True).start()
    app.run(host="127.0.0.1", port=8088, debug=False)


if __name__ == "__main__":
    main()
