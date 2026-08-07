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
import re
import shutil
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
    STAGING_DIR,
)
from painel import processos
from painel.totais_esperados import TOTAIS_BRONZE

REFRESH_SEGUNDOS = 5
PREFIXOS_BRONZE_ESPECIAIS = {"_layouts", "dicionarios"}
LINHAS_ATIVIDADE_RECENTE = 25

# De qual raiz do FTP cada tabela vem — usado só para saber que --dataset
# passar numa retentativa filtrada por --tabela (ver catalogo.py: RAIZES).
# O manifesto guarda a tabela de destino (caged_mov, rais_vinc, ...), não o
# dataset de origem, então esse mapa reconstrói o que falta.
# Tabelas que cada construtor de silver aceita (espelha o --tabela de cada CLI)
TABELAS_SILVER = {
    "caged": ("caged_mov", "caged_for", "caged_exc", "caged_old", "caged_ajustes"),
    "rais": ("rais_estab", "rais_vinc"),
}

TABELA_PARA_DATASET = {
    "caged_mov": "novo_caged", "caged_for": "novo_caged", "caged_exc": "novo_caged",
    "caged_old": "caged",
    "caged_ajustes": "caged_ajustes",
    "rais_estab": "rais", "rais_vinc": "rais",
}

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


def _listar_parquets(fs, bucket: str, tabela: str) -> list[str]:
    try:
        return fs.glob(f"{bucket}/{tabela}/**/*.parquet")
    except Exception:
        return []


def _contar_parquets(fs, bucket: str, tabela: str) -> int:
    return len(_listar_parquets(fs, bucket, tabela))


def _int(valor) -> int:
    try:
        return int(float(valor))
    except (TypeError, ValueError):
        return 0


def _float(valor) -> float:
    try:
        return float(valor)
    except (TypeError, ValueError):
        return 0.0


def _chave_item(row: dict) -> tuple:
    """Identifica o mesmo item ao longo de reprocessamentos (retentativas)."""
    return (row.get("tabela"), row.get("arquivo_fonte"))


def _ler_manifesto() -> dict:
    """
    Lê o manifesto inteiro (é o registro permanente de tudo que a extração já
    processou, nunca é apagado — é append-only, um reprocessamento soma uma
    linha nova, não substitui a antiga).

    "recentes" mostra o histórico bruto, na ordem em que aconteceu (útil para
    ver "isso falhou, depois foi retentado e deu certo" ao longo do tempo).

    "erros" e as contagens ok/erro, por outro lado, refletem o ESTADO ATUAL:
    se um item falhou e depois foi retentado com sucesso, só a última linha
    dele conta — ele some da lista de erros e passa a contar como ok, mesmo
    a linha de erro antiga continuando no arquivo para auditoria.
    """
    caminho = DIR_LOGS / "manifesto_extracao.csv"
    linhas: list[dict] = []

    if caminho.exists():
        try:
            with open(caminho, newline="", encoding="utf-8") as f:
                linhas = list(csv.DictReader(f))
        except Exception:
            pass

    # O arquivo é cronológico (append-only) -> a última ocorrência de cada
    # chave sobrescreve as anteriores no dict, sobrando só o estado atual.
    ultimo_por_item: dict[tuple, dict] = {}
    for row in linhas:
        ultimo_por_item[_chave_item(row)] = row

    erros_atuais = [row for row in ultimo_por_item.values() if row.get("status") == "erro"]
    ok_atuais = len(ultimo_por_item) - len(erros_atuais)
    erros_atuais.sort(key=lambda r: r.get("data_hora", ""), reverse=True)

    # Agregados por tabela: contagem de arquivos não diz volume — caged_exc
    # tem 75 arquivos e 616 mil linhas, caged_ajustes tem 128 e 29 milhões.
    por_tabela: dict[str, dict] = {}
    total_linhas = total_bytes = 0.0
    segundos_por_item: list[float] = []

    for row in ultimo_por_item.values():
        if row.get("status") != "ok":
            continue
        agg = por_tabela.setdefault(row.get("tabela", "?"), {"linhas": 0, "bytes": 0})
        agg["linhas"] += _int(row.get("linhas"))
        agg["bytes"] += _int(row.get("bytes_compactado"))
        total_linhas += _int(row.get("linhas"))
        total_bytes += _int(row.get("bytes_compactado"))
        seg = _float(row.get("segundos"))
        if seg > 0:
            segundos_por_item.append(seg)

    return {
        "total": len(ultimo_por_item),
        "ok": ok_atuais,
        "erro": len(erros_atuais),
        "recentes": list(reversed(linhas[-LINHAS_ATIVIDADE_RECENTE:])),
        "erros": erros_atuais,
        "linhas_total": int(total_linhas),
        "bytes_total": int(total_bytes),
        "segundos_medio": round(sum(segundos_por_item) / len(segundos_por_item), 1)
                          if segundos_por_item else None,
        "por_tabela": por_tabela,
    }


RE_ANO = re.compile(r"ano=(\d{4})")
RE_MES = re.compile(r"mes=(\d{1,2})")


def _cobertura(caminhos: list[str]) -> dict:
    """
    Competências presentes e — mais importante — as que faltam no meio.

    Para análise de série temporal um buraco no meio é bem pior que um total
    menor do que o esperado: "156/156 arquivos" não garante que os 13 anos
    estão contínuos. Só considera lacuna o que está DENTRO do intervalo já
    coberto (entre a primeira e a última competência); nada é reportado como
    faltando depois do fim da série, que é só dado ainda não publicado.
    """
    competencias = set()
    anuais = set()

    for caminho in caminhos:
        m_ano = RE_ANO.search(caminho)
        if not m_ano:
            continue
        ano = int(m_ano.group(1))
        m_mes = RE_MES.search(caminho)
        if m_mes:
            competencias.add((ano, int(m_mes.group(1))))
        else:
            anuais.add(ano)

    if competencias:
        ordenadas = sorted(competencias)
        (ano_i, mes_i), (ano_f, mes_f) = ordenadas[0], ordenadas[-1]
        esperadas = {
            (a, m)
            for a in range(ano_i, ano_f + 1)
            for m in range(1, 13)
            if (a, m) >= (ano_i, mes_i) and (a, m) <= (ano_f, mes_f)
        }
        faltando = sorted(esperadas - competencias)
        return {
            "inicio": f"{ano_i}-{mes_i:02d}",
            "fim": f"{ano_f}-{mes_f:02d}",
            "presentes": len(competencias),
            "faltando": [f"{a}-{m:02d}" for a, m in faltando],
        }

    if anuais:
        ordenados = sorted(anuais)
        faltando = [a for a in range(ordenados[0], ordenados[-1] + 1) if a not in anuais]
        return {
            "inicio": str(ordenados[0]),
            "fim": str(ordenados[-1]),
            "presentes": len(anuais),
            "faltando": [str(a) for a in faltando],
        }

    return {"inicio": None, "fim": None, "presentes": 0, "faltando": []}


def _disco() -> dict:
    """Uso do disco onde fica o staging — já tivemos carga morrendo por falta de espaço."""
    try:
        uso = shutil.disk_usage(STAGING_DIR)
        staging_bytes = sum(
            f.stat().st_size for f in Path(STAGING_DIR).rglob("*") if f.is_file()
        )
        return {
            "livre_gb": round(uso.free / 1e9, 1),
            "total_gb": round(uso.total / 1e9, 1),
            "staging_gb": round(staging_bytes / 1e9, 2),
        }
    except Exception:
        return {"livre_gb": None, "total_gb": None, "staging_gb": None}


def _montar_status() -> dict:
    fs = _fs()
    minio = _checar_minio(fs)

    tabelas_bronze = set(_listar_tabelas(fs, BUCKET_BRONZE)) | set(TOTAIS_BRONZE)
    tabelas_silver = set(_listar_tabelas(fs, BUCKET_SILVER))

    manifesto = _ler_manifesto()
    por_tabela = manifesto["por_tabela"]

    tabelas = []
    for nome in sorted(tabelas_bronze | tabelas_silver):
        caminhos_bronze = _listar_parquets(fs, BUCKET_BRONZE, nome) if minio["ok"] else []
        n_bronze = len(caminhos_bronze)
        n_silver = _contar_parquets(fs, BUCKET_SILVER, nome) if minio["ok"] else 0
        esperado = TOTAIS_BRONZE.get(nome)
        pct = min(100, round(n_bronze / esperado * 100)) if esperado else None
        # A silver grava um parquet por arquivo do bronze, então o próprio
        # bronze é a meta — dá para mostrar o progresso da tradução do mesmo
        # jeito que o do download, sem precisar de outra tabela de totais.
        pct_silver = min(100, round(n_silver / n_bronze * 100)) if n_bronze else None
        agg = por_tabela.get(nome, {})
        tabelas.append({
            "tabela": nome,
            "bronze": n_bronze,
            "bronze_esperado": esperado,
            "pct_bronze": pct,
            "silver": n_silver,
            "silver_esperado": n_bronze,
            "pct_silver": pct_silver,
            "tem_silver": n_silver > 0,
            "linhas": agg.get("linhas", 0),
            "bytes": agg.get("bytes", 0),
            "cobertura": _cobertura(caminhos_bronze),
        })

    return {
        "pronto": True,
        "gerado_em": time.time(),
        "minio": minio,
        "heartbeat": heartbeat.ler(),
        "tabelas": tabelas,
        "manifesto": manifesto,
        "processo": processos.status(),
        "disco": _disco(),
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

    resultado = processos.iniciar(dataset, ano_inicio, rotulo="-".join(dataset))
    return jsonify(resultado), (200 if resultado["ok"] else 409)


@app.route("/api/extracao/parar", methods=["POST"])
def api_parar():
    resultado = processos.parar()
    return jsonify(resultado), (200 if resultado["ok"] else 409)


@app.route("/api/silver/iniciar", methods=["POST"])
def api_silver_iniciar():
    """
    Constrói a silver (bronze traduzido pelos dicionários).

    Sem --forcar por padrão: a construção grava um parquet por arquivo do
    bronze e pula os que já existem, então re-disparar retoma de onde parou
    em vez de refazer tudo.
    """
    corpo = request.get_json(silent=True) or {}
    tabelas = corpo.get("tabelas") or []
    camada = corpo.get("camada", "caged")
    forcar = bool(corpo.get("forcar", False))

    validas = TABELAS_SILVER.get(camada)
    if validas is None:
        return jsonify({"ok": False, "erro": f"camada inválida: {camada}"}), 400
    if not tabelas:
        tabelas = list(validas)
    invalidas = [t for t in tabelas if t not in validas]
    if invalidas:
        return jsonify({"ok": False, "erro": f"tabela inválida para {camada}: {invalidas}"}), 400

    resultado = processos.iniciar_silver(tabelas, camada=camada, forcar=forcar)
    if resultado["ok"]:
        resultado["tabelas"] = tabelas
    return jsonify(resultado), (200 if resultado["ok"] else 409)


@app.route("/api/erros/retentar", methods=["POST"])
def api_retentar_erros():
    """
    Relança a extração filtrada só pelas tabelas/anos que hoje têm erro no
    manifesto. Não precisa de --forcar: um item que falhou nunca chegou a
    gravar no bronze, então o EstadoLake do run_extracao já o trata como
    pendente e vai reprocessar exatamente ele (e só ele, dentro do recorte).
    """
    erros = _ler_manifesto()["erros"]
    if not erros:
        return jsonify({"ok": False, "erro": "Nenhum erro registrado para retentar."}), 400

    tabelas = sorted({e["tabela"] for e in erros if e.get("tabela")})
    datasets_desconhecidos = [t for t in tabelas if t not in TABELA_PARA_DATASET]
    if datasets_desconhecidos:
        return jsonify({
            "ok": False,
            "erro": f"Tabela sem dataset conhecido: {datasets_desconhecidos}. "
                    f"Atualize TABELA_PARA_DATASET em painel/app.py.",
        }), 500

    dataset = sorted({TABELA_PARA_DATASET[t] for t in tabelas})
    anos = [int(e["ano"]) for e in erros if e.get("ano")]
    ano_inicio, ano_fim = (min(anos), max(anos)) if anos else (1985, 2030)

    resultado = processos.iniciar(
        dataset, ano_inicio, ano_fim=ano_fim, tabelas=tabelas, rotulo="retentar-erros",
    )
    if resultado["ok"]:
        resultado["itens_visados"] = len(erros)
        resultado["tabelas"] = tabelas
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
