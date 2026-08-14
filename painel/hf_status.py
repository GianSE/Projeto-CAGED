"""
Quantos arquivos de cada tabela já estão publicados no Hugging Face.

É o terceiro estágio do pipeline visível no painel: FTP -> bronze,
bronze -> silver traduzida, silver -> Hugging Face. Sem isso o painel mostra
a tradução terminando e nada sobre a publicação, que é justamente a parte
demorada e sujeita a queda de conexão.

POR QUE HTTP PURO E NÃO huggingface_hub
---------------------------------------
O venv do painel tem 5 dependências de propósito — ele só observa e dispara
subprocessos, enquanto os jobs pesados rodam no venv da raiz. Puxar o
huggingface_hub (e o pyarrow, o fsspec e o resto da árvore dele) para dentro
do painel só para contar arquivos inverteria essa separação. A API de árvore
do Hub é pública para dataset público e responde com JSON, então urllib
resolve.

O token NÃO é usado aqui: contar arquivos de um dataset público não exige
credencial, e o painel é a última coisa que deveria carregar uma credencial de
escrita.
"""
import json
import os
import time
import urllib.error
import urllib.request

# Configurável porque o nome do dataset é decisão de publicação, não do painel.
REPO_PADRAO = os.getenv("HF_REPO_COMPLETO", "Gianpedro/caged-microdados-traduzidos")

# A árvore recursiva de um repo com milhares de arquivos não é barata, e o
# painel atualiza a cada 5 s. O TTL desacopla os dois: durante um upload longo
# o número sobe a cada minuto, que é resolução de sobra para uma barra de
# progresso.
_TTL = 60
_cache: dict[str, tuple[dict, float]] = {}


def _consultar(repo: str) -> dict:
    url = f"https://huggingface.co/api/datasets/{repo}/tree/main?recursive=true"
    requisicao = urllib.request.Request(url, headers={"User-Agent": "painel-caged"})
    with urllib.request.urlopen(requisicao, timeout=20) as resposta:
        itens = json.load(resposta)

    por_tabela: dict[str, dict] = {}
    for item in itens:
        if item.get("type") != "file" or not item.get("path", "").endswith(".parquet"):
            continue
        tabela = item["path"].split("/")[0]
        entrada = por_tabela.setdefault(tabela, {"arquivos": 0, "bytes": 0})
        entrada["arquivos"] += 1
        entrada["bytes"] += item.get("size", 0) or 0

    return {
        "ok": True,
        "repo": repo,
        "url": f"https://huggingface.co/datasets/{repo}",
        "por_tabela": por_tabela,
        "arquivos": sum(t["arquivos"] for t in por_tabela.values()),
        "bytes": sum(t["bytes"] for t in por_tabela.values()),
    }


def ler(repo: str | None = None) -> dict:
    """
    Estado da publicação, com cache.

    Nunca levanta exceção: o painel precisa continuar mostrando bronze e silver
    mesmo sem internet ou com o repositório ainda inexistente. Um erro vira
    `ok: False` com o motivo, e o cache velho é preferido a nada — durante uma
    oscilação de rede é melhor mostrar o número de um minuto atrás do que zerar
    a barra de progresso na cara de quem está olhando.
    """
    repo = repo or REPO_PADRAO
    agora = time.time()

    cache = _cache.get(repo)
    if cache and agora - cache[1] < _TTL:
        return cache[0]

    try:
        estado = _consultar(repo)
    except urllib.error.HTTPError as e:
        # Antes da primeira publicação o repositório não existe, e o Hub
        # responde 401 — não 404. É deliberado: 404 revelaria a existência de
        # datasets privados de terceiros a quem não tem acesso. Como não
        # usamos token aqui, os dois códigos significam a mesma coisa para o
        # painel: "não há o que contar ainda".
        inexistente = e.code in (401, 404)
        motivo = "ainda não publicado" if inexistente else f"HTTP {e.code}"
        estado = {"ok": False, "erro": motivo, "repo": repo, "por_tabela": {},
                  "arquivos": 0, "bytes": 0}
        if inexistente:
            _cache[repo] = (estado, agora)
            return estado
        return cache[0] if cache else estado
    except Exception as e:
        estado = {"ok": False, "erro": str(e)[:120], "repo": repo, "por_tabela": {},
                  "arquivos": 0, "bytes": 0}
        return cache[0] if cache else estado

    _cache[repo] = (estado, agora)
    return estado
