"""
Ponte para rodar a extração e a silver sem MinIO (modo nuvem: GitHub Actions
+ Hugging Face, sem máquina local).

POR QUE UM TOGGLE, E NÃO UM PIPELINE PARALELO
----------------------------------------------
Cada camada escreve no MinIO num único ponto (extração: `transformador.py`;
silver: `construir_silver.py` de cada base — ver levantamento que motivou
este módulo). É mais seguro trocar a resolução de origem/destino nesse ponto
único do que duplicar a lógica de SQL que já roda em produção todo dia.

COMO FUNCIONA
-------------
MODO_NUVEM=1 liga o modo. Nele:
  - LEITURA do que já está publicado passa a ser `hf://datasets/<repo>/...`
    em vez de `s3://bucket/...` — o DuckDB lê (e globa) os dois do mesmo
    jeito, via httpfs.
  - ESCRITA passa a ser: grava num arquivo LOCAL temporário, publica esse
    arquivo no Hugging Face, apaga o local. Nunca existe `s3://` no modo
    nuvem — não há MinIO para escrever.
  - EXISTÊNCIA ("isso já foi processado?") passa a consultar a listagem de
    arquivos do repositório no Hub (`HfApi.list_repo_files`, uma chamada,
    cacheada neste processo) em vez de `s3.head_object` no MinIO.

Os dicionários do MTE (silver_caged/dimensoes.py) são a única parte que o
modo nuvem NÃO deriva sozinho: são poucos arquivos, mudam raramente, e
continuam sendo extraídos localmente uma vez e publicados no Hub — o job de
CI só os lê de lá. Ver `python -m extracao_ftp.nuvem --bootstrap-dicionarios`.
"""
import os
from pathlib import Path

MODO_NUVEM = os.getenv("MODO_NUVEM", "").strip().lower() in ("1", "true", "sim")

REPO_BRONZE_CAGED = os.getenv("REPO_BRONZE_CAGED", "Gianpedro/bronze_caged")
REPO_BRONZE_RAIS = os.getenv("REPO_BRONZE_RAIS", "Gianpedro/bronze_rais")
REPO_SILVER_TI_CAGED = os.getenv("REPO_SILVER_TI_CAGED", "Gianpedro/caged-tecnologia")
REPO_SILVER_TI_RAIS = os.getenv("REPO_SILVER_TI_RAIS", "Gianpedro/rais-tecnologia")

# Dicionários do MTE (CBO, CNAE, escolaridade, ...) — compartilhados entre
# CAGED e RAIS desde a extração local, por isso vivem num repositório
# próprio, não dentro do bronze de uma base específica. Publicados uma vez,
# manualmente (ver bootstrap_dicionarios_hf.py); o job de CI só lê.
REPO_DICIONARIOS = os.getenv("REPO_DICIONARIOS", "Gianpedro/mte-dicionarios")

# Staging local do job de CI — some ao final do job, não precisa sobreviver.
DIR_TEMP_NUVEM = Path(os.getenv("STAGING_DIR") or ".") / "_nuvem"

# Mesma pasta que gold_unificado/publicar_gold.py já lê para publicar — no
# modo nuvem, quem constrói a gold (gold_rais/construir_gold.py,
# gold_unificado/mapa.py) grava direto aqui, pulando o hop pelo MinIO.
DIR_GOLD_LOCAL = Path(__file__).resolve().parents[2] / "publicacao" / "gold"


def repo_bronze(tabela: str) -> str:
    return REPO_BRONZE_RAIS if tabela.startswith("rais") else REPO_BRONZE_CAGED


def repo_silver_ti(tabela: str) -> str:
    return REPO_SILVER_TI_RAIS if tabela.startswith("rais") else REPO_SILVER_TI_CAGED


def fonte_leitura(repo: str, prefixo: str = "") -> str:
    """`hf://datasets/<repo>/<prefixo>` — o DuckDB lê e globa direto, sem credencial."""
    prefixo = prefixo.lstrip("/")
    return f"hf://datasets/{repo}/{prefixo}" if prefixo else f"hf://datasets/{repo}"


_cache_listagem: dict[str, set] = {}


def arquivos_remotos(repo: str, forcar: bool = False) -> set:
    """Lista (uma vez por processo, salvo --forcar) os arquivos já publicados."""
    if forcar or repo not in _cache_listagem:
        from huggingface_hub import HfApi

        _cache_listagem[repo] = set(HfApi().list_repo_files(repo, repo_type="dataset"))
    return _cache_listagem[repo]


def existe_remoto(repo: str, caminho: str) -> bool:
    try:
        return caminho in arquivos_remotos(repo)
    except Exception:
        # Repo ainda não existe (primeira execução) — nada foi publicado.
        return False


def publicar_arquivo(local: Path, repo: str, caminho_repo: str, privado: bool = False) -> None:
    from huggingface_hub import HfApi

    api = HfApi()
    api.create_repo(repo_id=repo, repo_type="dataset", exist_ok=True, private=privado)
    api.upload_file(
        path_or_fileobj=str(local), path_in_repo=caminho_repo,
        repo_id=repo, repo_type="dataset",
    )
    arquivos_remotos(repo).add(caminho_repo)  # mantém o cache coerente sem relistar


def preparar_staging() -> None:
    DIR_TEMP_NUVEM.mkdir(parents=True, exist_ok=True)


def glob_bronze(tabela: str, resto: str = "**/*.parquet") -> str:
    """
    Glob de leitura da bronze inteira de uma tabela — usado onde o código
    só precisa que o DuckDB descubra os arquivos sozinho (ele faz isso
    nativamente tanto sobre s3:// quanto sobre hf://, sem Python no meio).
    """
    if MODO_NUVEM:
        return fonte_leitura(repo_bronze(tabela), f"{tabela}/{resto}")
    from extracao_ftp.config_extracao import BUCKET_BRONZE

    return f"s3://{BUCKET_BRONZE}/{tabela}/{resto}"


def caminho_gold(nome: str) -> str:
    """
    Caminho de uma tabela da gold final — tanto para ler quanto para
    escrever. No modo nuvem é um arquivo local (a mesma pasta que
    `gold_unificado.publicar_gold` lê para subir ao Hub); local é
    `s3://{BUCKET_GOLD}/...`. Os dois servem direto num `read_parquet`/`COPY
    TO` do DuckDB, sem mais nada — é por isso que não precisa de upload por
    arquivo aqui: tudo que a gold grava já cai na pasta certa.
    """
    if MODO_NUVEM:
        return str(DIR_GOLD_LOCAL / f"{nome}.parquet")
    from extracao_ftp.config_extracao import BUCKET_GOLD

    return f"s3://{BUCKET_GOLD}/{nome}.parquet"


def glob_silver_ti(tabela: str, resto: str = "**/*.parquet") -> str:
    """Mesma ideia que `glob_bronze`, para a silver de tecnologia já publicada."""
    if MODO_NUVEM:
        return fonte_leitura(repo_silver_ti(tabela), f"{tabela}/{resto}")
    from extracao_ftp.config_extracao import bucket_silver

    return f"s3://{bucket_silver(True)}/{tabela}/{resto}"


def resolvedor_repo(tabela: str):
    """
    bucket (string antiga do MinIO) -> repo HF, escolhido pela TABELA — CAGED
    e RAIS têm repositórios diferentes. Devolve uma função porque é isso que
    `FsNuvem` guarda (o bucket só é conhecido no momento do glob).
    """
    from extracao_ftp.config_extracao import BUCKET_BRONZE, bucket_silver

    mapa = {
        BUCKET_BRONZE: repo_bronze(tabela),
        bucket_silver(True): repo_silver_ti(tabela),
    }

    def resolver(bucket: str) -> str:
        if bucket not in mapa:
            raise ValueError(
                f"Sem repositorio HF para o bucket '{bucket}' no modo nuvem "
                "(provavelmente --mercado-completo, que so existe no MinIO local)"
            )
        return mapa[bucket]

    return resolver


class FsNuvem:
    """
    Substitui o s3fs no modo nuvem para o ÚNICO método que
    `construir_silver.py` usa dele: `glob("<bucket>/<tabela>/**/*.parquet")`.

    Devolve os caminhos no MESMO FORMATO que o s3fs devolveria (bucket como
    primeiro segmento, sem esquema) — assim o resto do código (regex de
    ano/mês, `_sql_str(f"s3://...")`) não precisa saber que a fonte real é
    a listagem do Hub, não o MinIO.
    """

    def __init__(self, tabela: str):
        self._resolver = resolvedor_repo(tabela)

    def glob(self, padrao: str) -> list[str]:
        bucket, resto = padrao.split("/", 1)
        prefixo = resto.removesuffix("**/*.parquet").rstrip("/")
        repo = self._resolver(bucket)
        return sorted(
            f"{bucket}/{a}" for a in arquivos_remotos(repo)
            if a.startswith(f"{prefixo}/") and a.endswith(".parquet")
        )


def publicar_diretorio(local_dir: Path, repo: str, prefixo_repo: str = "") -> int:
    """
    Sobe toda a árvore local (resultado de um COPY particionado hive) para o
    Hugging Face, preservando a estrutura de pastas — e apaga o local ao
    final. Um COPY com PARTITION_BY pode gerar várias subpastas de uma vez
    (um arquivo-fonte do CAGED antigo pode espalhar linhas por vários
    ano_particao/mes_particao), então a publicação também precisa ser em
    lote, não arquivo a arquivo.
    """
    arquivos = sorted(local_dir.rglob("*.parquet"))
    if not arquivos:
        return 0

    from huggingface_hub import HfApi, CommitOperationAdd

    api = HfApi()
    api.create_repo(repo_id=repo, repo_type="dataset", exist_ok=True)
    operacoes = []
    for arq in arquivos:
        rel = arq.relative_to(local_dir).as_posix()
        caminho_repo = f"{prefixo_repo}/{rel}" if prefixo_repo else rel
        operacoes.append(CommitOperationAdd(path_in_repo=caminho_repo, path_or_fileobj=str(arq)))

    api.create_commit(
        repo_id=repo, repo_type="dataset", operations=operacoes,
        commit_message=f"silver: {len(operacoes)} arquivo(s) via modo nuvem",
    )
    _cache_listagem.pop(repo, None)  # relista na próxima consulta: mudou de verdade

    for arq in arquivos:
        arq.unlink()
    return len(arquivos)
