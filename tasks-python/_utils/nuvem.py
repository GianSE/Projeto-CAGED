"""
Ponte com o Hugging Face Hub — leitura (hf://, via DuckDB) e escrita
(arquivo local temporário, publicado e apagado) das camadas bronze/silver/
gold. Sem MinIO: este é o único modo que o projeto roda hoje.

Os dicionários do MTE (CBO, CNAE, escolaridade, ...) são a única parte que
isto não deriva sozinho: mudam raramente, e continuam sendo publicados uma
vez por `_utils/bootstrap_dicionarios_hf.py`, rodado localmente — os jobs
de bronze/silver/gold só leem o que já está publicado.
"""
import os
from pathlib import Path

REPO_BRONZE_CAGED = os.getenv("REPO_BRONZE_CAGED", "Gianpedro/bronze-caged")
REPO_BRONZE_RAIS = os.getenv("REPO_BRONZE_RAIS", "Gianpedro/bronze-rais")
REPO_SILVER_TI_CAGED = os.getenv("REPO_SILVER_TI_CAGED", "Gianpedro/silver-caged-ti")
REPO_SILVER_TI_RAIS = os.getenv("REPO_SILVER_TI_RAIS", "Gianpedro/silver-rais-ti")
REPO_GOLD = os.getenv("REPO_GOLD", "Gianpedro/gold-analise-mercado-ti")
REPO_DICIONARIOS = os.getenv("REPO_DICIONARIOS", "Gianpedro/mte-dicionarios")

# Staging do job — some ao final, não precisa sobreviver entre execuções.
DIR_TEMP = Path(os.getenv("STAGING_DIR") or ".") / "_staging"

# Mesma pasta que o publicador da gold lê pra subir ao Hub.
DIR_GOLD_LOCAL = Path(__file__).resolve().parents[1] / "publicacao" / "gold"


def repo_bronze(tabela: str) -> str:
    return REPO_BRONZE_RAIS if tabela.startswith("rais") else REPO_BRONZE_CAGED


def repo_silver_ti(tabela: str) -> str:
    return REPO_SILVER_TI_RAIS if tabela.startswith("rais") else REPO_SILVER_TI_CAGED


def fonte_leitura(repo: str, prefixo: str = "") -> str:
    """`hf://datasets/<repo>/<prefixo>` — o DuckDB lê e globa direto, sem credencial."""
    prefixo = prefixo.lstrip("/")
    return f"hf://datasets/{repo}/{prefixo}" if prefixo else f"hf://datasets/{repo}"


def glob_bronze(tabela: str, resto: str = "**/*.parquet") -> str:
    return fonte_leitura(repo_bronze(tabela), f"{tabela}/{resto}")


def glob_silver_ti(tabela: str, resto: str = "**/*.parquet") -> str:
    return fonte_leitura(repo_silver_ti(tabela), f"{tabela}/{resto}")


def caminho_gold(nome: str) -> str:
    """Arquivo local que tanto lê quanto grava uma tabela da gold final."""
    return str(DIR_GOLD_LOCAL / f"{nome}.parquet")


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
        return False  # repo ainda não existe (primeira execução)


def anos_no_repo(repo: str, tabela: str, chave_particao: str) -> set[int]:
    """
    Anos (`ano=`/`ano_particao=`) já presentes num repo, por LISTAGEM — sem
    ler dado. Usado pra decidir "bronze tem o que a silver não tem" e
    "silver tem o que a gold não tem" sem bater limite de taxa do Hub.
    """
    import re

    anos = set()
    for caminho in arquivos_remotos(repo):
        if not caminho.startswith(f"{tabela}/"):
            continue
        m = re.search(rf"{chave_particao}=(\d{{4}})", caminho)
        if m:
            anos.add(int(m.group(1)))
    return anos


_cache_fontes: dict[tuple, set] = {}

# Tabelas grandes o bastante (30+ GB) pra NÃO caber numa leitura só — só
# essas ficam com a consulta restrita ao ano. As do CAGED (cada uma, no
# máximo, algumas centenas de MB somando todos os anos) leem inteiras de
# uma vez, o que evita bater limite de taxa do Hub (ver caminhos_fonte_ingeridos).
TABELAS_GRANDES = {"rais_vinc", "rais_estab"}


def caminhos_fonte_ingeridos(tabela: str, ano: int) -> set:
    """
    Caminhos do FTP (`caminho_fonte`, coluna gravada em toda linha da
    bronze) já representados no repositório de bronze — NÃO pelo nome do
    arquivo de destino (arquivo grande é quebrado em `_parteNN` só na
    publicação; o nome "sem parte" nunca existe sozinho no Hub).
    """
    tabela_inteira = tabela not in TABELAS_GRANDES
    chave = tabela if tabela_inteira else (tabela, ano)
    if chave in _cache_fontes:
        resultado = _cache_fontes[chave]
        if resultado is None:
            raise RuntimeError(f"consulta de caminho_fonte de {chave} já tinha "
                                "falhado nesta execução (ver tentativa original acima)")
        return resultado

    import time
    import duckdb

    alvo_log = tabela if tabela_inteira else f"{tabela}/{ano}"
    print(f"      🔗 consultando {alvo_log} no Hub...")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET enable_progress_bar=false;")
    aplicar_secret_hf(con)
    # "ano={ano}*" casa tanto ano=2022 quanto ano=2022_parcial.
    origem = glob_bronze(tabela) if tabela_inteira else glob_bronze(tabela, f"ano={ano}*/**/*.parquet")

    # Limite de taxa do Hub: a janela observada na prática foi de até 300s.
    # 6 tentativas com espera crescente (8s, 16, 32, 64, 128, 256 ≈ 504s)
    # cobre isso sem travar o job por tempo absurdo se o erro for outro.
    erro_final = None
    esperas = [8, 16, 32, 64, 128]
    for tentativa in range(6):
        try:
            linhas = con.execute(
                f"SELECT DISTINCT caminho_fonte FROM read_parquet('{origem}', union_by_name=true)"
            ).fetchall()
            fontes = {r[0] for r in linhas if r[0]}
            _cache_fontes[chave] = fontes
            return fontes
        except Exception as e:
            # "No files found" é resposta válida (nada publicado ainda) —
            # qualquer outro erro (rede, timeout, limite de taxa) é
            # transitório e vale tentar de novo.
            if "No files found" in str(e):
                _cache_fontes[chave] = set()
                return set()
            erro_final = e
            if tentativa < len(esperas):
                espera = esperas[tentativa]
                print(f"      ⏳ {alvo_log}: tentativa {tentativa + 1}/6 falhou "
                      f"({str(e)[:80]}), esperando {espera}s...")
                time.sleep(espera)

    # Cacheia a FALHA (None, não set()) pra não martelar de novo no mesmo
    # processo, e levanta — um set() vazio aqui seria lido como "nada
    # publicado ainda" e reprocessaria à toa (ou duplicaria dado existente).
    _cache_fontes[chave] = None
    alvo = tabela if tabela_inteira else f"{tabela} (ano {ano})"
    raise RuntimeError(
        f"não consegui consultar caminho_fonte de {alvo} depois de 6 tentativas: {erro_final}"
    )


def publicar_arquivo(local: Path, repo: str, caminho_repo: str, privado: bool = False) -> None:
    from huggingface_hub import HfApi

    api = HfApi()
    api.create_repo(repo_id=repo, repo_type="dataset", exist_ok=True, private=privado)
    api.upload_file(path_or_fileobj=str(local), path_in_repo=caminho_repo,
                     repo_id=repo, repo_type="dataset")
    arquivos_remotos(repo).add(caminho_repo)  # mantém o cache coerente sem relistar


def publicar_diretorio(local_dir: Path, repo: str, prefixo_repo: str = "") -> int:
    """
    Sobe toda a árvore local (resultado de um COPY particionado hive) pro
    Hugging Face, preservando a estrutura de pastas, e apaga o local ao
    final. Em lote (um commit): um COPY com PARTITION_BY pode gerar várias
    subpastas de uma vez (um arquivo-fonte do CAGED antigo pode espalhar
    linhas por vários ano_particao/mes_particao).
    """
    arquivos = sorted(local_dir.rglob("*.parquet"))
    if not arquivos:
        return 0

    from huggingface_hub import HfApi, CommitOperationAdd

    api = HfApi()
    api.create_repo(repo_id=repo, repo_type="dataset", exist_ok=True)
    operacoes = [
        CommitOperationAdd(
            path_in_repo=(f"{prefixo_repo}/{arq.relative_to(local_dir).as_posix()}"
                          if prefixo_repo else arq.relative_to(local_dir).as_posix()),
            path_or_fileobj=str(arq),
        )
        for arq in arquivos
    ]
    api.create_commit(repo_id=repo, repo_type="dataset", operations=operacoes,
                      commit_message=f"{len(operacoes)} arquivo(s)")
    _cache_listagem.pop(repo, None)  # relista na próxima consulta: mudou de verdade

    for arq in arquivos:
        arq.unlink()
    return len(arquivos)


def preparar_staging() -> None:
    DIR_TEMP.mkdir(parents=True, exist_ok=True)


def aplicar_secret_hf(con) -> None:
    """
    Autentica as leituras `hf://` do DuckDB com o HF_TOKEN do ambiente, e
    deixa o retry de HTTP mais paciente.

    Sem o SECRET explícito, toda leitura `hf://` via DuckDB é ANÔNIMA — o
    DuckDB não lê a variável de ambiente HF_TOKEN sozinho. E mesmo
    autenticado, o download do arquivo em si (resolve/main/...) tem limite
    de taxa próprio (do CDN) — por isso o retry mais paciente também.

    O `http_timeout` importa tanto quanto o retry: sem ele, uma conexão que
    trava (sem responder, sem dar 429) nunca aciona o retry — fica parada
    até o job inteiro bater o limite do runner, em vez de falhar em minutos
    e deixar o retry de 6 tentativas (até ~504s) fazer o trabalho dele.
    """
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET http_timeout=60000;")
    con.execute("SET http_retries=6;")
    con.execute("SET http_retry_wait_ms=8000;")
    con.execute("SET http_retry_backoff=2;")

    token = os.getenv("HF_TOKEN")
    if not token:
        return
    con.execute(f"""
        CREATE OR REPLACE SECRET secret_hf (
            TYPE huggingface,
            TOKEN '{token.replace("'", "''")}'
        );
    """)


DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")
DUCKDB_THREADS = os.getenv("DUCKDB_THREADS")  # vazio = deixa o DuckDB decidir
DIR_SPILL = DIR_TEMP / "duckdb_spill"


def conectar_duckdb():
    """
    Conexão DuckDB padrão do projeto: httpfs + secret do Hub + ajustes de
    memória (a RAIS passa de 10 GB descompactada — sem spill pra disco, o
    runner do Actions (7 GB de RAM) estoura).
    """
    import duckdb

    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false;")
    aplicar_secret_hf(con)

    DIR_SPILL.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET temp_directory='{DIR_SPILL.as_posix()}';")
    con.execute("SET preserve_insertion_order=false;")
    if DUCKDB_THREADS:
        con.execute(f"SET threads={int(DUCKDB_THREADS)};")
    return con
