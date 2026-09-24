"""
Agrupa os arquivos de cada partição da silver num arquivo só.

POR QUE
-------
A camada nasceu com um arquivo de saída por arquivo do bronze, e o bronze do
MTE é fragmentado: a RAIS vem por UF (e, nos anos grandes, partida em pedaços),
o CAGED vem por mês e ainda recebeu complementos. O resultado eram 1.714
arquivos de menos de 2 MB cada — 35 por ano só na RAIS.

Isso é ruim de ler, e o custo não está no tamanho: cada arquivo é uma conexão,
um rodapé de metadados e um planejamento à parte. Num bucket local isso é
lentidão; via HTTP, no Hugging Face, é uma requisição por arquivo, e a consulta
que deveria levar segundos leva minutos.

O QUE NÃO SE PERDE
------------------
A procedência não está no nome do arquivo: cada LINHA carrega `arquivo_fonte`,
`caminho_fonte` e `data_ingestao`. Juntar dez arquivos numa partição não apaga
de onde cada linha veio — a informação continua consultável, e passa a ser
consultável por linha em vez de por arquivo.

A ORDEM DAS OPERAÇÕES
---------------------
Escrever o agrupado e apagar os pedaços não pode ser feito em qualquer ordem:
- apagar antes de escrever perde dado se o processo morrer no meio;
- escrever na própria partição antes de apagar DUPLICA as linhas enquanto os
  dois existem, e um processo morto aí deixa a partição com o dobro.

Por isso o agrupado é escrito num caminho de preparo (`_agrupando/`), conferido
pela contagem, e só então os pedaços são apagados e o arquivo é movido para a
partição. Se o processo morrer entre apagar e mover, o arquivo está no preparo e
a próxima execução o encontra e conclui a mudança — daí a varredura de preparo
pendente antes de tudo.
"""
import argparse
import sys
import time

from silver_caged.construir_silver import COLUNA_ORIGEM
from extracao_ftp.config_extracao import (
    BUCKET_SILVER,
    BUCKET_SILVER_TI,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)

PREPARO = "_agrupando"
TABELAS = ("caged_mov", "caged_exc", "caged_for", "caged_old", "caged_ajustes",
           "rais_vinc", "rais_estab")


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def _particao(caminho: str) -> str:
    """As pastas `chave=valor` do caminho, que são a partição hive."""
    return "/".join(p for p in caminho.split("/") if "=" in p)


def _nome(tabela: str, particao: str) -> str:
    """Nome do agrupado: a tabela e os valores da partição, nesta ordem."""
    valores = [p.split("=", 1)[1] for p in particao.split("/") if p]
    valores = [v if v != "__HIVE_DEFAULT_PARTITION__" else "sem_mes" for v in valores]
    return f"{tabela}_{'_'.join(valores)}.parquet" if valores else f"{tabela}.parquet"


def _ja_prontas(con, arquivos: list[str]) -> set[str]:
    """
    Quais desses arquivos já são um agrupado completo.

    Critério: ter a coluna de procedência. Uma partição que já tem um arquivo
    só, com o nome canônico e com a coluna, não tem nada a ganhar em ser
    reescrita — e reescrever os 19 arquivos da RAIS custa 1 GB de leitura e
    escrita para produzir exatamente o que já estava lá. `parquet_schema` lê só
    o rodapé, e por arquivo.
    """
    if not arquivos:
        return set()
    lista = ", ".join(f"'s3://{c}'" for c in arquivos)
    return {r[0].removeprefix("s3://") for r in con.execute(
        f"SELECT DISTINCT file_name FROM parquet_schema([{lista}]) "
        f"WHERE name = '{COLUNA_ORIGEM}'").fetchall()}


def _contar(con, caminhos: list[str]) -> int:
    lista = ", ".join(f"'s3://{c}'" for c in caminhos)
    return con.execute(
        f"SELECT count(*) FROM read_parquet([{lista}], hive_partitioning=false, "
        f"union_by_name=true)").fetchone()[0]


def _escrever(con, origem: list[str], destino: str):
    """
    Junta os pedaços num arquivo só, carimbando de onde cada linha veio.

    `union_by_name` porque a série não é homogênea: colunas foram criadas e
    retiradas ao longo dos anos, e dentro de uma mesma partição pode haver
    arquivo antigo e complemento recente. Sem isso, o DuckDB casaria coluna por
    POSIÇÃO e misturaria conteúdo de campos diferentes — o tipo de erro que não
    dá exceção, só resposta errada.

    `hive_partitioning=false` porque a coluna de partição está no CAMINHO, e não
    deve entrar no arquivo: o agrupado ficaria com uma coluna que os vizinhos
    das outras partições não têm.

    `arquivo_bronze` é o que o nome do arquivo dizia e vai deixar de dizer: qual
    parquet do bronze produziu a linha. Sem ela, o construtor não saberia que a
    partição já está feita e gravaria tudo de novo ao lado do agrupado. Em
    reagrupamento a coluna já existe e é preservada — `filename` aí apontaria
    para o agrupado anterior, e não para o bronze.
    """
    lista = ", ".join(f"'s3://{c}'" for c in origem)
    ja_tem = any(c[0] == "arquivo_bronze" for c in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet([{lista}], hive_partitioning=false, "
        f"union_by_name=true)").fetchall())
    derivado = ("regexp_replace(regexp_replace(filename, '^.*/', ''), "
                "'(_\\d+)?\\.parquet$', '')")
    carimbo = (f"coalesce(arquivo_bronze, {derivado})" if ja_tem else derivado)
    con.execute(f"""
        COPY (
            SELECT * EXCLUDE (filename{', arquivo_bronze' if ja_tem else ''}),
                   {carimbo} AS arquivo_bronze
            FROM read_parquet([{lista}], hive_partitioning=false,
                              union_by_name=true, filename=true)
        ) TO 's3://{destino}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL}
        );
    """)


def _mover(fs, origem: str, destino: str):
    """Move no MinIO: copia e apaga. `fs.mv` faz isso, mas sem sobrescrever."""
    if fs.exists(destino):
        fs.rm(destino)
    fs.copy(origem, destino)
    fs.rm(origem)


def _concluir_pendentes(con, fs, bucket: str, tabela: str) -> int:
    """
    Conclui agrupamentos interrompidos de uma execução anterior.

    Um arquivo no preparo significa que ele já foi conferido: o que pode ter
    faltado é apagar os pedaços e movê-lo. Fazer isso antes de qualquer coisa
    evita que a varredura seguinte encontre a partição num estado meio-feito.
    """
    base = f"{bucket}/{PREPARO}/{tabela}"
    pendentes = sorted(fs.glob(f"{base}/**/*.parquet"))
    for staged in pendentes:
        particao = _particao(staged)
        destino_pasta = f"{bucket}/{tabela}/{particao}" if particao else f"{bucket}/{tabela}"
        alvo = f"{destino_pasta}/{staged.split('/')[-1]}"
        pedacos = [c for c in sorted(fs.glob(f"{destino_pasta}/*.parquet"))
                   if c.split("/")[-1] != alvo.split("/")[-1]]
        print(f"   ↻ retomando {particao or tabela}: {len(pedacos)} pedaço(s) a apagar")
        for p in pedacos:
            fs.rm(p)
        _mover(fs, staged, alvo)
    return len(pendentes)


def agrupar(con, fs, bucket: str, tabela: str, minimo: int, so_listar: bool) -> tuple[int, int]:
    """Agrupa cada partição da tabela. Devolve (partições agrupadas, falhas)."""
    retomadas = _concluir_pendentes(con, fs, bucket, tabela)

    arquivos = [c for c in sorted(fs.glob(f"{bucket}/{tabela}/**/*.parquet"))
                if f"/{PREPARO}/" not in c]
    if not arquivos:
        return 0, 0

    por_particao: dict[str, list[str]] = {}
    for c in arquivos:
        por_particao.setdefault(_particao(c), []).append(c)

    # Partição com um arquivo só, já no nome canônico e já com a coluna: nada
    # a fazer. Sem isto, rodar de novo reescreve a camada inteira sem mudar nada.
    unicos = {p: cs[0] for p, cs in por_particao.items()
              if len(cs) == 1 and cs[0].endswith("/" + _nome(tabela, p))}
    prontas = _ja_prontas(con, list(unicos.values()))
    candidatas = {p: cs for p, cs in por_particao.items()
                  if len(cs) >= minimo and unicos.get(p) not in prontas}
    print(f"\n📦 {bucket}/{tabela}: {len(arquivos)} arquivo(s) em {len(por_particao)} "
          f"partição(ões); {len(candidatas)} a agrupar"
          + (f" (retomadas: {retomadas})" if retomadas else ""))
    if so_listar:
        for p, cs in sorted(candidatas.items())[:10]:
            print(f"   {p or '(raiz)'}: {len(cs)} → 1  ({_nome(tabela, p)})")
        return 0, 0

    feitas = falhas = 0
    for n, (particao, pedacos) in enumerate(sorted(candidatas.items()), start=1):
        pasta = f"{bucket}/{tabela}/{particao}" if particao else f"{bucket}/{tabela}"
        nome = _nome(tabela, particao)
        staged = (f"{bucket}/{PREPARO}/{tabela}/{particao}/{nome}" if particao
                  else f"{bucket}/{PREPARO}/{tabela}/{nome}")
        try:
            antes = _contar(con, pedacos)
            _escrever(con, pedacos, staged)
            depois = _contar(con, [staged])
            if depois != antes:
                # Diferença aqui é perda de linha: não apaga nada, deixa a
                # partição como estava e segue para a próxima.
                fs.rm(staged)
                raise RuntimeError(f"contagem divergiu: {antes:,} → {depois:,}")

            for p in pedacos:
                if p.split("/")[-1] != nome:
                    fs.rm(p)
            _mover(fs, staged, f"{pasta}/{nome}")
            feitas += 1
            print(f"   [{n}/{len(candidatas)}] ✅ {particao or tabela}: "
                  f"{len(pedacos)} → 1 ({antes:,} linhas)")
        except Exception as e:  # noqa: BLE001
            falhas += 1
            print(f"   [{n}/{len(candidatas)}] ❌ {particao or tabela}: {str(e)[:180]}")
    return feitas, falhas


def _argumentos():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tabela", action="append",
                   help="tabela a agrupar (repetível); padrão: todas")
    p.add_argument("--camada", choices=("silver", "silver-ti", "ambas"), default="silver-ti")
    # Padrão 1, e não 2: a partição que já tem um arquivo só também precisa
    # passar por aqui, para receber a coluna `arquivo_bronze`. Pulá-la deixava a
    # coluna existindo em 2023, faltando em 2024 e voltando em 2025 — e uma
    # coluna que some e volta é exatamente o que a auditoria acusa como deriva
    # de schema, com razão. Reescrever um arquivo pequeno é barato; schema
    # desigual custa confiança na camada inteira.
    p.add_argument("--minimo", type=int, default=1,
                   help="só agrupa partição com pelo menos N arquivos (padrão: 1)")
    p.add_argument("--so-listar", action="store_true",
                   help="mostra o que faria, sem escrever nem apagar")
    return p.parse_args()


def main() -> int:
    a = _argumentos()
    con, fs = conectar_duckdb(), _fs()
    baldes = {"silver": [BUCKET_SILVER], "silver-ti": [BUCKET_SILVER_TI],
              "ambas": [BUCKET_SILVER_TI, BUCKET_SILVER]}[a.camada]
    tabelas = a.tabela or list(TABELAS)

    inicio = time.time()
    total_feitas = total_falhas = 0
    for bucket in baldes:
        for tabela in tabelas:
            if not fs.exists(f"{bucket}/{tabela}"):
                continue
            feitas, falhas = agrupar(con, fs, bucket, tabela, a.minimo, a.so_listar)
            total_feitas += feitas
            total_falhas += falhas

    print(f"\n🏁 {total_feitas} partição(ões) agrupada(s), {total_falhas} falha(s), "
          f"em {(time.time() - inicio) / 60:.1f} min")
    return 1 if total_falhas else 0


if __name__ == "__main__":
    sys.exit(main())
