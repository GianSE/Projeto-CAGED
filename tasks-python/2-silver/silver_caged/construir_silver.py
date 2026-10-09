"""
Constrói a camada silver do CAGED a partir da bronze + dicionários — já
filtrada em TI (recorte de tecnologia, ~1% das linhas) e em partição hive
(ano_particao=/mes_particao=), publicada em Gianpedro/silver-caged-ti.

Para cada tabela (caged_mov, caged_for, caged_exc, caged_old, caged_ajustes):
  1. Filtra o recorte de TECNOLOGIA (ver gold_caged/escopo_tecnologia.py).
  2. Para cada coluna codificada com dicionário disponível, faz LEFT JOIN e
     acrescenta uma coluna "<coluna>_descricao" com o texto legível — o
     código original é mantido, nada é substituído.
  3. Tipa os campos numéricos (vírgula decimal -> ponto, cast) e monta uma
     coluna de data a partir da competência AAAAMM.

RECORTE DE TECNOLOGIA
----------------------
O estudo é sobre o mercado de trabalho em tecnologia, e TI é ~1% dos
registros: gravar o mercado inteiro custaria ~100x mais espaço para dados
que nunca seriam consultados. O bronze continua completo (fonte da
verdade, dá para re-derivar com outra definição de TI quando quiser).

Uso (a partir de tasks-python, com o .venv ativo):
    python -m silver_caged.construir_silver --tabela caged_mov --ano-inicio 2024 --ano-fim 2024
"""
import argparse
import re
import sys
import time

from silver_caged import mapeamento as mp
from gold_caged import escopo_tecnologia as esc
from silver_caged.dicionarios import chave_normalizada, criar_view, existe
from _utils import nuvem

# Colunas que nunca são candidatas a tradução/tipagem (linhagem e partição)
COLUNAS_TECNICAS = {
    "ano_particao", "mes_particao", "recorte_particao",
    "arquivo_fonte", "caminho_fonte", "data_ingestao", "ano", "mes",
}


def _colunas_arquivo(con, caminho: str) -> list[str]:
    """Schema de UM arquivo parquet (o schema varia entre eras do CAGED antigo)."""
    try:
        return [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{caminho}') LIMIT 0").fetchall()]
    except Exception as e:
        print(f"   ⚠️  Não consegui ler o schema de {caminho}: {str(e)[:200]}")
        return []


def _colunas_bronze(con, tabela: str) -> list[str]:
    # Sem hive_partitioning: nem toda tabela tem o mesmo esquema de pastas
    # (caged_ajustes mistura arquivo anual "ano=2002/arq.parquet" com mensal
    # "ano=2010/mes=1/arq.parquet"). ano_particao/mes_particao já vêm como
    # colunas de verdade em cada linha, gravadas na ingestão bronze.
    caminho = nuvem.glob_bronze(tabela)
    try:
        return [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{caminho}') LIMIT 0").fetchall()]
    except Exception as e:
        print(f"   ⚠️  Não consegui ler o schema de {tabela}: {str(e)[:200]}")
        return []


def _mapa_traducao(tabela: str, colunas: list[str]) -> dict[str, dict]:
    """Devolve {coluna_bronze: spec} só para colunas com dicionário disponível no Hub."""
    geracao = mp.geracao(tabela)
    mapa = {}

    if geracao == "novo":
        for col in colunas:
            if col in COLUNAS_TECNICAS or col in mp.ABAS_NAO_TRADUZIVEIS:
                continue
            if existe(mp.BUCKET_SILVER_NAMESPACE_NOVO, col):
                mapa[col] = {"namespace": mp.BUCKET_SILVER_NAMESPACE_NOVO, "aba": col, "estilo": "2col"}
    else:
        for col, spec in mp.MAPA_CAGED_ANTIGO.items():
            if col not in colunas:
                continue
            if not existe(mp.BUCKET_DICT_NAMESPACE_ANTIGO, spec["aba"]):
                continue
            mapa[col] = {"namespace": mp.BUCKET_DICT_NAMESPACE_ANTIGO, **spec}

    return mapa


def preparar_dicionarios(con, tabela: str, colunas: list[str]) -> dict[str, str]:
    """
    Materializa UMA VEZ, por tabela, os dicionários que ela usa — em vez de
    recriar as views a cada arquivo do bronze (cada um relendo o parquet
    do dicionário no Hub).
    """
    mapa = _mapa_traducao(tabela, colunas)
    if not mapa:
        print("   ⚠️  Nenhuma coluna com dicionário disponível — silver sairá só tipada.")
        return {}

    prontos = {}
    for col, spec in mapa.items():
        spec = dict(spec)
        namespace, aba, estilo = spec.pop("namespace"), spec.pop("aba"), spec.pop("estilo")
        nome_view = f"dic_{tabela}_{col}"
        if criar_view(con, namespace, aba, estilo, nome_view, materializar=True, **spec):
            prontos[col] = nome_view

    print(f"   📖 {len(prontos)} coluna(s) com tradução: {', '.join(sorted(prontos))}")
    return prontos


def _select_silver(con, tabela: str, colunas: list[str], caminho_bronze: str,
                   dicionarios: dict[str, str]) -> str:
    geracao = mp.geracao(tabela)
    numericos = mp.NUMERICOS_NOVO_CAGED if geracao == "novo" else mp.NUMERICOS_CAGED_ANTIGO
    datas_aaaamm = mp.DATAS_AAAAMM_NOVO_CAGED if geracao == "novo" else mp.DATAS_AAAAMM_CAGED_ANTIGO
    numericos = {k: v for k, v in numericos.items() if k in colunas}
    datas_aaaamm = [c for c in datas_aaaamm if c in colunas]

    joins = []
    expressoes = []

    for col in colunas:
        if col in COLUNAS_TECNICAS:
            continue

        if col in numericos:
            tipo = numericos[col]
            expressoes.append(f'try_cast(replace(trim(b."{col}"), \',\', \'.\') AS {tipo}) AS "{col}"')
        else:
            expressoes.append(f'b."{col}" AS "{col}"')

        if col in dicionarios:
            nome_view = dicionarios[col]
            # O CAGED antigo grava código curto com zero à esquerda ("02",
            # "07"), mas o dicionário do layout traz o código sem padding
            # ("2", "7"). A chave canônica resolve isso dos dois lados.
            chave_fato = chave_normalizada(f'b."{col}"')
            joins.append(f'LEFT JOIN {nome_view} AS "{nome_view}" '
                        f'ON {chave_fato} = "{nome_view}".codigo_norm')
            expressoes.append(f'"{nome_view}".descricao AS "{col}_descricao"')

        if col in datas_aaaamm:
            expressoes.append(f'try_strptime(trim(b."{col}"), \'%Y%m\')::DATE AS "{col}_data"')

    for col in COLUNAS_TECNICAS & set(colunas):
        expressoes.append(f'b."{col}" AS "{col}"')

    select = ",\n            ".join(expressoes)
    join_sql = "\n            ".join(joins)

    # Recorte de tecnologia já na leitura do bronze: o DuckDB descarta ~99%
    # das linhas antes de traduzir qualquer coisa.
    col_cnae, col_cbo = esc.colunas_da_tabela(tabela, colunas)
    predicado = esc.sql_filtro_tecnologia(
        f'b."{col_cnae}"' if col_cnae else None,
        f'b."{col_cbo}"' if col_cbo else None,
    )
    where = f"WHERE {predicado}" if predicado else ""
    if not predicado:
        print(f"      ⚠️  {tabela}: sem coluna de CNAE/CBO neste arquivo — "
              f"gravando sem recorte de tecnologia")

    return f"""
        SELECT
            {select}
        FROM (SELECT * FROM read_parquet('{caminho_bronze}') AS b {where}) AS b
        {join_sql}
    """


def _ano_do_caminho(caminho: str) -> int | None:
    m = re.search(r"ano=(\d{4})", caminho)
    return int(m.group(1)) if m else None


def _copy_particionado(destino: str, stem: str, query: str) -> str:
    """
    COPY com partição hive ano_particao=/mes_particao=.

    PARTITION_BY sobre as COLUNAS, não sobre o caminho do bronze de
    origem: caged_ajustes mistura arquivo anual com mensal, e derivar a
    partição do caminho produziria profundidade irregular.

    FILENAME_PATTERN carimba o arquivo de origem: sem isso todo COPY
    escreveria "data_0.parquet" na mesma pasta e um mês sobrescreveria o
    outro. OVERWRITE_OR_IGNORE é obrigatório: a partir do segundo arquivo
    a pasta de destino já existe.

    O mês vai SEM zero à esquerda (mes_particao=1, não =01) — com zero o
    DuckDB infere VARCHAR pra partição, e filtro por faixa ("1º semestre")
    quebra ("Cannot compare VARCHAR and INTEGER_LITERAL").
    """
    return f"""
        COPY ({query}) TO '{destino}' (
            FORMAT PARQUET,
            PARTITION_BY (ano_particao, mes_particao),
            FILENAME_PATTERN '{stem}_{{i}}',
            COMPRESSION 'ZSTD',
            COMPRESSION_LEVEL 3,
            OVERWRITE_OR_IGNORE true
        );
    """


def construir(con, tabela: str, ano_inicio: int, ano_fim: int) -> bool:
    """
    Constrói a silver de uma tabela, UM ARQUIVO BRONZE POR VEZ.

    Processar a tabela inteira num único COPY estouraria a memória (21
    hash tables de dicionário + dezenas de milhões de linhas). Arquivo a
    arquivo o pico de memória fica no tamanho de um mês de dados, e cada
    arquivo pode ter seu próprio schema — o CAGED antigo muda de colunas
    entre eras.

    A decisão de QUAL ANO reprocessar já foi feita na orquestração do CI
    (ver verificar_pendencias_silver): aqui sempre reconstrói o que foi
    pedido, sem checar arquivo a arquivo o que já existe.
    """
    print(f"\n{'=' * 70}\n  🔨 SILVER: {tabela}\n{'=' * 70}")
    inicio = time.time()

    todos = sorted(f"{tabela}/{a}" for a in nuvem.arquivos_remotos(nuvem.repo_bronze(tabela))
                   if a.startswith(f"{tabela}/") and a.endswith(".parquet"))
    arquivos = [a for a in todos
               if (ano := _ano_do_caminho(a)) is None or ano_inicio <= ano <= ano_fim]
    if not arquivos:
        print("   ⏭️  Sem dados em bronze para este recorte, pulando.")
        return False

    # Uma vez por tabela, não uma vez por arquivo: o mapa de tradução é o
    # mesmo para todos, e o schema é a união das colunas (o CAGED antigo
    # muda de colunas entre eras).
    dicionarios = preparar_dicionarios(con, tabela, _colunas_bronze(con, tabela))

    nuvem.preparar_staging()
    destino_local = nuvem.DIR_TEMP / tabela
    destino_local.mkdir(parents=True, exist_ok=True)

    total_linhas = 0
    feitos = falhas = 0

    for n, origem in enumerate(arquivos, start=1):
        stem = origem.split("/")[-1].removesuffix(".parquet")
        _, resto_origem = origem.split("/", 1)
        origem_hf = nuvem.fonte_leitura(nuvem.repo_bronze(tabela), resto_origem)

        colunas = _colunas_arquivo(con, origem_hf)
        if not colunas:
            falhas += 1
            continue

        query = _select_silver(con, tabela, colunas, origem_hf, dicionarios)

        try:
            con.execute(_copy_particionado(str(destino_local), stem, query))
            # Conferência pelo que FOI escrito, não pelo que se pretendia:
            # só o count(*) do parquet de destino prova que saiu legível.
            leitura = f"{destino_local}/**/{stem}_*.parquet"
            try:
                linhas = con.execute(f"SELECT count(*) FROM read_parquet('{leitura}')").fetchone()[0]
            except Exception as e:
                # Filtro que não casa com nada é resultado válido, não
                # falha: o COPY particionado simplesmente não cria arquivo.
                if "No files found" not in str(e):
                    raise
                linhas = 0
            total_linhas += linhas
            feitos += 1
            print(f"   [{n}/{len(arquivos)}] ✅ {origem.split('/')[-1]}: {linhas:,} linhas")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(arquivos)}] ❌ {origem.split('/')[-1]}: {str(e)[:200]}")

    n_publicados = nuvem.publicar_diretorio(destino_local, nuvem.repo_silver_ti(tabela))
    print(f"   ☁️  {n_publicados} arquivo(s) publicado(s) em {nuvem.repo_silver_ti(tabela)}")

    decorrido = time.time() - inicio
    print(f"   📊 {feitos} gravado(s), {falhas} falha(s) "
          f"| {total_linhas:,} linhas em {decorrido / 60:.1f} min")
    return falhas == 0


def _argumentos():
    p = argparse.ArgumentParser(description="Constrói a camada silver do CAGED.")
    p.add_argument("--tabela", nargs="+", choices=mp.TODAS_TABELAS, default=list(mp.TODAS_TABELAS))
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    return p.parse_args()


def main() -> int:
    args = _argumentos()
    con = nuvem.conectar_duckdb()

    sucesso = 0
    for tabela in args.tabela:
        if construir(con, tabela, args.ano_inicio, args.ano_fim):
            sucesso += 1

    print(f"\n🏁 {sucesso}/{len(args.tabela)} tabela(s) construída(s) na silver.")
    return 0 if sucesso == len(args.tabela) else 2


if __name__ == "__main__":
    sys.exit(main())
