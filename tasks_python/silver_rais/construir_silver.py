"""
Constrói a camada silver da RAIS a partir da bronze + dicionários.

Mesmo princípio do silver_caged: LEFT JOIN nos dicionários (coluna nova
"<coluna>_descricao", código original preservado), tipagem de numéricos e
datas quando configurado em mapeamento.py.

Diferença de particionamento: a RAIS não tem competência mensal como o CAGED
— cada arquivo é ano inteiro (rais_estab) ou ano+UF/região (rais_vinc), então
a silver particiona só por ano_particao (ver catalogo.py: ItemTrabalho nunca
grava mes_particao para RAIS).

Uso (a partir de tasks_python, com o .venv ativo):

    python -m silver_rais.construir_silver --listar
    python -m silver_rais.construir_silver --tabela rais_estab rais_vinc
"""
import argparse
import re
import sys
import time

from extracao_ftp.config_extracao import (
    BUCKET_BRONZE,
    BUCKET_SILVER,
    bucket_silver,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)
from gold_caged import escopo_tecnologia as esc
from silver_caged.dicionarios import chave_normalizada, criar_view, existe
from silver_rais import mapeamento as mp

COLUNAS_TECNICAS = {
    "ano_particao", "mes_particao", "recorte_particao",
    "arquivo_fonte", "caminho_fonte", "data_ingestao", "ano", "mes",
}


def _fs_minio():
    import s3fs

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}", "region_name": MINIO_REGION},
    )


def _colunas_bronze(con, tabela: str) -> list[str]:
    caminho = f"s3://{BUCKET_BRONZE}/{tabela}/**/*.parquet"
    try:
        return [
            r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{caminho}') LIMIT 0"
            ).fetchall()
        ]
    except Exception as e:
        print(f"   ⚠️  Não consegui ler o schema de {tabela}: {str(e)[:200]}")
        return []


def _mapa_traducao(fs, tabela: str, colunas: list[str]) -> dict[str, dict]:
    """
    {coluna_bronze: spec} para esta tabela.

    Aqui NÃO há casamento automático por nome, ao contrário do CAGED: na RAIS
    o nome da coluna quase nunca é o nome da aba (só `municipio` casava), e
    varrer as 21 planilhas de layout atrás de abas homônimas fundiria tabelas
    de código de anos diferentes. Todo de/para é declarado e conferido em
    mapeamento.py.
    """
    mapa = {}
    for col, spec in mp.MAPA_MANUAL.get(tabela, {}).items():
        # O nome muda entre layouts (2023+ acrescentou "_codigo"); o mapa é
        # escrito com o nome canônico e o resolvedor acha o real.
        real = mp.resolver(col, colunas)
        if real is None:
            continue
        col = real
        if not existe(fs, mp.NAMESPACE_DICIONARIO, spec["aba"], spec.get("planilha")):
            print(f"      ⚠️  {col}: aba {spec['aba']} não existe em "
                  f"{spec.get('planilha')} — segue sem tradução")
            continue
        mapa[col] = {"namespace": mp.NAMESPACE_DICIONARIO, **spec}
    return mapa


def preparar_dicionarios(con, fs, tabela: str, colunas: list[str]) -> dict[str, str]:
    """
    Materializa UMA VEZ, por tabela, os dicionários que ela usa.

    Mesma razão do silver_caged: view é preguiçosa, então cada tradução releria
    o parquet do dicionário no MinIO — e agora que a RAIS processa arquivo a
    arquivo isso seria uma releitura por arquivo. Em tabela temporária a leitura
    acontece uma vez só.

    Devolve {coluna_do_fato: nome_da_tabela_temporaria}, só para os dicionários
    que existem e vieram com linhas.
    """
    mapa = _mapa_traducao(fs, tabela, colunas)
    if not mapa:
        print("   ⚠️  Nenhuma coluna traduzível — confira MAPA_MANUAL em mapeamento.py.")
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


def _select_silver(fs, con, tabela: str, colunas: list[str],
                   dicionarios: dict[str, str], caminho_bronze: str,
                   so_tecnologia: bool = True,
                   faixa: tuple[int, int] | None = None) -> str:
    numericos = {k: v for k, v in mp.NUMERICOS.items() if k in colunas}
    datas_aaaamm = [c for c in mp.DATAS_AAAAMM if c in colunas]

    joins, expressoes = [], []

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
            chave_fato = chave_normalizada(f'b."{col}"')
            joins.append(
                f'LEFT JOIN {nome_view} AS "{nome_view}" '
                f'ON {chave_fato} = "{nome_view}".codigo_norm'
            )
            expressoes.append(f'"{nome_view}".descricao AS "{col}_descricao"')

        if col in datas_aaaamm:
            expressoes.append(f'try_strptime(trim(b."{col}"), \'%Y%m\')::DATE AS "{col}_data"')

    for col in COLUNAS_TECNICAS & set(colunas):
        expressoes.append(f'b."{col}" AS "{col}"')

    select = ",\n            ".join(expressoes)
    join_sql = "\n            ".join(joins)

    # O filtro entra ANTES dos LEFT JOINs de dicionário: descarta ~99% das
    # linhas antes de traduzir qualquer coisa. Traduzir 55 milhões de vínculos
    # por ano para depois jogar quase todos fora seria desperdício puro.
    where = ""
    if so_tecnologia:
        # Resolve pelo mesmo caminho da tradução: em 2023+ as colunas ganharam
        # "_codigo" e o CBO inverteu a ordem das palavras. Sem isto o filtro era
        # PULADO e o mercado inteiro entrava no bucket de TI — 402 milhões de
        # linhas, com o aviso perdido no meio do log.
        col_cnae, col_cbo = esc.colunas_da_tabela(tabela, colunas)
        col_cnae = mp.resolver(col_cnae, colunas) if col_cnae else             mp.resolver("cnae_20_subclasse", colunas)
        col_cbo = mp.resolver(col_cbo, colunas) if col_cbo else             mp.resolver("cbo_ocupacao_2002", colunas)
        predicado = esc.sql_filtro_tecnologia(
            f'b."{col_cnae}"' if col_cnae else None,
            f'b."{col_cbo}"' if col_cbo else None,
        )
        if predicado:
            where = f"WHERE {predicado}"
        else:
            print(f"      ⚠️  {tabela}: sem coluna de CNAE/CBO — gravando sem recorte")

    # Leitura em FAIXA DE LINHAS quando o arquivo é grande demais para uma
    # passada só. `file_row_number` é podado por row group pelo próprio DuckDB
    # — medido: ler as linhas 5.000.000 a 5.500.000 de um arquivo de 5,6 mi
    # levou 0,0 s, ou seja, ele não varreu o que estava fora da faixa.
    if faixa:
        fonte = (f"SELECT * EXCLUDE (file_row_number) "
                 f"FROM read_parquet('{caminho_bronze}', file_row_number=true) AS b")
        corte = f"file_row_number >= {faixa[0]} AND file_row_number < {faixa[1]}"
        where = f"{where} AND {corte}" if where else f"WHERE {corte}"
    else:
        fonte = f"SELECT * FROM read_parquet('{caminho_bronze}') AS b"

    return f"""
        SELECT
            {select}
        FROM ({fonte} {where}) AS b
        {join_sql}
    """


def _colunas_arquivo(con, caminho_s3: str) -> list[str]:
    """Schema de UM arquivo do bronze — o layout da RAIS muda entre períodos."""
    try:
        return [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{caminho_s3}') LIMIT 0").fetchall()]
    except Exception as e:
        print(f"   ⚠️  Não consegui ler o schema de {caminho_s3}: {str(e)[:200]}")
        return []


def _stems_existentes(fs, bucket: str, tabela: str) -> set[str]:
    """
    O que já foi gravado, identificado por ANO + arquivo de origem.

    O ano é indispensável: no bronze da RAIS o nome não o carrega
    ("ano=2019/rais_vinc_sp.parquet"), então `rais_vinc_sp` existe nos 19 anos.
    Comparando só o nome, a retomada pulava cada UF em todos os anos seguintes
    ao primeiro processado — 2007 saiu com 37 arquivos e 2009 com UM, sem erro
    nenhum no log.
    """
    existentes = set()
    for caminho in fs.glob(f"{bucket}/{tabela}/**/*.parquet"):
        nome = caminho.split("/")[-1].removesuffix(".parquet")
        m = re.search(r"ano_particao=(\d{4})", caminho)
        existentes.add(f"{m.group(1) if m else '?'}/{nome.rsplit('_', 1)[0]}")
    return existentes


# Alvo de linhas por passada. Calibrado pelo que comprovadamente coube nesta
# máquina: os arquivos do CAGED têm ~4,4 milhões de linhas e passaram sempre.
# Os da RAIS chegam a 25,5 milhões (rais_vinc_sp), num pipeline com 33 joins —
# seis vezes mais linhas e vinte vezes mais bytes que o maior arquivo do CAGED.
LINHAS_POR_PEDACO = 3_000_000


def _pedacos(con, origem_s3: str, stem: str) -> list[tuple[str, tuple[int, int] | None]]:
    """
    Divide um arquivo grande em faixas de linhas processáveis.

    Devolve [(rotulo, faixa)]; faixa None significa "o arquivo inteiro de uma
    vez", que é o caso da maioria.

    A contagem sai do RODAPÉ do parquet (`parquet_file_metadata`), sem ler
    dados. E o corte por `file_row_number` é podado por row group pelo DuckDB,
    então cada pedaço lê só a parte dele — não é uma varredura completa por
    pedaço.

    O rótulo carrega o número da parte porque ele vira nome de arquivo, e é
    ele que a retomada usa para saber o que já foi gravado: cair no meio de um
    arquivo de 25 milhões de linhas passa a custar um pedaço, não o arquivo.

    "parte" por extenso, e não "p": o sufixo vai para os nomes publicados no
    Hugging Face, onde é lido por quem baixa o dataset. "p00" seria ambíguo
    (página? partição? parte?) para quem não conhece o pipeline.
    """
    try:
        grupos = con.execute(f"""
            SELECT DISTINCT row_group_id, row_group_num_rows
            FROM parquet_metadata('{origem_s3}')
            ORDER BY row_group_id
        """).fetchall()
    except Exception:
        return [(stem, None)]

    total = sum(n for _, n in grupos)
    if not total:
        return [(stem, None)]
    if total <= LINHAS_POR_PEDACO:
        return [(stem, None)]

    # Divisão UNIFORME: quantos pedaços cabem, e aí divide por igual. Fatias
    # fixas deixariam um resto — 9.095.508 linhas viravam três de 3 milhões
    # mais uma de 95 mil, e essa sobra vira um parquet minúsculo no dataset
    # publicado. Com 4 pedaços de ~2,27 milhões não sobra nada.
    quantidade = -(-total // LINHAS_POR_PEDACO)

    # Cortes na BORDA DOS ROW GROUPS, não em qualquer linha. O row group é a
    # unidade mínima que o parquet sabe pular: um corte no meio obriga o DuckDB
    # a ler o grupo inteiro e descartar metade. Na borda, a poda é exata.
    bordas = []
    acumulado = 0
    for _, linhas in grupos:
        acumulado += linhas
        bordas.append(acumulado)

    # Para cada divisa ideal, a borda de row group MAIS PRÓXIMA — não a
    # primeira que ultrapassa. Fechar o pedaço assim que passa do alvo faz o
    # excesso se acumular a cada volta e empurrar toda a sobra para o último:
    # medido no rais_vinc_sp, dava oito pedaços de 3,01 milhões e um de 1,38.
    cortes = []
    for k in range(1, quantidade):
        ideal = total * k / quantidade
        borda = min(bordas, key=lambda b: abs(b - ideal))
        if borda not in cortes and 0 < borda < total:
            cortes.append(borda)

    partes, inicio = [], 0
    for fim in cortes + [total]:
        if fim > inicio:
            partes.append((f"{stem}_parte{len(partes):02d}", (inicio, fim)))
            inicio = fim
    return partes


def _copy_particionado(destino_s3: str, stem: str, query: str) -> str:
    """
    COPY com partição hive por ano.

    A RAIS não tem competência mensal — cada arquivo é um ano inteiro
    (rais_estab) ou ano+UF (rais_vinc) — então a partição é só ano_particao.
    Diferente do CAGED, onde o mês existe e entra na árvore.

    FILENAME_PATTERN carimba o arquivo de origem porque no rais_vinc VÁRIOS
    arquivos caem no mesmo ano (um por região): sem isso, cada um sobrescreveria
    o anterior dentro de ano_particao=YYYY e sobraria só a última região.
    """
    return f"""
        COPY ({query}) TO '{destino_s3}' (
            FORMAT PARQUET,
            PARTITION_BY (ano_particao),
            FILENAME_PATTERN '{stem}_{{i}}',
            COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
            OVERWRITE_OR_IGNORE true
        );
    """


def construir(con, fs, tabela: str, so_tecnologia: bool = True,
              forcar: bool = False, ano_inicio: int = 0, ano_fim: int = 9999) -> bool:
    """
    Constrói a silver de uma tabela, UM ARQUIVO BRONZE POR VEZ.

    Antes isto era um único COPY sobre o glob da tabela inteira. No CAGED essa
    forma estourou a memória e foi trocada; aqui seria pior, porque o rais_vinc
    são 34 GB de bronze numa máquina de 10 GB — não é questão de se falharia.

    Arquivo a arquivo o pico de memória fica no tamanho de um arquivo, a carga
    vira retomável (pula o que já existe) e cada arquivo pode ter seu próprio
    schema, o que importa porque o layout da RAIS muda ao longo dos anos.
    """
    bucket_destino = bucket_silver(so_tecnologia)
    recorte = "só tecnologia" if so_tecnologia else "mercado completo"
    print(f"\n{'=' * 70}\n  🔨 SILVER: {tabela}  ({recorte} → {bucket_destino})\n{'=' * 70}")
    inicio = time.time()

    arquivos = sorted(fs.glob(f"{BUCKET_BRONZE}/{tabela}/**/*.parquet"))
    if not arquivos:
        print("   ⏭️  Sem dados em bronze para esta tabela, pulando.")
        return False

    if ano_inicio or ano_fim != 9999:
        arquivos = [a for a in arquivos
                    if (ano := _ano_do_caminho(a)) is None or ano_inicio <= ano <= ano_fim]

    # Dicionários uma vez por tabela, a partir da união das colunas de todos os
    # arquivos — não uma vez por arquivo.
    dicionarios = preparar_dicionarios(con, fs, tabela, _colunas_bronze(con, tabela))
    ja_gravados = set() if forcar else _stems_existentes(fs, bucket_destino, tabela)

    destino_s3 = f"s3://{bucket_destino}/{tabela}"
    total_linhas = feitos = pulados = falhas = 0

    for n, origem in enumerate(arquivos, start=1):
        stem = origem.split("/")[-1].removesuffix(".parquet")
        origem_s3 = f"s3://{origem}"

        # A retomada é por ANO + PEDAÇO. O ano porque o nome não o carrega; o
        # pedaço porque em rais_vinc_sp são 9, e cair no último não pode custar
        # os oito anteriores.
        ano_arq = _ano_do_caminho(origem) or "?"
        partes = [(rotulo, faixa) for rotulo, faixa in _pedacos(con, origem_s3, stem)
                  if f"{ano_arq}/{rotulo}" not in ja_gravados]
        if not partes:
            pulados += 1
            continue

        colunas = _colunas_arquivo(con, origem_s3)
        if not colunas:
            falhas += 1
            continue

        for rotulo, faixa in partes:
            query = _select_silver(fs, con, tabela, colunas, dicionarios, origem_s3,
                                   so_tecnologia=so_tecnologia, faixa=faixa)
            try:
                con.execute(_copy_particionado(destino_s3, rotulo, query))
                linhas = con.execute(
                    f"SELECT count(*) FROM read_parquet('{destino_s3}/**/{rotulo}_*.parquet')"
                ).fetchone()[0]
                total_linhas += linhas
                feitos += 1
                print(f"   [{n}/{len(arquivos)}] ✅ {rotulo}: {linhas:,} linhas")
            except Exception as e:
                falhas += 1
                print(f"   [{n}/{len(arquivos)}] ❌ {rotulo}: {str(e)[:200]}")

    print(f"   📊 {feitos} gravado(s), {pulados} já existente(s), {falhas} falha(s) "
          f"| {total_linhas:,} linhas novas em {(time.time() - inicio) / 60:.1f} min")
    return falhas == 0


def _ano_do_caminho(caminho: str) -> int | None:
    m = re.search(r"ano=(\d{4})", caminho)
    return int(m.group(1)) if m else None


def _argumentos():
    p = argparse.ArgumentParser(description="Constrói a camada silver da RAIS.")
    p.add_argument("--tabela", nargs="+", choices=mp.TABELAS_RAIS, default=list(mp.TABELAS_RAIS))
    p.add_argument("--listar", action="store_true", help="Só mostra o mapeamento de tradução e sai")
    p.add_argument("--mercado-completo", action="store_true",
                   help="Grava TODO o mercado em bucket separado, em vez de só tecnologia. "
                        "Ordens de grandeza maior — a RAIS completa passa de 40 GB.")
    p.add_argument("--forcar", action="store_true",
                   help="Reprocessa arquivos que já existem na silver (padrão: pula)")
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    return p.parse_args()


def main() -> int:
    args = _argumentos()
    con = conectar_duckdb()
    fs = _fs_minio()

    if args.listar:
        for tabela in args.tabela:
            colunas = _colunas_bronze(con, tabela)
            if not colunas:
                print(f"{tabela}: sem dados em bronze ainda")
                continue
            mapa = _mapa_traducao(fs, tabela, colunas)
            print(f"\n{tabela} ({len(colunas)} colunas, {len(mapa)} traduzíveis):")
            for col in colunas:
                marca = "✅" if col in mapa else "  "
                extra = f" -> {mapa[col]['namespace']}/{mapa[col]['aba']}" if col in mapa else ""
                print(f"   {marca} {col}{extra}")
        return 0

    sucesso = 0
    for tabela in args.tabela:
        if construir(con, fs, tabela, so_tecnologia=not args.mercado_completo,
                     forcar=args.forcar, ano_inicio=args.ano_inicio, ano_fim=args.ano_fim):
            sucesso += 1

    print(f"\n🏁 {sucesso}/{len(args.tabela)} tabela(s) construída(s) na silver.")
    return 0 if sucesso == len(args.tabela) else 2


if __name__ == "__main__":
    sys.exit(main())
