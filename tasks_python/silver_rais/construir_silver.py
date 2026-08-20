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
        if col not in colunas:
            continue
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
                   so_tecnologia: bool = True) -> str:
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
        col_cnae, col_cbo = esc.colunas_da_tabela(tabela, colunas)
        predicado = esc.sql_filtro_tecnologia(
            f'b."{col_cnae}"' if col_cnae else None,
            f'b."{col_cbo}"' if col_cbo else None,
        )
        if predicado:
            where = f"WHERE {predicado}"
        else:
            print(f"      ⚠️  {tabela}: sem coluna de CNAE/CBO — gravando sem recorte")

    return f"""
        SELECT
            {select}
        FROM (SELECT * FROM read_parquet('{caminho_bronze}') AS b {where}) AS b
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
    """Arquivos-fonte já gravados, numa listagem só em vez de um exists() por arquivo."""
    existentes = set()
    for caminho in fs.glob(f"{bucket}/{tabela}/**/*.parquet"):
        nome = caminho.split("/")[-1].removesuffix(".parquet")
        existentes.add(nome.rsplit("_", 1)[0])
    return existentes


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
        if stem in ja_gravados:
            pulados += 1
            continue

        origem_s3 = f"s3://{origem}"
        colunas = _colunas_arquivo(con, origem_s3)
        if not colunas:
            falhas += 1
            continue

        query = _select_silver(fs, con, tabela, colunas, dicionarios, origem_s3,
                               so_tecnologia=so_tecnologia)
        try:
            con.execute(_copy_particionado(destino_s3, stem, query))
            linhas = con.execute(
                f"SELECT count(*) FROM read_parquet('{destino_s3}/**/{stem}_*.parquet')"
            ).fetchone()[0]
            total_linhas += linhas
            feitos += 1
            print(f"   [{n}/{len(arquivos)}] ✅ {stem}: {linhas:,} linhas")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(arquivos)}] ❌ {stem}: {str(e)[:200]}")

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
