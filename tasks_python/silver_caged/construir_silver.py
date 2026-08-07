"""
Constrói a camada silver do CAGED a partir da bronze + dicionários.

Para cada tabela (caged_mov, caged_for, caged_exc, caged_old, caged_ajustes):

  1. Filtra o recorte de TECNOLOGIA (ver gold_caged/escopo_tecnologia.py).
  2. Para cada coluna codificada com dicionário disponível, faz LEFT JOIN e
     acrescenta uma coluna "<coluna>_descricao" com o texto legível — o código
     original é mantido, nada é substituído.
  3. Tipa os campos numéricos (vírgula decimal -> ponto, cast) e monta uma
     coluna de data a partir da competência AAAAMM.
  4. Grava um parquet por arquivo do bronze em s3://silver/<tabela>, ZSTD-3.

RECORTE DE TECNOLOGIA (padrão)
------------------------------
O estudo é sobre o mercado de trabalho em tecnologia, e TI é ~1% dos
registros: gravar o mercado inteiro custaria ~100x mais espaço em disco para
dados que nunca seriam consultados — decisivo na RAIS, que sozinha passaria
de 60 GB.

O que NÃO se perde: o bronze continua completo (é a fonte da verdade, dá para
re-derivar com outra definição de TI a qualquer momento) e a linha de base do
mercado geral vem dos agregados da gold, calculados direto do bronze. Sem
essa linha de base, "TI cresceu 8%" não significaria nada.

Use --mercado-completo para gravar tudo.

Uso (a partir de tasks_python, com o .venv ativo):

    python -m silver_caged.construir_silver --listar
    python -m silver_caged.construir_silver --tabela caged_mov caged_for caged_exc
    python -m silver_caged.construir_silver --tabela caged_old --ano-inicio 2015
"""
import argparse
import re
import sys
import time

from extracao_ftp.config_extracao import (
    BUCKET_BRONZE,
    BUCKET_SILVER,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)
from silver_caged import mapeamento as mp
from gold_caged import escopo_tecnologia as esc
from silver_caged.dicionarios import chave_normalizada, criar_view, existe

# Colunas que nunca são candidatas a tradução/tipagem (linhagem e partição)
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


def _colunas_arquivo(con, caminho_s3: str) -> list[str]:
    """Schema de UM arquivo parquet (o schema varia entre eras do CAGED antigo)."""
    try:
        return [
            r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{caminho_s3}') LIMIT 0"
            ).fetchall()
        ]
    except Exception as e:
        print(f"   ⚠️  Não consegui ler o schema de {caminho_s3}: {str(e)[:200]}")
        return []


def _colunas_bronze(con, tabela: str) -> list[str]:
    # Sem hive_partitioning: nem toda tabela tem o mesmo esquema de pastas
    # (caged_ajustes mistura arquivo anual "ano=2002/arq.parquet" com mensal
    # "ano=2010/mes=1/arq.parquet" — hive_partitioning=1 exige partição
    # uniforme e quebra nessa mistura). ano_particao/mes_particao já vêm como
    # colunas de verdade em cada linha, gravadas na ingestão bronze; não
    # precisa reconstruir a partição a partir do caminho.
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


def _mapa_traducao(con, fs, tabela: str, colunas: list[str]) -> dict[str, dict]:
    """
    Devolve {coluna_bronze: spec} só para colunas com dicionário de fato
    disponível no MinIO. `spec` é um dict com pelo menos "namespace", "aba",
    "estilo" — e "coluna"/"campo" quando o estilo exigir (ver dicionarios.py).
    """
    geracao = mp.geracao(tabela)
    mapa = {}

    if geracao == "novo":
        for col in colunas:
            if col in COLUNAS_TECNICAS or col in mp.ABAS_NAO_TRADUZIVEIS:
                continue
            if existe(fs, mp.BUCKET_SILVER_NAMESPACE_NOVO, col):
                mapa[col] = {
                    "namespace": mp.BUCKET_SILVER_NAMESPACE_NOVO,
                    "aba": col,
                    "estilo": "2col",
                }
    else:
        for col, spec in mp.MAPA_CAGED_ANTIGO.items():
            if col not in colunas:
                continue
            if not existe(fs, mp.BUCKET_DICT_NAMESPACE_ANTIGO, spec["aba"]):
                continue
            mapa[col] = {"namespace": mp.BUCKET_DICT_NAMESPACE_ANTIGO, **spec}

    return mapa


def _select_silver(con, fs, tabela: str, colunas: list[str], caminho_bronze: str,
                   silencioso: bool = False, so_tecnologia: bool = False) -> str:
    geracao = mp.geracao(tabela)
    numericos = mp.NUMERICOS_NOVO_CAGED if geracao == "novo" else mp.NUMERICOS_CAGED_ANTIGO
    datas_aaaamm = mp.DATAS_AAAAMM_NOVO_CAGED if geracao == "novo" else mp.DATAS_AAAAMM_CAGED_ANTIGO
    numericos = {k: v for k, v in numericos.items() if k in colunas}
    datas_aaaamm = [c for c in datas_aaaamm if c in colunas]

    mapa = _mapa_traducao(con, fs, tabela, colunas)
    if not silencioso:
        if mapa:
            print(f"   📖 {len(mapa)} coluna(s) com tradução: {', '.join(sorted(mapa))}")
        else:
            print("   ⚠️  Nenhuma coluna com dicionário disponível — silver sairá só tipada.")

    joins = []
    expressoes = []

    for col in colunas:
        if col in COLUNAS_TECNICAS:
            continue

        if col in numericos:
            tipo = numericos[col]
            expressoes.append(
                f'try_cast(replace(trim(b."{col}"), \',\', \'.\') AS {tipo}) AS "{col}"'
            )
        else:
            expressoes.append(f'b."{col}" AS "{col}"')

        if col in mapa:
            spec = dict(mapa[col])
            namespace, aba, estilo = spec.pop("namespace"), spec.pop("aba"), spec.pop("estilo")
            nome_view = f"dic_{tabela}_{col}"
            if criar_view(con, namespace, aba, estilo, nome_view, **spec):
                # O CAGED antigo grava código curto com zero à esquerda
                # ("02", "07"), mas o dicionário do layout traz o código sem
                # padding ("2", "7"). A chave canônica (ver dicionarios.py)
                # resolve isso dos dois lados e mantém a junção como
                # igualdade simples, que o DuckDB executa via hash join.
                chave_fato = chave_normalizada(f'b."{col}"')
                joins.append(
                    f'LEFT JOIN {nome_view} AS "{nome_view}" '
                    f'ON {chave_fato} = "{nome_view}".codigo_norm'
                )
                expressoes.append(f'"{nome_view}".descricao AS "{col}_descricao"')

        if col in datas_aaaamm:
            expressoes.append(
                f'try_strptime(trim(b."{col}"), \'%Y%m\')::DATE AS "{col}_data"'
            )

    for col in COLUNAS_TECNICAS & set(colunas):
        expressoes.append(f'b."{col}" AS "{col}"')

    select = ",\n            ".join(expressoes)
    join_sql = "\n            ".join(joins)

    # Recorte de tecnologia aplicado JÁ NA LEITURA do bronze: o predicado
    # entra antes dos LEFT JOINs de dicionário, então o DuckDB descarta ~99%
    # das linhas antes de traduzir qualquer coisa. Filtrar depois traduziria
    # 725 milhões de linhas para jogar fora quase todas.
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


def construir(con, fs, tabela: str, ano_inicio: int, ano_fim: int, forcar: bool,
              so_tecnologia: bool = False) -> bool:
    """
    Constrói a silver de uma tabela, UM ARQUIVO BRONZE POR VEZ.

    Processar a tabela inteira num único COPY estourava a memória (21 hash
    tables de dicionário + dezenas de milhões de linhas > limite do DuckDB
    nesta máquina). Arquivo a arquivo o pico de memória fica no tamanho de um
    mês de dados, a carga vira retomável (pula o que já existe) e cada
    arquivo pode ter seu próprio schema — o CAGED antigo muda de colunas
    entre eras, e um COPY único sobre o glob inteiro exigiria schema uniforme.
    """
    print(f"\n{'=' * 70}\n  🔨 SILVER: {tabela}\n{'=' * 70}")
    inicio = time.time()

    arquivos = sorted(fs.glob(f"{BUCKET_BRONZE}/{tabela}/**/*.parquet"))
    if not arquivos:
        print("   ⏭️  Sem dados em bronze para esta tabela, pulando.")
        return False

    if ano_inicio or ano_fim:
        arquivos = [
            a for a in arquivos
            if (ano := _ano_do_caminho(a)) is None or ano_inicio <= ano <= ano_fim
        ]

    total_linhas = 0
    feitos = pulados = falhas = 0
    primeiro = True

    for n, origem in enumerate(arquivos, start=1):
        destino_rel = origem.replace(f"{BUCKET_BRONZE}/", f"{BUCKET_SILVER}/", 1)

        if not forcar and fs.exists(destino_rel):
            pulados += 1
            continue

        origem_s3 = f"s3://{origem}"
        destino_s3 = f"s3://{destino_rel}"

        colunas = _colunas_arquivo(con, origem_s3)
        if not colunas:
            falhas += 1
            continue

        # O mapa de tradução é o mesmo para todos os arquivos da tabela;
        # imprime só na primeira vez para não poluir o log com 156 repetições.
        query = _select_silver(con, fs, tabela, colunas, origem_s3,
                               silencioso=not primeiro, so_tecnologia=so_tecnologia)
        primeiro = False

        try:
            con.execute(f"""
                COPY ({query}) TO '{destino_s3}' (
                    FORMAT PARQUET,
                    COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                    COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL}
                );
            """)
            linhas = con.execute(
                f"SELECT count(*) FROM read_parquet('{destino_s3}')"
            ).fetchone()[0]
            total_linhas += linhas
            feitos += 1
            print(f"   [{n}/{len(arquivos)}] ✅ {origem.split('/')[-1]}: {linhas:,} linhas")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(arquivos)}] ❌ {origem.split('/')[-1]}: {str(e)[:200]}")

    decorrido = time.time() - inicio
    print(f"   📊 {feitos} gravado(s), {pulados} já existente(s), {falhas} falha(s) "
          f"| {total_linhas:,} linhas novas em {decorrido / 60:.1f} min")
    return falhas == 0


def _argumentos():
    p = argparse.ArgumentParser(description="Constrói a camada silver do CAGED.")
    p.add_argument("--tabela", nargs="+", choices=mp.TODAS_TABELAS, default=list(mp.TODAS_TABELAS))
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    p.add_argument("--listar", action="store_true", help="Só mostra o mapeamento de tradução e sai")
    p.add_argument("--forcar", action="store_true",
                   help="Reprocessa arquivos que já existem na silver (padrão: pula)")
    p.add_argument("--mercado-completo", action="store_true",
                   help="Grava TODO o mercado em vez de só tecnologia (padrão: só TI). "
                        "O mercado completo ocupa ~100x mais espaço; a linha de base "
                        "para comparação vem dos agregados da gold, calculados do bronze.")
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
            mapa = _mapa_traducao(con, fs, tabela, colunas)
            print(f"\n{tabela} ({len(colunas)} colunas, {len(mapa)} traduzíveis):")
            for col in colunas:
                if col in mapa:
                    spec = mapa[col]
                    extra = spec.get("campo") or spec.get("coluna") or ""
                    sufixo = f" [{extra}]" if extra else ""
                    print(f"   ✅ {col}  ->  {spec['namespace']}/{spec['aba']} ({spec['estilo']}){sufixo}")
        return 0

    sucesso = 0
    for tabela in args.tabela:
        if construir(con, fs, tabela, args.ano_inicio, args.ano_fim, args.forcar,
                     so_tecnologia=not args.mercado_completo):
            sucesso += 1

    print(f"\n🏁 {sucesso}/{len(args.tabela)} tabela(s) construída(s) na silver.")
    return 0 if sucesso == len(args.tabela) else 2


if __name__ == "__main__":
    sys.exit(main())
