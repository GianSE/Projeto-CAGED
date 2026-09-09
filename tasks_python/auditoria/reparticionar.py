"""
Uniformiza a profundidade das partições de uma tabela da silver.

POR QUE ISTO EXISTE
-------------------
`caged_ajustes` não podia ser lida como tabela única:

    Binder Error: Hive partition mismatch between file
      ano_particao=2002/caged_ajustes_2002.parquet         (sem nível de mês)
      ano_particao=2010/mes_particao=1/caged_ajustes_...    (com nível de mês)

Até 2009 o MTE publicava os ajustes num arquivo anual; de 2010 em diante,
mensal. O construtor espelhou isso, e a tabela ficou com dois níveis de
profundidade — o DuckDB recusa o glob inteiro. Cada ano lia bem sozinho, que é
por que a auditoria (que varre ano a ano) nunca viu: o defeito só aparece na
consulta que atravessa a série, que é justamente a do dashboard e a do gold.

O mês existe nos anos antigos, mas não onde parecia: a coluna `mes_particao`
está inteiramente NULA neles. Quem carrega o mês é `competencia_declarada`
('200501'). E é ela mesma que define a partição nos anos mensais — conferido
em 2015/mes=3, onde 2.616 de 2.616 linhas têm `competencia_declarada=201503` e
NENHUMA tem `competencia_movimentacao=201503`. A distinção importa: ajuste é
declaração atrasada, o movimento é de um ano e a declaração de outro.

Reparticionar não reprocessa nada — relê o parquet já traduzido e regrava sob
o caminho certo. Mesma lição de `repor_do_hub` e `harmonizar_nomes`: quando a
diferença é de FORMA, mover é melhor que refazer.

Uso:
    python -m auditoria.reparticionar --tabela caged_ajustes
"""
import argparse
import re
import sys

from extracao_ftp.config_extracao import BUCKET_SILVER_TI, conectar_duckdb

# De onde tirar o mês quando a coluna de partição vem nula. Por tabela, porque
# a resposta depende do significado do registro, não do formato do arquivo.
FONTE_DO_MES = {"caged_ajustes": "competencia_declarada"}


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def main() -> int:
    p = argparse.ArgumentParser(description="Uniformiza a profundidade das partições.")
    p.add_argument("--tabela", nargs="+", default=["caged_ajustes"])
    p.add_argument("--bucket", default=BUCKET_SILVER_TI)
    p.add_argument("--so-listar", action="store_true")
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=2")
    fs = _fs()

    rasos = []
    for tabela in args.tabela:
        for caminho in sorted(fs.glob(f"{args.bucket}/{tabela}/**/*.parquet")):
            if "mes_particao=" not in caminho:
                rasos.append((tabela, caminho))

    if not rasos:
        print("🏁 Nenhuma partição rasa — profundidade já uniforme.")
        return 0

    print(f"📐 {len(rasos)} arquivo(s) num nível acima do resto da tabela\n")
    if args.so_listar:
        for _, c in rasos:
            print(f"   {c}")
        return 0

    movidos = vazios = falhas = 0
    for n, (tabela, caminho) in enumerate(rasos, start=1):
        nome = caminho.split("/")[-1]
        stem = nome.removesuffix(".parquet")
        ano = int(re.search(r"ano_particao=(\d{4})", caminho).group(1))
        url = f"s3://{caminho}"
        fonte = FONTE_DO_MES.get(tabela)
        if not fonte:
            print(f"   [{n}/{len(rasos)}] ⏭️  {nome}: sem fonte de mês definida para {tabela}")
            continue

        try:
            total = con.execute(f"SELECT count(*) FROM read_parquet('{url}')").fetchone()[0]
            if total == 0:
                # Arquivo sem linha nenhuma. Não há o que particionar, e mantê-lo
                # no nível raso continuaria quebrando a leitura da tabela inteira.
                fs.rm(caminho)
                vazios += 1
                print(f"   [{n}/{len(rasos)}] 🗑️  {nome}: 0 linhas, removido "
                      f"(não havia dado, só o caminho que quebrava o glob)")
                continue

            # Recusa antes de gravar se o mês não der para derivar, ou se alguma
            # linha cairia em OUTRO ano — isso contaminaria uma partição alheia.
            ruins, fora = con.execute(f"""
                SELECT count(*) FILTER (WHERE "{fonte}" IS NULL
                                          OR length("{fonte}") < 6
                                          OR TRY_CAST(substr("{fonte}", 5, 2) AS BIGINT) IS NULL),
                       count(*) FILTER (WHERE substr("{fonte}", 1, 4) <> '{ano}')
                FROM read_parquet('{url}')""").fetchone()
            if ruins or fora:
                falhas += 1
                print(f"   [{n}/{len(rasos)}] ❌ {nome}: {ruins:,} sem mês derivável, "
                      f"{fora:,} de outro ano — original preservado")
                continue

            con.execute(f"""
                COPY (SELECT * EXCLUDE (ano_particao, mes_particao),
                             {ano} AS ano_particao,
                             CAST(substr("{fonte}", 5, 2) AS BIGINT) AS mes_particao
                      FROM read_parquet('{url}'))
                TO 's3://{args.bucket}/{tabela}'
                (FORMAT PARQUET, COMPRESSION ZSTD,
                 PARTITION_BY (ano_particao, mes_particao),
                 FILENAME_PATTERN '{stem}_{{i}}', OVERWRITE_OR_IGNORE)""")

            gravado = con.execute(
                f"SELECT count(*) FROM read_parquet("
                f"'s3://{args.bucket}/{tabela}/ano_particao={ano}/mes_particao=*/*.parquet')"
            ).fetchone()[0]
            if gravado != total:
                falhas += 1
                print(f"   [{n}/{len(rasos)}] ❌ {nome}: gravou {gravado:,} de {total:,} "
                      f"— original preservado")
                continue

            fs.rm(caminho)
            movidos += 1
            print(f"   [{n}/{len(rasos)}] ✅ {nome}: {total:,} linhas em "
                  f"{ano}, agora sob mes_particao=")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(rasos)}] ❌ {nome}: {str(e)[:130]}")

    print(f"\n🏁 {movidos} reparticionado(s), {vazios} vazio(s) removido(s), {falhas} falha(s)")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
