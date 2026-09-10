"""
Preenche uma descrição que ficou nula porque o código encurtou na origem.

POR QUE ISTO EXISTE
-------------------
No layout de 2023 o MTE passou a publicar o CNAE 95 com 4 dígitos em vez de 5:
até 2022 vinha '72109', '72214'; de 2023 em diante, '7210', '7221'. O
dicionário é indexado pelos códigos de 5 dígitos, então o join não casa nada e
`cnae_95_classe_descricao` sai 100% nula nos três anos.

Isto NÃO é limitação da fonte — os códigos são variados e significam coisas
distintas, ao contrário do '999997' dos bairros. Foi a auditoria aprendendo a
separar "origem só tem marcador" de "join falhou" que revelou o caso: antes ele
estava enterrado junto com 40 achados de uma coluna só.

O conserto é seguro porque o código de 4 dígitos é PREFIXO do de 5, e o prefixo
é inequívoco: nenhum prefixo de 4 dígitos do dicionário corresponde a duas
descrições diferentes (conferido: 0 ambíguos em 1.228 entradas). Traduzir por
prefixo devolve exatamente a descrição que o código completo devolveria.

Uso:
    python -m auditoria.traduzir_por_prefixo --tabela rais_estab rais_vinc \
        --coluna cnae_95_classe --digitos 4
"""
import argparse
import sys
from pathlib import Path

from extracao_ftp.config_extracao import BUCKET_SILVER_TI, conectar_duckdb
from silver_caged import dimensoes

RAIZ = Path(__file__).resolve().parents[2]

# O dicionário consolidado vive no LAKE, como todo o resto — ver
# `silver_caged.dimensoes.caminho_canonico`. Ele já apontou para
# `publicacao/rais/dicionarios.parquet`, e isso era um erro de direção: um
# módulo de manutenção passava a depender da pasta de PUBLICAÇÃO, que é
# espelho descartável. Limpar o espelho apagava a fonte deste módulo.
DICIONARIO = dimensoes.caminho_canonico("rais")


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def main() -> int:
    p = argparse.ArgumentParser(description="Traduz por prefixo do código.")
    p.add_argument("--tabela", nargs="+", required=True)
    p.add_argument("--coluna", required=True)
    p.add_argument("--digitos", type=int, default=4)
    p.add_argument("--dicionario", default=str(DICIONARIO))
    p.add_argument("--bucket", default=BUCKET_SILVER_TI)
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=2")
    fs = _fs()
    desc = f"{args.coluna}_descricao"

    # O prefixo precisa ser único ANTES de qualquer gravação: traduzir com
    # prefixo ambíguo trocaria uma coluna vazia por uma coluna errada, que é
    # muito pior — vazio se enxerga, errado não.
    caminho_dic = args.dicionario.replace(chr(92), "/")
    ambiguos = con.execute(f"""
        SELECT count(*) FROM (
            SELECT substr(codigo, 1, {args.digitos})
            FROM read_parquet('{caminho_dic}')
            WHERE coluna = '{args.coluna}'
            GROUP BY 1 HAVING count(DISTINCT descricao) > 1)""").fetchone()[0]
    if ambiguos:
        print(f"❌ {ambiguos} prefixo(s) de {args.digitos} dígitos com mais de uma "
              f"descrição — traduzir assim inventaria dado. Abortado.")
        return 1

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dic AS
        SELECT DISTINCT substr(codigo, 1, {args.digitos}) AS chave, descricao
        FROM read_parquet('{caminho_dic}') WHERE coluna = '{args.coluna}'""")
    n_dic = con.execute("SELECT count(*) FROM dic").fetchone()[0]
    print(f"📖 {n_dic} chave(s) de {args.digitos} dígitos, nenhuma ambígua\n")

    alvos = []
    for tabela in args.tabela:
        for caminho in sorted(fs.glob(f"{args.bucket}/{tabela}/**/*.parquet")):
            cols = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('s3://{caminho}')").fetchall()]
            if args.coluna not in cols or desc not in cols:
                continue
            vazia, total = con.execute(
                f'SELECT count("{desc}"), count(*) FROM read_parquet(\'s3://{caminho}\')'
            ).fetchone()
            if vazia == 0 and total > 0:
                alvos.append((caminho, cols, total))

    if not alvos:
        print(f"🏁 Nenhum arquivo com {desc} vazia.")
        return 0

    print(f"🔤 {len(alvos)} arquivo(s) com {desc} 100% nula\n")
    feitos = falhas = 0
    for n, (caminho, cols, total) in enumerate(alvos, start=1):
        url, temporario = f"s3://{caminho}", f"s3://{caminho}.traduzindo"
        nome = caminho.split("/")[-1]
        try:
            sel = ", ".join(
                f'd.descricao AS "{c}"' if c == desc else f'b."{c}"' for c in cols)
            con.execute(f"""
                COPY (SELECT {sel} FROM read_parquet('{url}') b
                      LEFT JOIN dic d
                        ON substr(b."{args.coluna}", 1, {args.digitos}) = d.chave)
                TO '{temporario}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
            linhas, preenchidas = con.execute(
                f'SELECT count(*), count("{desc}") FROM read_parquet(\'{temporario}\')'
            ).fetchone()

            if linhas != total or preenchidas == 0:
                fs.rm(f"{caminho}.traduzindo")
                falhas += 1
                print(f"   [{n}/{len(alvos)}] ❌ {nome}: {linhas:,} linhas "
                      f"(esperado {total:,}), {preenchidas:,} traduzidas "
                      f"— original preservado")
                continue

            fs.rm(caminho)
            fs.mv(f"{caminho}.traduzindo", caminho)
            feitos += 1
            pct = 100 * preenchidas / linhas
            print(f"   [{n}/{len(alvos)}] ✅ {nome}  "
                  f"({preenchidas:,} de {linhas:,} traduzidas, {pct:.1f}%)")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(alvos)}] ❌ {nome}: {str(e)[:130]}")

    print(f"\n🏁 {feitos} traduzido(s), {falhas} falha(s)")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
