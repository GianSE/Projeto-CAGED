"""
Converte para DOUBLE as colunas numéricas que ficaram VARCHAR na silver de TI.

POR QUE ISTO EXISTE
-------------------
O layout de 2023 do MTE renomeou `vl_remun_media_sm` para `vl_rem_media_sm`.
A tipagem numérica do construtor casava nome exato, não pegou os nomes novos,
e a remuneração foi gravada como texto. A série ficou DOUBLE até 2023 e
VARCHAR em 2024-2025 — e `AVG(vl_remun_media_nom)` sobre a série inteira
falha, ou pior, funciona sobre um subconjunto silencioso.

A checagem por ANO esconde o problema: `DESCRIBE` sobre `ano_particao=2023`
devolvia DOUBLE, mas sete arquivos daquele ano (sp_parte06/07, sul_parte00-04)
estavam VARCHAR. Só a leitura ARQUIVO A ARQUIVO expõe a mistura. Por isso este
módulo varre por arquivo, não por partição.

Por que DOUBLE e não DECIMAL: os 17 anos já construídos estão DOUBLE, e gravar
os novos como DECIMAL recriaria a mesma deriva de tipo que se quer corrigir. O
erro de representação é ~1e-13 relativo, dez ordens de grandeza abaixo do erro
do próprio dado (a RAIS é declarada pelo empregador). Onde a precisão importa
de fato é na agregação do gold, e lá se converte para DECIMAL(18,2) na hora de
somar — soma de DOUBLE em paralelo não é associativa e não reproduz.

SOBRE O TRY_CAST
----------------
`TRY_CAST` devolve NULL calado quando o texto não é número. Esse é exatamente
o modo de falha que já custou caro neste pipeline quatro vezes: sucesso
aparente, dado a menos. Por isso cada arquivo é conferido — não-nulos antes e
depois — e só substitui o original se a contagem bater. Se cair, o arquivo é
preservado e a divergência é reportada.

Uso:
    python -m auditoria.tipar_numericos
    python -m auditoria.tipar_numericos --tabela rais_vinc --so-listar
"""
import argparse
import sys

from extracao_ftp.config_extracao import BUCKET_SILVER_TI, conectar_duckdb
from silver_rais import mapeamento as mp


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def _varchar_numericas(con, url: str) -> list[str]:
    """Colunas que deveriam ser numéricas e vieram como texto."""
    tipos = {r[0]: r[1] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{url}')").fetchall()}
    return [c for c in mp.NUMERICOS if tipos.get(c) == "VARCHAR"]


def main() -> int:
    p = argparse.ArgumentParser(description="Tipa como DOUBLE as numéricas que ficaram VARCHAR.")
    p.add_argument("--tabela", nargs="+", default=["rais_vinc", "rais_estab"])
    p.add_argument("--bucket", default=BUCKET_SILVER_TI)
    p.add_argument("--so-listar", action="store_true",
                   help="Só mostra os arquivos afetados, não reescreve nada.")
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=2")
    fs = _fs()

    print(f"🔎 Varrendo {args.bucket} arquivo a arquivo\n")
    pendentes = []
    for tabela in args.tabela:
        caminhos = fs.glob(f"{args.bucket}/{tabela}/**/*.parquet")
        for caminho in sorted(caminhos):
            colunas = _varchar_numericas(con, f"s3://{caminho}")
            if colunas:
                pendentes.append((caminho, colunas))
        print(f"   {tabela}: {len(caminhos)} arquivo(s), "
              f"{sum(1 for c, _ in pendentes if f'/{tabela}/' in c)} com numérica em texto")

    if not pendentes:
        print("\n🏁 Nada a converter — toda numérica já está tipada.")
        return 0

    print(f"\n📋 {len(pendentes)} arquivo(s) a converter")
    if args.so_listar:
        for caminho, colunas in pendentes[:20]:
            print(f"   {caminho.split('/')[-1]}: {len(colunas)} coluna(s)")
        return 0

    convertidos = falhas = 0
    for n, (caminho, colunas) in enumerate(pendentes, start=1):
        url, temporario = f"s3://{caminho}", f"s3://{caminho}.convertendo"
        nome = caminho.split("/")[-1]
        try:
            todas = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{url}')").fetchall()]
            sel = ", ".join(
                f'TRY_CAST("{c}" AS DOUBLE) AS "{c}"' if c in colunas else f'"{c}"'
                for c in todas)

            # A conferência que impede o TRY_CAST de comer dado em silêncio.
            contagem = ", ".join(f'count("{c}")' for c in colunas)
            antes = con.execute(f"SELECT {contagem} FROM read_parquet('{url}')").fetchone()

            con.execute(f"COPY (SELECT {sel} FROM read_parquet('{url}')) "
                        f"TO '{temporario}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            depois = con.execute(f"SELECT {contagem} FROM read_parquet('{temporario}')").fetchone()

            perdidas = [(c, a, d) for c, a, d in zip(colunas, antes, depois) if a != d]
            if perdidas:
                fs.rm(f"{caminho}.convertendo")
                falhas += 1
                detalhe = ", ".join(f"{c}: {a}->{d}" for c, a, d in perdidas[:3])
                print(f"   [{n}/{len(pendentes)}] ❌ {nome}: cast perderia valor ({detalhe}) "
                      f"— original preservado")
                continue

            fs.rm(caminho)
            fs.mv(f"{caminho}.convertendo", caminho)
            convertidos += 1
            print(f"   [{n}/{len(pendentes)}] ✅ {nome}  "
                  f"({len(colunas)} coluna(s), {antes[0]:,} valor(es) conferido(s))")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(pendentes)}] ❌ {nome}: {str(e)[:120]}")

    print(f"\n🏁 {convertidos} convertido(s), {falhas} falha(s)")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
