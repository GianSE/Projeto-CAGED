"""
Remove colunas inúteis de uma tabela já construída, conferindo antes e depois.

POR QUE ISTO EXISTE
-------------------
`tipo_estab_1_descricao` saiu 100% nula nos 19 anos das duas tabelas da RAIS —
40 dos 42 achados da auditoria eram essa única coluna. A causa: o mapeamento
apontava `tipo_estab_1` para o dicionário indexado por CÓDIGO ('01'/'03'), mas
a coluna já vem DESCRITA da origem ('CNPJ'/'CEI'). O join nunca casava.

A informação não se perde ao remover: `tipo_estab_descricao` já entrega
exatamente 'CNPJ'/'CEI' a partir do código, e a própria `tipo_estab_1` continua
no arquivo com o texto original. O que se remove é só a coluna vazia.

`mapeamento.py` foi corrigido para não gerar a coluna de novo; este módulo
limpa o que já foi gravado, sem reconstruir do bronze.

A poda só acontece depois de conferir que a contagem de linhas bate. Remover
coluna é irreversível no arquivo, e um arquivo truncado no meio da regravação
seria pior que a coluna nula.

Uso:
    python -m auditoria.podar_colunas --coluna tipo_estab_1_descricao \
        --tabela rais_vinc rais_estab
"""
import argparse
import sys

from extracao_ftp.config_extracao import BUCKET_SILVER_TI, conectar_duckdb


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def main() -> int:
    p = argparse.ArgumentParser(description="Remove colunas de uma tabela já construída.")
    p.add_argument("--coluna", nargs="+", required=True)
    p.add_argument("--tabela", nargs="+", required=True)
    p.add_argument("--bucket", default=BUCKET_SILVER_TI)
    p.add_argument("--so-listar", action="store_true")
    p.add_argument("--forcar", action="store_true",
                   help="Poda mesmo se a coluna tiver algum valor não nulo. "
                        "Sem isto, coluna com dado é preservada — a segurança "
                        "que impede este módulo de virar uma faca solta.")
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=2")
    fs = _fs()

    alvos = []
    for tabela in args.tabela:
        for caminho in sorted(fs.glob(f"{args.bucket}/{tabela}/**/*.parquet")):
            presentes = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('s3://{caminho}')").fetchall()]
            tem = [c for c in args.coluna if c in presentes]
            if tem:
                alvos.append((caminho, tem))

    if not alvos:
        print("🏁 Nenhum arquivo contém as colunas pedidas.")
        return 0

    print(f"✂️  {len(alvos)} arquivo(s) contêm {', '.join(args.coluna)}\n")
    if args.so_listar:
        for c, cols in alvos[:20]:
            print(f"   {c.split('/')[-1]}: {cols}")
        return 0

    podados = preservados = falhas = 0
    for n, (caminho, cols) in enumerate(alvos, start=1):
        url, temporario = f"s3://{caminho}", f"s3://{caminho}.podando"
        nome = caminho.split("/")[-1]
        try:
            contagem = ", ".join(f'count("{c}")' for c in cols)
            total_e_nn = con.execute(
                f"SELECT count(*), {contagem} FROM read_parquet('{url}')").fetchone()
            total, nao_nulos = total_e_nn[0], total_e_nn[1:]

            if any(nao_nulos) and not args.forcar:
                preservados += 1
                detalhe = ", ".join(f"{c}={v:,}" for c, v in zip(cols, nao_nulos) if v)
                print(f"   [{n}/{len(alvos)}] ⏭️  {nome}: tem dado ({detalhe}), preservado")
                continue

            con.execute(f"COPY (SELECT * EXCLUDE ({', '.join(f'\"{c}\"' for c in cols)}) "
                        f"FROM read_parquet('{url}')) "
                        f"TO '{temporario}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            depois = con.execute(
                f"SELECT count(*) FROM read_parquet('{temporario}')").fetchone()[0]
            if depois != total:
                fs.rm(f"{caminho}.podando")
                falhas += 1
                print(f"   [{n}/{len(alvos)}] ❌ {nome}: {depois:,} de {total:,} linhas "
                      f"— original preservado")
                continue

            fs.rm(caminho)
            fs.mv(f"{caminho}.podando", caminho)
            podados += 1
            print(f"   [{n}/{len(alvos)}] ✅ {nome}  ({total:,} linhas, {len(cols)} coluna(s) removida(s))")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(alvos)}] ❌ {nome}: {str(e)[:130]}")

    print(f"\n🏁 {podados} podado(s), {preservados} preservado(s) por ter dado, {falhas} falha(s)")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
