"""
Repõe a silver de TI da RAIS do Hub, harmonizando os nomes de coluna.

POR QUE ISTO EXISTE
-------------------
O MTE reescreveu o layout da RAIS em 2023: `cbo_ocupacao_2002` virou
`cbo_2002_ocupacao_codigo`, `sexo_trabalhador` virou `sexo_codigo`, `remun`
virou `rem`. O resolver de `mapeamento.py` passou a ENCONTRAR essas colunas,
mas a primeira versão gravava a saída com o nome novo. Resultado: 2007-2022
com `cbo_ocupacao_2002_descricao` e 2023-2025 com
`cbo_2002_ocupacao_codigo_descricao` — a mesma informação sob dois nomes.

Nenhuma consulta atravessa a série assim. O gold somaria só até 2022 e
perderia os três anos mais recentes em silêncio, que é o modo de falha caro:
o número sai, só que errado.

O conteúdo publicado está CERTO — o filtro de TI pegou (47 mil linhas por
arquivo, não 40 milhões) e as 32 traduções estão preenchidas. O que está
errado é o nome. Reconstruir do bronze custaria horas de leitura de 50 GB por
ano para chegar exatamente nos mesmos valores; baixar os 113 arquivos já
publicados e renomear as colunas leva minutos.

Quando a diferença é de FORMA e não de CONTEÚDO, renomear é melhor que
refazer. Mesma lição de `repor_do_hub.py`, um nível acima: lá o nome da
pasta, aqui o nome da coluna.

Uso:
    python -m auditoria.harmonizar_nomes --ano-inicio 2023 --ano-fim 2025
"""
import argparse
import re
import sys

from auditoria.repor_do_hub import listar
from extracao_ftp.config_extracao import BUCKET_SILVER_TI, conectar_duckdb
from silver_rais import mapeamento as mp

REPO = "Gianpedro/rais-tecnologia"


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def renomes(tabela: str, colunas: list[str]) -> dict[str, str]:
    """
    Nome publicado -> nome canônico, pela MESMA regra que o construtor usa.

    Reaproveitar `mp.resolver` é o que garante que o resultado seja idêntico
    ao de reconstruir: se a regra divergisse, teríamos um terceiro vocabulário.
    """
    mapa = {}
    for canonico in (list(mp.MAPA_MANUAL.get(tabela, {})) + list(mp.NUMERICOS)
                     + list(mp.ALIASES)):
        real = mp.resolver(canonico, colunas)
        if real and real != canonico:
            mapa[real] = canonico
            if f"{real}_descricao" in colunas:
                mapa[f"{real}_descricao"] = f"{canonico}_descricao"
    return mapa


def _harmonizar_local(con, fs, args) -> int:
    """
    Renomeia as colunas no próprio bucket, sem passar pelo Hub.

    Necessário porque o bucket deixou de ser um espelho do que está publicado:
    depois do cast numérico e da poda de coluna nula, reler do Hub desfaria as
    duas correções. A verificação de linhas continua valendo — renomear não
    pode perder registro.
    """
    from extracao_ftp.config_extracao import BUCKET_SILVER_TI

    alvos = []
    for tabela in args.tabela:
        for caminho in sorted(fs.glob(f"{BUCKET_SILVER_TI}/{tabela}/**/*.parquet")):
            m = re.search(r"ano_particao=(\d{4})", caminho)
            if m and args.ano_inicio <= int(m.group(1)) <= args.ano_fim:
                alvos.append((tabela, caminho))

    if not alvos:
        print("❌ Nenhum arquivo local na faixa pedida.")
        return 1

    print(f"🔤 {len(alvos)} arquivo(s) locais a conferir\n")
    feitos = intactos = falhas = 0
    for n, (tabela, caminho) in enumerate(alvos, start=1):
        url, temporario = f"s3://{caminho}", f"s3://{caminho}.renomeando"
        nome = caminho.split("/")[-1]
        try:
            colunas = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{url}')").fetchall()]
            mapa = {k: v for k, v in renomes(tabela, colunas).items() if k in colunas}
            if not mapa:
                intactos += 1
                continue

            antes = con.execute(f"SELECT count(*) FROM read_parquet('{url}')").fetchone()[0]
            sel = ", ".join(f'"{c}" AS "{mapa.get(c, c)}"' for c in colunas)
            con.execute(f"COPY (SELECT {sel} FROM read_parquet('{url}')) "
                        f"TO '{temporario}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            depois = con.execute(
                f"SELECT count(*) FROM read_parquet('{temporario}')").fetchone()[0]
            if depois != antes:
                fs.rm(f"{caminho}.renomeando")
                falhas += 1
                print(f"   [{n}/{len(alvos)}] ❌ {nome}: {depois:,} de {antes:,} linhas "
                      f"— original preservado")
                continue

            fs.rm(caminho)
            fs.mv(f"{caminho}.renomeando", caminho)
            feitos += 1
            print(f"   [{n}/{len(alvos)}] ✅ {nome}  ({len(mapa)} coluna(s) renomeada(s))")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(alvos)}] ❌ {nome}: {str(e)[:130]}")

    print(f"\n🏁 {feitos} harmonizado(s), {intactos} já canônico(s), {falhas} falha(s)")
    return 0 if not falhas else 2


def main() -> int:
    p = argparse.ArgumentParser(description="Repõe a RAIS do Hub harmonizando nomes de coluna.")
    p.add_argument("--tabela", nargs="+", default=["rais_vinc", "rais_estab"])
    p.add_argument("--ano-inicio", type=int, default=2023)
    p.add_argument("--ano-fim", type=int, default=9999)
    p.add_argument("--repo", default=REPO)
    p.add_argument("--origem", choices=("hub", "local"), default="hub",
                   help="'hub' repõe do que está publicado; 'local' renomeia "
                        "no próprio bucket. Use 'local' quando o bucket já "
                        "recebeu correções que o Hub ainda não tem — reler do "
                        "Hub as desfaria.")
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=2")
    fs = _fs()

    if args.origem == "local":
        return _harmonizar_local(con, fs, args)

    alvos = []
    for caminho in listar(args.repo):
        tabela = caminho.split("/")[0]
        m = re.search(r"ano_particao=(\d{4})", caminho)
        if tabela in args.tabela and m and args.ano_inicio <= int(m.group(1)) <= args.ano_fim:
            alvos.append((tabela, caminho))

    if not alvos:
        print(f"❌ Nada publicado em {args.repo} para {args.tabela} na faixa pedida.")
        return 1

    print(f"🔤 {len(alvos)} arquivo(s) a harmonizar de {args.repo}\n")
    feitos = pulados = falhas = 0
    cache: dict[str, dict[str, str]] = {}

    for n, (tabela, caminho) in enumerate(sorted(alvos), start=1):
        destino = f"{BUCKET_SILVER_TI}/{caminho}"
        if fs.exists(destino):
            pulados += 1
            continue
        origem = f"hf://datasets/{args.repo}/{caminho}"
        try:
            colunas = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{origem}')").fetchall()]
            # O mapa só depende do schema, e o schema é o mesmo dentro do ano.
            chave = f"{tabela}/{re.search(r'ano_particao=(\d{4})', caminho).group(1)}"
            mapa = cache.setdefault(chave, renomes(tabela, colunas))
            sel = ", ".join(f'"{c}" AS "{mapa.get(c, c)}"' for c in colunas)
            con.execute(
                f"COPY (SELECT {sel} FROM read_parquet('{origem}')) "
                f"TO 's3://{destino}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            feitos += 1
            print(f"   [{n}/{len(alvos)}] ✅ {caminho.split('/')[-1]}  "
                  f"({len(mapa)} coluna(s) renomeada(s))")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(alvos)}] ❌ {caminho}: {str(e)[:130]}")

    print(f"\n🏁 {feitos} harmonizado(s), {pulados} já existente(s), {falhas} falha(s)")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
