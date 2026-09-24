"""
Ajusta o recorte de tecnologia na silver JÁ CONSTRUÍDA, sem reprocessar a série.

A MUDANÇA
---------
Com o orientador, o recorte foi revisado em duas direções (ver
`gold_caged.escopo_tecnologia`):

    ENTRA  a família 2112 (estatísticos) — onde analistas e cientistas de
           dados são registrados, 72% deles fora de empresa de TI
    SAI    2031-10/15/20/25 (pesquisadores de ciências naturais) e 3171-15
           (programador de máquina CNC), que vinham de carona na seleção por
           família e não são tecnologia

Medido antes de mexer em qualquer arquivo:

                    entram (2112)   saem (excluídos)
    caged_mov             11.577           21.299
    caged_old              6.421           32.995
    caged_ajustes            309            1.335
    caged_for                160              419
    caged_exc                 37               36
    rais_vinc        (do bronze)          147.234

Sai bem mais do que entra: o programador CNC pesava muito mais do que a
proporção de 1% sugeria. O recorte fica menor e mais limpo.

POR QUE AJUSTAR E NÃO RECONSTRUIR
---------------------------------
Reconstruir a silver de TI do zero custaria a série inteira de novo — só o
caged_old levou 12 horas na primeira vez. E a mudança é pequena: algumas dezenas
de milhares de linhas entrando e saindo, em 39 milhões.

    SAIR      reescreve só os arquivos que contêm linhas excluídas, tirando-as.
    CAGED     as linhas novas vêm da silver de MERCADO COMPLETO, que já está
              traduzida e tem a mesma estrutura: é só filtrar.
    RAIS      a RAIS completa não está na silver, então as linhas novas saem do
              bronze pelo construtor normal — com o filtro trocado pelo do
              complemento e o destino trocado por uma área de preparo, para não
              sobrescrever a silver existente.

AS LINHAS NOVAS VIRAM "PARTE 90"
--------------------------------
O complemento de cada arquivo de origem é gravado ao lado dele como
`{origem}_parte90_0.parquet`. "Parte" é a convenção que o projeto já usa para
pedaços da mesma origem, e é o que a auditoria reconhece: o sufixo é retirado
ao identificar a origem, então a completude continua contando uma origem por
arquivo do bronze. O número 90 fica longe dos pedaços normais (o maior arquivo
da RAIS vira 9 pedaços), então não há colisão.

O ARQUIVO NOVO COPIA O SCHEMA DO VIZINHO
---------------------------------------
A leitura da tabela inteira exige colunas iguais em todos os arquivos. O
complemento é gravado com exatamente as colunas do arquivo vizinho da mesma
partição, na mesma ordem e com o mesmo tipo — coluna que o vizinho tem e a
fonte não, entra nula. Sem isso bastaria uma coluna a mais para quebrar a
leitura da série, que é o defeito que a auditoria já pegou uma vez.

Uso:
    python -m auditoria.ajustar_recorte --etapa todas
    python -m auditoria.ajustar_recorte --etapa sair --so-listar
"""
import argparse
import re
import sys
import time

from extracao_ftp.config_extracao import (
    BUCKET_SILVER,
    BUCKET_SILVER_TI,
    conectar_duckdb,
)
from gold_caged import escopo_tecnologia as esc

COLUNAS = {
    "caged_mov": ("subclasse", "cbo2002ocupacao"),
    "caged_exc": ("subclasse", "cbo2002ocupacao"),
    "caged_for": ("subclasse", "cbo2002ocupacao"),
    "caged_old": ("cnae_20_subclas", "cbo_2002_ocupacao"),
    "caged_ajustes": ("cnae_20_subclas", "cbo_2002_ocupacao"),
    "rais_vinc": ("cnae_20_subclasse", "cbo_ocupacao_2002"),
}
TABELAS_CAGED = ("caged_mov", "caged_exc", "caged_for", "caged_old", "caged_ajustes")
SUFIXO = "_parte90"
PREPARO = f"{BUCKET_SILVER_TI}/_complemento"


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def _ler(caminho: str) -> str:
    """
    Um arquivo só, SEM partição hive.

    O DuckDB detecta `ano_particao=2024/` no caminho e acrescenta a coluna
    sozinho. Regravar o arquivo com `SELECT *` levaria essa coluna para DENTRO
    dele, e o arquivo passaria a ter uma coluna que os vizinhos não têm.
    """
    return f"read_parquet('s3://{caminho}', hive_partitioning=false)"


def _schema(con, caminho: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute(
        f"DESCRIBE SELECT * FROM {_ler(caminho)}").fetchall()]


def _vizinho(fs, tabela: str, pasta: str) -> str | None:
    """Um arquivo existente para servir de molde: mesma partição, senão mesmo ano."""
    for padrao in (f"{pasta}/*.parquet", f"{pasta.rsplit('/mes_particao=', 1)[0]}/**/*.parquet",
                   f"{BUCKET_SILVER_TI}/{tabela}/**/*.parquet"):
        candidatos = [c for c in sorted(fs.glob(padrao)) if SUFIXO not in c]
        if candidatos:
            return candidatos[0]
    return None


def _alinhado(con, origem_sql: str, colunas_fonte: set[str],
              molde: list[tuple[str, str]], filtro: str) -> str:
    """SELECT que devolve as linhas no schema exato do molde."""
    partes = []
    for nome, tipo in molde:
        if nome in colunas_fonte:
            partes.append(f'CAST("{nome}" AS {tipo}) AS "{nome}"')
        else:
            partes.append(f'CAST(NULL AS {tipo}) AS "{nome}"')
    return f"SELECT {', '.join(partes)} FROM {origem_sql} WHERE {filtro}"


# ================================================================== SAIR
def sair(con, fs, so_listar: bool) -> int:
    print("\n✂️  SAIR — linhas das ocupações excluídas, fora de empresa de TI\n")
    falhas = total_removido = 0
    for tabela, (cnae, cbo) in COLUNAS.items():
        arquivos = [c for c in sorted(fs.glob(f"{BUCKET_SILVER_TI}/{tabela}/**/*.parquet"))]
        removidas_tabela = arquivos_tocados = 0
        for n, caminho in enumerate(arquivos, start=1):
            colunas = {c for c, _ in _schema(con, caminho)}
            if cbo not in colunas:
                continue
            filtro = esc.sql_saida_cbo(cnae if cnae in colunas else None, cbo)
            antes, sairao = con.execute(
                f"SELECT count(*), count(*) FILTER (WHERE {filtro}) FROM {_ler(caminho)}"
            ).fetchone()
            if not sairao:
                continue
            arquivos_tocados += 1
            removidas_tabela += sairao
            if so_listar:
                continue
            temporario = f"{caminho}.recortando"
            try:
                con.execute(f"COPY (SELECT * FROM {_ler(caminho)} WHERE NOT {filtro}) "
                            f"TO 's3://{temporario}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                depois = con.execute(
                    f"SELECT count(*) FROM {_ler(temporario)}").fetchone()[0]
                if depois != antes - sairao:
                    fs.rm(temporario)
                    falhas += 1
                    print(f"   ❌ {caminho.split('/')[-1]}: {depois:,} ≠ {antes - sairao:,} — preservado")
                    continue
                fs.rm(caminho)
                fs.mv(temporario, caminho)
            except Exception as e:
                falhas += 1
                print(f"   ❌ {caminho.split('/')[-1]}: {str(e)[:120]}")
        total_removido += removidas_tabela
        print(f"   [{list(COLUNAS).index(tabela) + 1}/{len(COLUNAS)}] {tabela:<14} "
              f"{removidas_tabela:>9,} linha(s) em {arquivos_tocados} arquivo(s)"
              f"{'  (só listado)' if so_listar else ''}")
    print(f"\n   total: {total_removido:,} linha(s) {'a sair' if so_listar else 'removida(s)'}, "
          f"{falhas} falha(s)")
    return falhas


# ================================================================= CAGED
def _stem(caminho: str) -> str:
    """'caged_mov_202001_0.parquet' -> 'caged_mov_202001'."""
    return re.sub(r"_\d{1,2}$", "", caminho.split("/")[-1].removesuffix(".parquet"))


def complementar_caged(con, fs, so_listar: bool) -> int:
    print("\n➕ COMPLEMENTO CAGED — família 2112 a partir da silver de mercado completo\n")
    falhas = total = 0
    for tabela in TABELAS_CAGED:
        cnae, cbo = COLUNAS[tabela]
        fontes = sorted(fs.glob(f"{BUCKET_SILVER}/{tabela}/**/*.parquet"))
        gravadas = 0
        for n, fonte in enumerate(fontes, start=1):
            colunas = {c for c, _ in _schema(con, fonte)}
            if cbo not in colunas:
                continue
            filtro = esc.sql_complemento_cbo(cnae if cnae in colunas else None, cbo)
            qtd = con.execute(f"SELECT count(*) FROM {_ler(fonte)} WHERE {filtro}").fetchone()[0]
            if not qtd:
                continue
            gravadas += qtd
            if so_listar:
                continue

            ano = re.search(r"ano_particao=(\d{4})", fonte).group(1)
            stem = _stem(fonte)
            # Os anos anuais do caged_ajustes (2002-2009) vêm sem mês na silver
            # de mercado completo. Na silver de TI eles foram reparticionados pela
            # competência declarada (ver auditoria.reparticionar); o complemento
            # segue a mesma regra, senão voltaria a quebrar a leitura da tabela.
            if "HIVE_DEFAULT" in fonte:
                meses = [int(r[0]) for r in con.execute(
                    f"SELECT DISTINCT substr(competencia_declarada, 5, 2) FROM {_ler(fonte)} "
                    f"WHERE {filtro} AND competencia_declarada IS NOT NULL").fetchall()]
                destinos = [(m, f"{BUCKET_SILVER_TI}/{tabela}/ano_particao={ano}/mes_particao={m}",
                             f"{filtro} AND substr(competencia_declarada, 5, 2)::INT = {m}")
                            for m in meses]
            else:
                mes = re.search(r"mes_particao=(\d{1,2})", fonte).group(1)
                destinos = [(int(mes), f"{BUCKET_SILVER_TI}/{tabela}/ano_particao={ano}/mes_particao={mes}",
                             filtro)]

            for _, pasta, filtro_mes in destinos:
                molde_arq = _vizinho(fs, tabela, pasta)
                if not molde_arq:
                    falhas += 1
                    print(f"   ❌ {stem}: nenhum arquivo de molde em {tabela}")
                    continue
                molde = _schema(con, molde_arq)
                alvo = f"{pasta}/{stem}{SUFIXO}_0.parquet"
                try:
                    esperado = con.execute(
                        f"SELECT count(*) FROM {_ler(fonte)} WHERE {filtro_mes}").fetchone()[0]
                    con.execute(f"COPY ({_alinhado(con, _ler(fonte), colunas, molde, filtro_mes)}) "
                                f"TO 's3://{alvo}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                    gravado = con.execute(f"SELECT count(*) FROM {_ler(alvo)}").fetchone()[0]
                    if gravado != esperado or _schema(con, alvo) != molde:
                        fs.rm(alvo)
                        falhas += 1
                        print(f"   ❌ {alvo.split('/')[-1]}: contagem ou schema divergente — removido")
                except Exception as e:
                    falhas += 1
                    print(f"   ❌ {stem}: {str(e)[:120]}")
        total += gravadas
        print(f"   [{TABELAS_CAGED.index(tabela) + 1}/{len(TABELAS_CAGED)}] {tabela:<14} "
              f"{gravadas:>9,} linha(s){'  (só listado)' if so_listar else ''}")
    print(f"\n   total: {total:,} linha(s) {'a entrar' if so_listar else 'acrescentada(s)'}, "
          f"{falhas} falha(s)")
    return falhas


# ================================================================== RAIS
def complementar_rais(con, fs, so_listar: bool) -> int:
    """
    Roda o construtor normal da RAIS com duas trocas, e depois encaixa.

    As trocas são feitas no módulo e desfeitas no `finally`: o construtor não
    ganha modo novo, e quem o usar depois deste script encontra o
    comportamento de sempre.
    """
    from silver_rais import construir_silver as cs

    print("\n➕ COMPLEMENTO RAIS — família 2112 a partir do bronze\n")
    if so_listar:
        print("   (a RAIS completa não está na silver; a contagem só sai rodando o bronze)")
        return 0

    if fs.exists(PREPARO):
        fs.rm(PREPARO, recursive=True)

    filtro_original, bucket_original = esc.sql_filtro_tecnologia, cs.bucket_silver
    esc.sql_filtro_tecnologia = lambda cnae, cbo: esc.sql_complemento_cbo(cnae, cbo) if cbo else None
    cs.bucket_silver = lambda _so_ti: PREPARO
    try:
        ok = cs.construir(con, fs, "rais_vinc", so_tecnologia=True, forcar=True)
    finally:
        esc.sql_filtro_tecnologia, cs.bucket_silver = filtro_original, bucket_original
    if not ok:
        print("   ⚠️  o construtor reportou falhas; o encaixe segue com o que foi gravado")

    # Encaixe: agrupa os pedaços do preparo por ano + origem e grava UM
    # complemento por origem, no schema do vizinho.
    grupos: dict[tuple[str, str], list[str]] = {}
    for caminho in fs.glob(f"{PREPARO}/rais_vinc/**/*.parquet"):
        ano = re.search(r"ano_particao=(\d{4})", caminho).group(1)
        origem = re.sub(r"_parte\d+$", "", _stem(caminho))
        grupos.setdefault((ano, origem), []).append(caminho)

    falhas = total = 0
    for n, ((ano, origem), pedacos) in enumerate(sorted(grupos.items()), start=1):
        pasta = f"{BUCKET_SILVER_TI}/rais_vinc/ano_particao={ano}"
        molde_arq = _vizinho(fs, "rais_vinc", pasta)
        if not molde_arq:
            falhas += 1
            print(f"   ❌ {ano}/{origem}: sem molde")
            continue
        molde = _schema(con, molde_arq)
        lista = ", ".join(f"'s3://{p}'" for p in pedacos)
        fonte_sql = f"read_parquet([{lista}], hive_partitioning=false, union_by_name=true)"
        colunas = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {fonte_sql}").fetchall()}
        alvo = f"{pasta}/{origem}{SUFIXO}_0.parquet"
        try:
            esperado = con.execute(f"SELECT count(*) FROM {fonte_sql}").fetchone()[0]
            if not esperado:
                continue
            con.execute(f"COPY ({_alinhado(con, fonte_sql, colunas, molde, 'true')}) "
                        f"TO 's3://{alvo}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            gravado = con.execute(f"SELECT count(*) FROM {_ler(alvo)}").fetchone()[0]
            if gravado != esperado or _schema(con, alvo) != molde:
                fs.rm(alvo)
                falhas += 1
                print(f"   ❌ {ano}/{origem}: contagem ou schema divergente — removido")
                continue
            total += gravado
            print(f"   [{n}/{len(grupos)}] ✅ {ano}/{origem}{SUFIXO}: {gravado:,} linha(s)")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(grupos)}] ❌ {ano}/{origem}: {str(e)[:120]}")

    if not falhas and fs.exists(PREPARO):
        fs.rm(PREPARO, recursive=True)
    print(f"\n   total: {total:,} vínculo(s) acrescentado(s) à RAIS, {falhas} falha(s)")
    return falhas


def main() -> int:
    p = argparse.ArgumentParser(description="Ajusta o recorte de TI na silver já construída.")
    p.add_argument("--etapa", choices=("sair", "caged", "rais", "todas"), default="todas")
    p.add_argument("--so-listar", action="store_true",
                   help="Conta o que entraria e sairia, sem gravar nada.")
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=4")
    fs = _fs()

    print("=" * 74)
    print("  AJUSTE DO RECORTE DE TECNOLOGIA")
    print(f"  entra: famílias {sorted(esc.CBO_FAMILIAS_ACRESCENTADAS)}   "
          f"sai: {sorted(esc.CBO_EXCLUIDOS)}")
    print("=" * 74)

    inicio = time.time()
    falhas = 0
    if args.etapa in ("sair", "todas"):
        falhas += sair(con, fs, args.so_listar)
    if args.etapa in ("caged", "todas"):
        falhas += complementar_caged(con, fs, args.so_listar)
    if args.etapa in ("rais", "todas"):
        falhas += complementar_rais(con, fs, args.so_listar)

    print(f"\n🏁 ajuste concluído em {(time.time() - inicio) / 60:.1f} min, {falhas} falha(s)")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
