"""
Gera o parquet de dimensões que acompanha o dataset publicado.

POR QUE PUBLICAR AS DIMENSÕES SE O FATO JÁ VEM TRADUZIDO
--------------------------------------------------------
O fato traz código e descrição lado a lado, então ninguém PRECISA da dimensão
para ler os dados. Ela agrega três coisas que o fato desnormalizado não dá:

  1. A lista COMPLETA de códigos, inclusive os que não aparecem no período —
     saber que uma ocupação existe e teve zero movimentação é informação.
  2. Auditabilidade: quem duvidar de uma tradução confere contra o de/para
     oficial, sem precisar caçar a planilha no FTP do ministério.
  3. A opção de trabalhar só com IDs, para quem preferir o modelo dimensional.

Custa poucas centenas de KB num dataset de gigabytes.

POR QUE UM ARQUIVO LONGO, E NÃO UM dim_ POR CONCEITO
-----------------------------------------------------
Porque "sexo" não é um conceito só. No CAGEDEST sexo é 1=Masculino/2=Feminino;
no Novo CAGED é 1=Homem/3=Mulher. Raça/cor e grau de instrução também divergem.
Um `dim_sexo.parquet` fundiria dois sistemas de código incompatíveis, e o erro
apareceria como uma contagem errada — não como uma falha.

O formato longo (tabela, coluna, codigo, descricao) torna essa diferença
explícita e impossível de ignorar: para juntar, você precisa dizer de qual
tabela é o código. Também evita espalhar ~100 arquivos minúsculos pela árvore
do repositório.

Uso:
    python -m silver_caged.dimensoes --camada caged
    python -m silver_caged.dimensoes --camada rais --destino ...
"""
import argparse
import sys
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_SILVER,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)

RAIZ_PUBLICACAO = Path(__file__).resolve().parents[2] / "publicacao"

# ONDE O DICIONÁRIO CONSOLIDADO MORA
# ----------------------------------
# No lake, junto com todo o resto. Ele viveu um tempo apenas em
# `publicacao/{camada}/dicionarios.parquet`, e isso estava errado por dois
# motivos:
#
#   - `publicacao/` é ESPELHO. Serve para montar o que sobe ao Hugging Face e
#     é descartável por definição — os publicadores rebaixam do MinIO antes de
#     enviar. Ter ali a única cópia de um dado derivado significava que limpar
#     o espelho apagava dado.
#   - `auditoria/traduzir_por_prefixo` lê este arquivo em tempo de execução.
#     Um módulo de manutenção dependendo de pasta de publicação inverte a
#     direção do fluxo: quem produz passa a depender de quem distribui.
#
# O dicionário BRUTO (uma aba de planilha por arquivo) já estava no lake, em
# `bronze/dicionarios/`. O consolidado é derivado dele, então é silver — mesma
# camada, mesma regra que todas as outras tabelas.
def caminho_canonico(camada: str) -> str:
    """O lugar do dicionário consolidado no lake."""
    return f"s3://{BUCKET_SILVER}/dicionarios/{camada}.parquet"


def _fontes(camada: str):
    """Devolve (construtor, tabelas, fn_mapa) da camada — as assinaturas diferem."""
    if camada == "caged":
        from silver_caged import construir_silver as cs, mapeamento as mp

        return cs, mp.TODAS_TABELAS, lambda con, fs, t, c: cs._mapa_traducao(con, fs, t, c)

    from silver_rais import construir_silver as cs, mapeamento as mp

    return cs, mp.TABELAS_RAIS, lambda con, fs, t, c: cs._mapa_traducao(fs, t, c)


def _extraido_em(fs, caminho_parquet: str) -> str:
    """
    Quando este de/para foi extraído do FTP.

    Vem da data de escrita do parquet do dicionário, que é exatamente o instante
    da extração — evita uma terceira ida ao FTP só para carimbar a data.

    Importa porque o MTE revisa as planilhas de layout: quem baixar a dimensão
    hoje e daqui a um ano precisa saber se o de/para mudou ou se é o mesmo. E
    precisa saber isso lendo o arquivo, não consultando o nosso MinIO — por isso
    a data viaja como coluna, e não fica só no metadado do storage.
    """
    try:
        alvos = fs.glob(caminho_parquet.replace("s3://", ""))
        if not alvos:
            return ""
        return max(fs.info(a)["LastModified"] for a in alvos).date().isoformat()
    except Exception:
        return ""


def _procedencia(con, caminho_parquet: str) -> tuple[str, str, str]:
    """
    De qual planilha, aba e caminho do FTP saiu este de/para.

    Sem isto a dimensão diz "o código 1 é Branca" sem dizer onde conferir — e
    como o arquivo publicado junta 126 de/para num só, a origem de cada linha
    seria impossível de recuperar. `caminho_ftp` só existe nos dicionários
    extraídos depois que passamos a gravá-lo; nos antigos volta vazio em vez de
    quebrar a geração.
    """
    try:
        colunas = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{caminho_parquet}') LIMIT 0").fetchall()}
        ftp = "any_value(caminho_ftp)" if "caminho_ftp" in colunas else "''"
        planilha, aba, caminho = con.execute(f"""
            SELECT any_value(planilha_origem), any_value(aba_origem), {ftp}
            FROM read_parquet('{caminho_parquet}')
        """).fetchone()
    except Exception:
        return "", "", ""

    # O nome vem prefixado com o slug da pasta ("novo_caged__Layout ....xlsx"),
    # que é detalhe de armazenamento nosso — quem procura no FTP quer o nome
    # original do arquivo.
    planilha = (planilha or "").split("__", 1)[-1]
    return planilha, aba or "", caminho or ""


def gerar(camada: str, destino: "str | Path | None" = None) -> "str | Path | None":
    """
    Consolida as dimensões da camada num parquet único.

    `destino` aceita caminho local (para montar o espelho de publicação) ou
    URI `s3://` (o lake). Sem argumento, grava no lugar canônico — que é o
    lake, para que o arquivo local seja sempre uma CÓPIA e nunca a única.
    """
    from silver_caged.dicionarios import criar_view, _caminho

    destino = destino if destino is not None else caminho_canonico(camada)

    cs, tabelas, fn_mapa = _fontes(camada)
    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    fs = cs._fs_minio()

    partes = []
    for tabela in tabelas:
        colunas = cs._colunas_bronze(con, tabela)
        if not colunas:
            print(f"   ⏭️  {tabela}: sem bronze, pulando")
            continue

        mapa = fn_mapa(con, fs, tabela, colunas)
        for coluna, spec in sorted(mapa.items()):
            spec = dict(spec)
            namespace, aba, estilo = spec.pop("namespace"), spec.pop("aba"), spec.pop("estilo")
            nome_view = f"dim_{tabela}_{coluna}"
            if not criar_view(con, namespace, aba, estilo, nome_view, **spec):
                continue

            fonte = _caminho(namespace, aba, spec.get("planilha"))
            planilha, aba_origem, ftp = _procedencia(con, fonte)
            extraido = _extraido_em(fs, fonte)

            def lit(v: str) -> str:
                return "'" + v.replace("'", "''") + "'"

            # A view já entrega uma linha por chave canônica, sem duplicata.
            partes.append(
                f"SELECT '{tabela}' AS tabela, '{coluna}' AS coluna, "
                f"codigo, descricao, "
                f"{lit(planilha)} AS planilha, {lit(aba_origem)} AS aba, "
                f"{lit(ftp)} AS caminho_ftp, {lit(extraido)} AS extraido_em "
                f"FROM {nome_view}"
            )
        print(f"   📖 {tabela}: {len(mapa)} dimensão(ões)")

    if not partes:
        print("   ⚠️  Nenhuma dimensão gerada.")
        return None

    if isinstance(destino, Path):
        destino.parent.mkdir(parents=True, exist_ok=True)
        alvo = destino.as_posix()
    else:
        alvo = str(destino)

    con.execute(f"""
        COPY (
            SELECT * FROM ({' UNION ALL '.join(partes)})
            ORDER BY tabela, coluna, try_cast(codigo AS BIGINT) NULLS LAST, codigo
        ) TO '{alvo}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL}
        );
    """)

    linhas, colunas_distintas = con.execute(
        f"SELECT count(*), count(DISTINCT (tabela, coluna)) "
        f"FROM read_parquet('{alvo}')"
    ).fetchone()
    print(f"\n   ✅ {linhas:,} códigos em {colunas_distintas} dimensão(ões)")
    tamanho = (f"{destino.stat().st_size / 1024:.0f} KB "
               if isinstance(destino, Path) else "")
    print(f"   📁 {tamanho}-> {alvo}")
    return destino


def main() -> int:
    p = argparse.ArgumentParser(description="Gera o parquet de dimensões do dataset.")
    p.add_argument("--camada", choices=("caged", "rais"), required=True)
    p.add_argument("--destino", type=Path, default=None,
                   help="Caminho local, para montar o espelho de publicação. "
                        "Sem isto grava no lake (s3://silver/dicionarios/).")
    p.add_argument("--tambem-local", action="store_true",
                   help="Grava no lake E no espelho de publicação.")
    args = p.parse_args()

    print(f"📚 Dimensões da camada {args.camada}\n")
    if args.destino:
        return 0 if gerar(args.camada, args.destino) else 1

    if not gerar(args.camada):
        return 1
    if args.tambem_local:
        copia = (RAIZ_PUBLICACAO / ("completo" if args.camada == "caged" else "rais")
                 / "dicionarios.parquet")
        gerar(args.camada, copia)
    return 0


if __name__ == "__main__":
    sys.exit(main())
