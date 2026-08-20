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
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)

RAIZ_PUBLICACAO = Path(__file__).resolve().parents[2] / "publicacao"


def _fontes(camada: str):
    """Devolve (construtor, tabelas, fn_mapa) da camada — as assinaturas diferem."""
    if camada == "caged":
        from silver_caged import construir_silver as cs, mapeamento as mp

        return cs, mp.TODAS_TABELAS, lambda con, fs, t, c: cs._mapa_traducao(con, fs, t, c)

    from silver_rais import construir_silver as cs, mapeamento as mp

    return cs, mp.TABELAS_RAIS, lambda con, fs, t, c: cs._mapa_traducao(fs, t, c)


def gerar(camada: str, destino: Path) -> Path | None:
    from silver_caged.dicionarios import criar_view

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
            # A view já entrega uma linha por chave canônica, sem duplicata.
            partes.append(
                f"SELECT '{tabela}' AS tabela, '{coluna}' AS coluna, "
                f"codigo, descricao FROM {nome_view}"
            )
        print(f"   📖 {tabela}: {len(mapa)} dimensão(ões)")

    if not partes:
        print("   ⚠️  Nenhuma dimensão gerada.")
        return None

    destino.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
            SELECT * FROM ({' UNION ALL '.join(partes)})
            ORDER BY tabela, coluna, try_cast(codigo AS BIGINT) NULLS LAST, codigo
        ) TO '{destino.as_posix()}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL}
        );
    """)

    linhas, colunas_distintas = con.execute(
        f"SELECT count(*), count(DISTINCT (tabela, coluna)) "
        f"FROM read_parquet('{destino.as_posix()}')"
    ).fetchone()
    print(f"\n   ✅ {linhas:,} códigos em {colunas_distintas} dimensão(ões)")
    print(f"   📁 {destino.stat().st_size / 1024:.0f} KB -> {destino}")
    return destino


def main() -> int:
    p = argparse.ArgumentParser(description="Gera o parquet de dimensões do dataset.")
    p.add_argument("--camada", choices=("caged", "rais"), required=True)
    p.add_argument("--destino", type=Path, default=None,
                   help="Padrão: publicacao/{completo|rais}/dicionarios.parquet")
    args = p.parse_args()

    destino = args.destino or (
        RAIZ_PUBLICACAO / ("completo" if args.camada == "caged" else "rais")
        / "dicionarios.parquet"
    )
    print(f"📚 Dimensões da camada {args.camada}\n")
    return 0 if gerar(args.camada, destino) else 1


if __name__ == "__main__":
    sys.exit(main())
