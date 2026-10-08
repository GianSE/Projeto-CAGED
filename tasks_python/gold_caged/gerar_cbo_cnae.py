"""
Gera as tabelas de referência cbos.parquet e cnaes.parquet: todo código
CBO/CNAE do dicionário do MTE, com a hierarquia oficial derivada do próprio
código, e uma coluna `categoria` pronta para curadoria manual.

POR QUE EXISTE
---------------
O recorte de TI hoje (gold_caged/escopo_tecnologia.py) é código a código,
não por família/divisão inteira — famílias/divisões "quase certas" têm
exceção (CBO_EXCLUIDOS) e tem código avulso fora de qualquer família
(CBO_AVULSOS_TI, CNAE_TI por subclasse). Isso é importado de volta aqui:
a categoria "ti" nasce pré-preenchida EXATAMENTE como o recorte já validado
diz, não por uma aproximação grosseira de família/divisão.

Categorias novas (Engenharia, Saúde, Educação, ...) ficam para você curar
por cima das colunas de hierarquia já derivadas — família pro CBO, divisão
pro CNAE, o mesmo nível que já funcionou pra TI.

Uso:
    python -m gold_caged.gerar_cbo_cnae
"""
import sys
from pathlib import Path

from extracao_ftp.config_extracao import conectar_duckdb
from gold_caged.cnae_secao import FAIXAS_SECAO
from gold_caged.escopo_tecnologia import (
    CBO_AVULSOS_TI,
    CBO_EXCLUIDOS,
    CBO_FAMILIAS_TI,
    CNAE_TI,
)

DIR_SAIDA = Path(__file__).resolve().parents[2] / "publicacao" / "referencias"

REPO_DICIONARIOS = "Gianpedro/mte-dicionarios"
FONTE_CBO = (f"hf://datasets/{REPO_DICIONARIOS}/novo_caged_legado_movimentacoes/"
             "layout_novo_caged_movimentacao/cbo2002ocupacao.parquet")
FONTE_CNAE = (f"hf://datasets/{REPO_DICIONARIOS}/novo_caged_legado_movimentacoes/"
              "layout_novo_caged_movimentacao/subclasse.parquet")


def _secao(divisao: int) -> str | None:
    for ini, fim, letra, _nome in FAIXAS_SECAO:
        if ini <= divisao <= fim:
            return letra
    return None


def gerar_cbos(con) -> int:
    linhas = con.execute(f"""
        SELECT col_00 AS codigo, col_01 AS descricao
        FROM read_parquet('{FONTE_CBO}')
        WHERE col_00 != 'Código' AND col_00 IS NOT NULL
    """).fetchall()

    familias_ti = set(CBO_FAMILIAS_TI)
    excluidos = set(CBO_EXCLUIDOS)
    avulsos = set(CBO_AVULSOS_TI)

    registros = []
    for codigo, descricao in linhas:
        codigo = codigo.strip()
        if not codigo.isdigit() or len(codigo) < 4:
            continue
        codigo = codigo.zfill(6)
        familia = codigo[:4]
        categoria = None
        if codigo in avulsos:
            categoria = "ti"
        elif familia in familias_ti and codigo not in excluidos:
            categoria = "ti"
        registros.append((
            codigo, descricao, codigo[0], codigo[:2], codigo[:3], familia, categoria,
        ))

    con.execute("""
        CREATE OR REPLACE TABLE _cbos (
            codigo VARCHAR, descricao VARCHAR, grande_grupo VARCHAR,
            subgrupo_principal VARCHAR, subgrupo VARCHAR, familia VARCHAR,
            categoria VARCHAR
        )
    """)
    con.executemany("INSERT INTO _cbos VALUES (?, ?, ?, ?, ?, ?, ?)", registros)

    destino = DIR_SAIDA / "cbos.parquet"
    con.execute(f"COPY (SELECT * FROM _cbos ORDER BY codigo) TO '{destino.as_posix()}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)")
    return len(registros)


def gerar_cnaes(con) -> int:
    linhas = con.execute(f"""
        SELECT col_00 AS codigo, col_01 AS descricao
        FROM read_parquet('{FONTE_CNAE}')
        WHERE col_00 != 'Código' AND col_00 IS NOT NULL
    """).fetchall()

    ti = set(CNAE_TI)

    registros = []
    for codigo, descricao in linhas:
        codigo = codigo.strip()
        if not codigo.isdigit() or len(codigo) > 7:
            continue
        # Divisão 01-09 perde o zero à esquerda na fonte (6 dígitos em vez
        # de 7) — sem o zfill essas subclasses ficavam de fora da tabela.
        codigo = codigo.zfill(7)
        divisao = int(codigo[:2])
        categoria = "ti" if codigo in ti else None
        registros.append((
            codigo, descricao, _secao(divisao), codigo[:2], codigo[:3], codigo[:6], categoria,
        ))

    con.execute("""
        CREATE OR REPLACE TABLE _cnaes (
            codigo VARCHAR, descricao VARCHAR, secao VARCHAR,
            divisao VARCHAR, grupo VARCHAR, classe VARCHAR, categoria VARCHAR
        )
    """)
    con.executemany("INSERT INTO _cnaes VALUES (?, ?, ?, ?, ?, ?, ?)", registros)

    destino = DIR_SAIDA / "cnaes.parquet"
    con.execute(f"COPY (SELECT * FROM _cnaes ORDER BY codigo) TO '{destino.as_posix()}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)")
    return len(registros)


def main() -> int:
    DIR_SAIDA.mkdir(parents=True, exist_ok=True)
    con = conectar_duckdb()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    n_cbo = gerar_cbos(con)
    n_cnae = gerar_cnaes(con)

    for nome, n in (("cbos.parquet", n_cbo), ("cnaes.parquet", n_cnae)):
        destino = DIR_SAIDA / nome
        ti = con.execute(
            f"SELECT count(*) FROM read_parquet('{destino.as_posix()}') WHERE categoria = 'ti'"
        ).fetchone()[0]
        print(f"✅ {nome}: {n} código(s), {ti} já marcado(s) 'ti' -> {destino}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
