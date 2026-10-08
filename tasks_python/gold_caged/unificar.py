"""
Gera um parquet POR ANO com as duas gerações do CAGED harmonizadas.

POR QUE UNIFICAR
----------------
Publicar `caged_mov.parquet` e `caged_old.parquet` separados obriga quem
consome a descobrir sozinho que `saldomovimentacao` e `saldo_mov` são a mesma
coisa, que a competência muda de nome, e que o setor está numa taxonomia em
uma base e em outra na outra. Nomes estáveis tornam a série 2007–2026
utilizável direto.

POR QUE POR ANO, E NÃO UM ARQUIVO SÓ
-------------------------------------
Mesmo motivo de `consolidar.py`: um arquivo por ano deixa corrigir ou
publicar um ano sem reescrever a série inteira (ver o buraco de 2022 achado
na auditoria — era exatamente esse o tipo de correção pontual que um arquivo
único tornava caro).

O SETOR TAMBÉM É UNIFICADO
--------------------------
O CAGED antigo não traz a seção CNAE pronta — traz o subsetor IBGE, que é
outra taxonomia. Mas traz `cnae_20_subclas` com os MESMOS códigos CNAE 2.0 do
Novo CAGED, e a seção é determinada pela divisão (dois primeiros dígitos).
Derivando a seção (ver cnae_secao.py), a análise setorial deixa de ficar
presa ao período recente e passa a cobrir os 20 anos.

A derivação foi validada contra a seção oficial do Novo CAGED: bate em 100%
dos casos (as únicas diferenças eram de grafia do próprio dicionário do MTE,
agora replicada).

Uso:
    python -m gold_caged.unificar
    python -m gold_caged.unificar --ano-inicio 2022 --ano-fim 2022
"""
import argparse
import sys
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_SILVER_TI,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)
from gold_caged.cnae_secao import sql_secao
from gold_unificado.dicionario_canonico import sql_canonico
from gold_caged.escopo_tecnologia import sql_area_ti

DIR_SAIDA = Path(__file__).resolve().parents[2] / "publicacao" / "detalhado"

TAMANHO_ROW_GROUP = 50_000


def anos_disponiveis(con) -> list[int]:
    mov = f"s3://{BUCKET_SILVER_TI}/caged_mov/**/*.parquet"
    old = f"s3://{BUCKET_SILVER_TI}/caged_old/**/*.parquet"
    anos = set()
    for origem in (mov, old):
        try:
            anos.update(r[0] for r in con.execute(
                f"SELECT DISTINCT ano_particao FROM read_parquet('{origem}', union_by_name=true)"
            ).fetchall())
        except Exception:
            pass
    return sorted(anos)


def _existe(con, origem: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM read_parquet('{origem}') LIMIT 0")
        return True
    except Exception:
        return False


def sql_unificado(con, ano: int) -> str | None:
    """
    Monta a união das duas gerações PARA UM ANO. Nos anos de transição (uma
    geração sem dado ainda, ou já sem dado) só o lado que existe entra —
    sem isso, read_parquet num glob vazio derruba o ano inteiro.
    """
    mov = f"s3://{BUCKET_SILVER_TI}/caged_mov/ano_particao={ano}/**/*.parquet"
    old = f"s3://{BUCKET_SILVER_TI}/caged_old/ano_particao={ano}/**/*.parquet"

    bloco_mov = f"""
        SELECT
            competenciamov_data                AS competencia,
            ano_particao                       AS ano,
            mes_particao                       AS mes,
            uf_descricao                       AS uf,
            municipio_descricao                AS municipio,
            secao_descricao                    AS setor,
            subclasse                          AS cnae_subclasse,
            subclasse_descricao                AS cnae_subclasse_descricao,
            cbo2002ocupacao                    AS cbo,
            cbo2002ocupacao_descricao          AS ocupacao,
            {sql_area_ti("cbo2002ocupacao")}   AS area_ti,
            {sql_canonico("sexo", "sexo_descricao")} AS sexo,
            {sql_canonico("raca_cor", "racacor_descricao")} AS raca_cor,
            {sql_canonico("escolaridade", "graudeinstrucao_descricao")} AS escolaridade,
            idade,
            saldomovimentacao                  AS saldo,
            salario                            AS salario,
            tipomovimentacao_descricao         AS tipo_movimentacao,
            'Novo CAGED'                       AS geracao
        FROM read_parquet('{mov}')
    """ if _existe(con, mov) else None

    bloco_old = f"""
        SELECT
            competencia_declarada_data         AS competencia,
            ano_particao                       AS ano,
            mes_particao                       AS mes,
            uf_descricao                       AS uf,
            municipio_descricao                AS municipio,
            {sql_secao("cnae_20_subclas")}     AS setor,
            cnae_20_subclas                    AS cnae_subclasse,
            cnae_20_subclas_descricao          AS cnae_subclasse_descricao,
            cbo_2002_ocupacao                  AS cbo,
            cbo_2002_ocupacao_descricao        AS ocupacao,
            {sql_area_ti("cbo_2002_ocupacao")}  AS area_ti,
            {sql_canonico("sexo", "sexo_descricao")} AS sexo,
            {sql_canonico("raca_cor", "raca_cor_descricao")} AS raca_cor,
            {sql_canonico("escolaridade", "grau_instrucao_descricao")} AS escolaridade,
            idade,
            saldo_mov                          AS saldo,
            salario_mensal                     AS salario,
            tipo_mov_desagregado_descricao     AS tipo_movimentacao,
            'CAGED antigo'                     AS geracao
        FROM read_parquet('{old}')
    """ if _existe(con, old) else None

    blocos = [b for b in (bloco_mov, bloco_old) if b]
    return "\nUNION ALL BY NAME\n".join(blocos) if blocos else None


def unificar_ano(con, ano: int) -> bool:
    destino = DIR_SAIDA / f"caged_ti_{ano}.parquet"
    sql = sql_unificado(con, ano)
    if not sql:
        print(f"   ⏭️  {ano}: nenhuma das duas gerações tem dado, pulando")
        return False

    con.execute(f"""
        COPY (SELECT * FROM ({sql}) ORDER BY competencia)
        TO '{destino.as_posix()}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
            ROW_GROUP_SIZE {TAMANHO_ROW_GROUP}
        );
    """)

    linhas, setores, geracoes = con.execute(f"""
        SELECT count(*), count(DISTINCT setor), count(DISTINCT geracao)
        FROM read_parquet('{destino.as_posix()}')
    """).fetchone()
    tam = destino.stat().st_size
    print(f"   ✅ {ano}  {linhas:>9,} linhas  {setores} setores  "
          f"{geracoes} geração(ões)  {tam / 1e6:>6.1f} MB")
    return True


def _argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    return p.parse_args()


def main() -> int:
    args = _argumentos()
    DIR_SAIDA.mkdir(parents=True, exist_ok=True)
    con = conectar_duckdb()

    print("📦 Unificando as duas gerações do CAGED (por ano)\n")
    anos = [a for a in anos_disponiveis(con) if args.ano_inicio <= a <= args.ano_fim]
    ok = sum(unificar_ano(con, ano) for ano in anos)

    total = sum(f.stat().st_size for f in DIR_SAIDA.glob("caged_ti_*.parquet"))
    print(f"\n🏁 {ok} ano(s) unificado(s) · {total / 1e6:.1f} MB no total")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
