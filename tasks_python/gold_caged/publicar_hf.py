"""
Publica os parquets consolidados como um dataset público no Hugging Face.

O dashboard hospedado não acessa o MinIO — ele lê os arquivos por HTTPS. O HF
serve com suporte a range request, então o DuckDB baixa só os row groups que a
consulta precisa, em vez do arquivo inteiro.

PRÉ-REQUISITOS
--------------
1. Conta em huggingface.co e um token de escrita
   (Settings > Access Tokens > New token, papel "write").
2. O token na variável de ambiente:
       set HF_TOKEN=hf_xxxxx           (cmd)
       $env:HF_TOKEN="hf_xxxxx"        (PowerShell)
3. Os parquets consolidados:
       python -m gold_caged.consolidar

Uso:
    python -m gold_caged.publicar_hf --repo SEU_USUARIO/caged-tecnologia

Ao final ele imprime a variável DADOS_URL_BASE para configurar no deploy.
"""
import argparse
import os
import re
import sys
from pathlib import Path

DIR_DETALHADO = Path(__file__).resolve().parents[2] / "publicacao" / "detalhado"

CARTAO = """---
license: odc-by
language:
  - pt
tags:
  - brazil
  - labor-market
  - technology
  - caged
pretty_name: CAGED — Mercado de Trabalho em Tecnologia
---

# CAGED — Mercado de Trabalho em Tecnologia (Brasil)

Microdados do **CAGED** (Cadastro Geral de Empregados e Desempregados,
Ministério do Trabalho e Emprego) tratados e recortados para o mercado de
trabalho em tecnologia.

## Origem

`ftp.mtps.gov.br/pdet/microdados` — dados públicos do PDET/MTE.

## Tratamento aplicado

1. Códigos traduzidos pelos **dicionários oficiais** do próprio MTE: cada
   coluna codificada ganhou uma `<coluna>_descricao` legível, mantendo o
   código original ao lado.
2. Encoding detectado por arquivo — o acervo mistura UTF-8 (Novo CAGED,
   2020+) e Latin-1 (CAGED antigo); assumir um só gera mojibake silencioso.
3. Campos numéricos tipados (a fonte usa vírgula decimal) e competência
   `AAAAMM` convertida para data.

## Recorte de tecnologia

União de **duas definições**, que descrevem populações diferentes:

- **Setor de TI** — CNAE de serviços de tecnologia (divisões 62 e 63):
  quem trabalha em empresa de tecnologia.
- **Ocupação de TI** — famílias CBO de ocupações de tecnologia: quem exerce
  função de tecnologia, **em qualquer setor**.

A distinção importa: no Brasil a maior parte dos profissionais de TI trabalha
fora do setor de TI (bancos, varejo, indústria, saúde). Manter as duas lentes
permite medir exatamente isso.

O recorte é por **família de código**, nunca por palavra-chave na descrição —
buscar "sistemas" ou "dados" traria "Operação de Sistemas de Irrigação por
Aspersão" e "Montador de Sistemas de Combustível de Aeronaves".

## Arquivos

{tabela_arquivos}

Ordenados por competência, o que permite ao leitor pular row groups ao
filtrar por período.

## Métrica principal

`saldomovimentacao` vale **+1 na admissão** e **−1 no desligamento**. O saldo
(soma) é a geração líquida de emprego formal — a métrica-título do CAGED.

## Como consultar

```python
import duckdb

BASE = "https://huggingface.co/datasets/{repo}/resolve/main"

duckdb.sql(f'''
    SELECT uf_descricao, count(*) FILTER (WHERE saldomovimentacao = 1) AS admissoes,
           sum(saldomovimentacao) AS saldo
    FROM read_parquet('{{BASE}}/caged_mov.parquet')
    WHERE ano_particao = 2025
    GROUP BY 1 ORDER BY saldo DESC
''').show()
```

## Licença e citação

Dados originais são públicos (MTE/PDET). Este derivado é distribuído sob
ODC-BY: cite a fonte original e este tratamento.
"""


DESCRICAO_TABELA = {
    "caged_mov": "Movimentações do Novo CAGED (2020+)",
    "caged_for": "Movimentações declaradas fora do prazo (Novo CAGED)",
    "caged_exc": "Exclusões de movimentações (Novo CAGED)",
    "caged_old": "CAGED antigo — CAGEDEST (2007–2019)",
    "caged_ajustes": "Ajustes/declarações fora do prazo (CAGED antigo)",
    "caged_ti": "As duas gerações harmonizadas num esquema só",
}

# "caged_mov_2024.parquet" -> ("caged_mov", 2024). Um arquivo por ano (ver
# gold_caged/consolidar.py e gold_caged/unificar.py) — o nome é o contrato
# que o dashboard lê de volta em dashboard/dados.py.
PADRAO_ANO = re.compile(r"^(.+)_(\d{4})\.parquet$")

# Nomes do formato antigo (um arquivo por tabela, sem ano) — ver --limpar-legado.
LEGADO = {f"{t}.parquet" for t in (*DESCRICAO_TABELA, "caged_ti")}


def _tabela_de_arquivos(arquivos: list[Path]) -> str:
    """
    Monta a tabela do card por TABELA (um intervalo de anos cada), a partir
    do que está REALMENTE sendo publicado — fixar a lista no texto faria o
    card prometer arquivos ausentes numa publicação parcial.
    """
    por_tabela: dict[str, list[int]] = {}
    tamanho: dict[str, int] = {}
    for a in arquivos:
        m = PADRAO_ANO.match(a.name)
        if not m:
            continue
        tabela, ano = m.group(1), int(m.group(2))
        por_tabela.setdefault(tabela, []).append(ano)
        tamanho[tabela] = tamanho.get(tabela, 0) + a.stat().st_size

    linhas = ["| Tabela | Conteúdo | Anos | Arquivos | Tamanho |", "|---|---|---|---|---|"]
    for tabela, anos in sorted(por_tabela.items()):
        descricao = DESCRICAO_TABELA.get(tabela, "—")
        anos.sort()
        linhas.append(
            f"| `{tabela}_<ano>.parquet` | {descricao} | {anos[0]}–{anos[-1]} | "
            f"{len(anos)} | {tamanho[tabela] / 1e6:.1f} MB |"
        )
    return "\n".join(linhas)


def _tem_login_cli() -> bool:
    """Detecta o token guardado pelo `huggingface-cli login`."""
    try:
        from huggingface_hub import get_token

        return bool(get_token())
    except Exception:
        return False


def limpar_legado(api, repo: str) -> int:
    """
    Remove os arquivos do formato antigo (um por tabela, sem ano) que o
    esquema por ano substitui. Lista fixa e pequena — nunca toca nos
    arquivos "<tabela>_<ano>.parquet" nem na mirror da silver que convive no
    mesmo repositório (ver silver_caged/publicar_hf.py).
    """
    from huggingface_hub import CommitOperationDelete

    remotos = set(api.list_repo_files(repo, repo_type="dataset"))
    sobrando = sorted(LEGADO & remotos)
    if not sobrando:
        print("   ✅ nada do formato antigo para remover")
        return 0

    print(f"   🗑️  removendo {len(sobrando)} arquivo(s) do formato antigo: {sobrando}")
    api.create_commit(
        repo_id=repo, repo_type="dataset",
        operations=[CommitOperationDelete(path_in_repo=f) for f in sobrando],
        commit_message="Remove arquivos consolidados do formato antigo (substituidos por ano)",
    )
    return len(sobrando)


def main() -> int:
    p = argparse.ArgumentParser(description="Publica os parquets no Hugging Face.")
    # Com padrão, e não obrigatório: os arquivos consolidados pertencem ao mesmo
    # dataset da camada de TI — ficam na raiz dele, ao lado das partições — e o
    # painel dispara este módulo sem passar argumento nenhum.
    p.add_argument("--repo", default="Gianpedro/caged-tecnologia",
                   help="Destino no formato usuario/nome-do-dataset")
    p.add_argument("--privado", action="store_true",
                   help="Cria o dataset privado (padrão: público)")
    p.add_argument("--ano-inicio", type=int, default=0,
                   help="Publica só os arquivos de ano >= este (padrão: todos os locais)")
    p.add_argument("--ano-fim", type=int, default=9999,
                   help="Publica só os arquivos de ano <= este (padrão: todos os locais)")
    p.add_argument("--limpar-legado", action="store_true",
                   help="Remove do repositorio os arquivos do formato antigo (um por tabela, sem ano)")
    args = p.parse_args()

    # Três origens aceitas, da mais segura para a mais prática:
    #   1. login do huggingface-cli — token fica em ~/.cache/huggingface,
    #      FORA do projeto (não viaja se você zipar ou copiar a pasta);
    #   2. variável de ambiente HF_TOKEN;
    #   3. HF_TOKEN no .env da raiz — daí o load_dotenv abaixo, sem o qual
    #      colocar o token no .env simplesmente não teria efeito aqui.
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    # `HF_TOKEN=` (vazio, como fica no .env antes de você colar o token) daria
    # string vazia — que o HfApi trataria como credencial e ignoraria o login
    # do CLI. Normaliza para None, que é o que faz ele usar o login guardado.
    token = os.getenv("HF_TOKEN") or None

    if not token and not _tem_login_cli():
        print("❌ Nenhuma credencial do Hugging Face encontrada. Use uma destas:")
        print("   1) ../.venv/Scripts/huggingface-cli login      (recomendado)")
        print("   2) set HF_TOKEN=hf_xxxxx")
        print("   3) HF_TOKEN=hf_xxxxx no .env da raiz")
        print("\n   Crie o token em https://huggingface.co/settings/tokens (papel: write)")
        return 1

    def _no_recorte(a: Path) -> bool:
        m = PADRAO_ANO.match(a.name)
        return bool(m) and args.ano_inicio <= int(m.group(2)) <= args.ano_fim

    arquivos = sorted(a for a in DIR_DETALHADO.glob("*.parquet") if _no_recorte(a))
    if not arquivos:
        print(f"❌ Nenhum parquet em {DIR_DETALHADO} no recorte pedido.")
        print("   Rode antes: python -m gold_caged.consolidar  (e gold_caged.unificar)")
        return 1

    from huggingface_hub import HfApi

    api = HfApi(token=token)
    print(f"📤 Publicando em https://huggingface.co/datasets/{args.repo}\n")

    api.create_repo(repo_id=args.repo, repo_type="dataset",
                    private=args.privado, exist_ok=True)

    if args.limpar_legado:
        limpar_legado(api, args.repo)

    # O card vai junto: é ele que documenta o recorte metodológico para quem
    # baixar os dados sem ter lido o trabalho.
    cartao = DIR_DETALHADO / "README.md"
    cartao.write_text(
        CARTAO.replace("{repo}", args.repo)
              .replace("{tabela_arquivos}", _tabela_de_arquivos(arquivos)),
        encoding="utf-8",
    )

    total = 0
    for arquivo in arquivos + [cartao]:
        tam = arquivo.stat().st_size
        print(f"   ⬆️  {arquivo.name:<25} {tam / 1e6:>7.1f} MB")
        api.upload_file(
            path_or_fileobj=str(arquivo),
            path_in_repo=arquivo.name,
            repo_id=args.repo,
            repo_type="dataset",
        )
        total += tam

    base = f"https://huggingface.co/datasets/{args.repo}/resolve/main"
    print(f"\n🏁 {total / 1e6:.1f} MB publicados.")
    print("\nConfigure o dashboard com:")
    print(f"   DADOS_URL_BASE={base}")
    print("\nNo Streamlit Cloud, isso vai em Settings > Secrets ou como variável de ambiente.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
