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

| Arquivo | Conteúdo |
|---|---|
| `caged_mov.parquet` | Movimentações do Novo CAGED (2020+) |
| `caged_old.parquet` | CAGED antigo (2007–2019) |
| `caged_ajustes.parquet` | Declarações fora do prazo |
| `caged_for.parquet` | Movimentações fora do prazo (Novo CAGED) |
| `caged_exc.parquet` | Exclusões (Novo CAGED) |

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


def main() -> int:
    p = argparse.ArgumentParser(description="Publica os parquets no Hugging Face.")
    p.add_argument("--repo", required=True,
                   help="Destino no formato usuario/nome-do-dataset")
    p.add_argument("--privado", action="store_true",
                   help="Cria o dataset privado (padrão: público)")
    args = p.parse_args()

    token = os.getenv("HF_TOKEN")
    if not token:
        print("❌ Defina HF_TOKEN com um token de escrita do Hugging Face.")
        print("   Crie em: https://huggingface.co/settings/tokens")
        return 1

    arquivos = sorted(DIR_DETALHADO.glob("*.parquet"))
    if not arquivos:
        print(f"❌ Nenhum parquet em {DIR_DETALHADO}.")
        print("   Rode antes: python -m gold_caged.consolidar")
        return 1

    from huggingface_hub import HfApi

    api = HfApi(token=token)
    print(f"📤 Publicando em https://huggingface.co/datasets/{args.repo}\n")

    api.create_repo(repo_id=args.repo, repo_type="dataset",
                    private=args.privado, exist_ok=True)

    # O card vai junto: é ele que documenta o recorte metodológico para quem
    # baixar os dados sem ter lido o trabalho.
    cartao = DIR_DETALHADO / "README.md"
    cartao.write_text(CARTAO.replace("{repo}", args.repo), encoding="utf-8")

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
