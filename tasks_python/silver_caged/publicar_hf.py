"""
Publica a silver COMPLETA do CAGED (mercado inteiro, traduzida) no Hugging Face.

Diferente de gold_caged/publicar_hf.py, que sobe um punhado de parquets
consolidados do recorte de TI, aqui o que vai é a árvore particionada inteira:
milhares de arquivos em ano_particao=/mes_particao=.

POR QUE PASSA PELO DISCO
------------------------
Os dados moram no MinIO e o Hugging Face quer um diretório local. O espelho em
publicacao/completo/ existe por isso — e, de quebra, é uma cópia local do
dataset publicado, útil quando o MinIO estiver fora do ar.

O espelhamento pula o que já tem o mesmo tamanho no destino, então rodar de
novo depois de uma queda de conexão continua de onde parou em vez de baixar
tudo outra vez.

POR QUE upload_large_folder E NÃO upload_file NUM LAÇO
-----------------------------------------------------
São milhares de arquivos e dezenas de GB. O upload_large_folder agrupa em
commits, paraleliza e guarda o progresso em .cache/ dentro da pasta — se cair
no meio, a retomada não reenvia o que já subiu. Um laço de upload_file faria um
commit por arquivo (milhares de commits no histórico do repo) e recomeçaria do
zero a cada falha.

Uso:
    python -m silver_caged.publicar_hf --repo SEU_USUARIO/caged-brasil
    python -m silver_caged.publicar_hf --repo ... --so-espelhar   (só baixa)
    python -m silver_caged.publicar_hf --repo ... --so-subir      (só envia)
"""
import argparse
import os
import sys
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_SILVER,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
)
from silver_caged import mapeamento as mp

DIR_LOCAL = Path(__file__).resolve().parents[2] / "publicacao" / "completo"

DESCRICAO_TABELA = {
    "caged_mov": "Movimentações do Novo CAGED (2020+)",
    "caged_for": "Movimentações declaradas fora do prazo (Novo CAGED)",
    "caged_exc": "Exclusões de movimentações (Novo CAGED)",
    "caged_old": "CAGED antigo — CAGEDEST (2007–2019)",
    "caged_ajustes": "Ajustes e declarações fora do prazo (CAGED antigo)",
}

CARTAO = """---
license: odc-by
language:
  - pt
tags:
  - brazil
  - labor-market
  - caged
  - microdata
pretty_name: CAGED — Microdados Traduzidos (mercado completo)
size_categories:
  - 100M<n<1B
---

# CAGED — Microdados Traduzidos (Brasil, mercado completo)

Microdados do **CAGED** (Cadastro Geral de Empregados e Desempregados,
Ministério do Trabalho e Emprego) com os **códigos traduzidos** pelos
dicionários oficiais do próprio MTE.

O CAGED é publicado inteiramente codificado: sexo é `1`, grau de instrução é
`1..11`, ocupação é um código CBO, setor é um código CNAE. Ler os microdados
crus exige cruzar à mão dezenas de planilhas de layout espalhadas pelo FTP do
ministério, que mudam de formato entre as gerações da base. Aqui esse trabalho
já está feito.

> **Mercado completo.** Este dataset traz **todas** as movimentações, de todos
> os setores. Para o recorte de tecnologia, veja
> [`{repo_ti}`]({url_ti}) — não some os dois, as linhas de TI estão nos dois.

## Origem

`ftp.mtps.gov.br/pdet/microdados` — dados públicos do PDET/MTE.

## Tratamento aplicado

1. **Códigos traduzidos** pelos dicionários oficiais do MTE: cada coluna
   codificada ganhou uma `<coluna>_descricao` legível, **mantendo o código
   original ao lado**. Nada foi substituído — dá para conferir a tradução e
   para reagrupar por código.
2. **Encoding detectado por arquivo.** O acervo mistura UTF-8 (Novo CAGED,
   2020+) e Latin-1 (CAGED antigo); assumir um só produz mojibake silencioso
   nos nomes de município e ocupação.
3. **Campos numéricos tipados** — a fonte usa vírgula decimal — e competência
   `AAAAMM` convertida para `DATE`.

## Estrutura

Particionado em Hive por ano e mês:

```
{{tabela}}/ano_particao=YYYY/mes_particao=M/*.parquet
```

Filtrar por ano ou mês lê só as pastas correspondentes, sem tocar no resto.

{tabela_arquivos}

`mes_particao=__HIVE_DEFAULT_PARTITION__` aparece em `caged_ajustes`: os
arquivos de 2002 a 2009 são anuais na fonte, sem competência mensal.

## Métrica principal

`saldomovimentacao` (Novo CAGED) e `saldo_mov` (CAGED antigo) valem **+1 na
admissão** e **−1 no desligamento**. A soma é a geração líquida de emprego
formal — a métrica-título do CAGED.

## Como consultar

Sem baixar nada, direto do Hub:

```python
import duckdb

duckdb.sql("INSTALL httpfs; LOAD httpfs;")
duckdb.sql('''
    SELECT uf_descricao,
           count(*) FILTER (WHERE saldomovimentacao = 1) AS admissoes,
           sum(saldomovimentacao)                        AS saldo
    FROM read_parquet(
        'hf://datasets/{repo}/caged_mov/**/*.parquet',
        hive_partitioning = true
    )
    WHERE ano_particao = 2025
    GROUP BY 1 ORDER BY saldo DESC
''').show()
```

O `WHERE ano_particao = 2025` é resolvido pelo nome das pastas: os outros anos
nem chegam a ser baixados.

## Licença e citação

Os dados originais são públicos (MTE/PDET). Este derivado é distribuído sob
**ODC-BY**: cite a fonte original e este tratamento.
"""


def _fs_minio():
    import s3fs

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}", "region_name": MINIO_REGION},
    )


def espelhar(fs, tabelas: list[str], destino: Path) -> tuple[int, int]:
    """Baixa silver/<tabela>/** para o disco, preservando a árvore de partições."""
    baixados = pulados = 0

    for tabela in tabelas:
        arquivos = sorted(fs.glob(f"{BUCKET_SILVER}/{tabela}/**/*.parquet"))
        if not arquivos:
            print(f"   ⏭️  {tabela}: nada na silver ainda")
            continue

        print(f"   📥 {tabela}: {len(arquivos)} arquivo(s)")
        for n, remoto in enumerate(arquivos, start=1):
            relativo = remoto.split(f"{BUCKET_SILVER}/", 1)[1]
            local = destino / relativo
            tamanho = fs.info(remoto)["size"]

            # Mesmo tamanho = já espelhado. Um parquet truncado por queda de
            # conexão tem tamanho diferente do original, então é repuxado.
            if local.exists() and local.stat().st_size == tamanho:
                pulados += 1
                continue

            local.parent.mkdir(parents=True, exist_ok=True)
            fs.get(remoto, str(local))
            baixados += 1
            if baixados % 100 == 0:
                print(f"      {n}/{len(arquivos)} — {baixados} baixado(s)")

    return baixados, pulados


def _tabela_de_arquivos(destino: Path, tabelas: list[str]) -> str:
    """
    Monta a tabela do card a partir do que está REALMENTE no espelho.

    Fixar a lista no texto faria o card prometer tabelas ausentes numa
    publicação parcial — e ela é parcial por natureza enquanto alguma silver
    ainda está sendo construída.
    """
    linhas = ["| Tabela | Conteúdo | Arquivos | Tamanho |", "|---|---|---|---|"]
    for tabela in tabelas:
        pasta = destino / tabela
        if not pasta.exists():
            continue
        arquivos = list(pasta.rglob("*.parquet"))
        if not arquivos:
            continue
        tam = sum(a.stat().st_size for a in arquivos)
        linhas.append(f"| `{tabela}/` | {DESCRICAO_TABELA.get(tabela, '—')} | "
                      f"{len(arquivos)} | {tam / 1e9:.2f} GB |")
    return "\n".join(linhas)


def _credencial() -> str | None:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    # HF_TOKEN vazio no .env viraria string vazia, que o HfApi trataria como
    # credencial e ignoraria o login do CLI. None é o que faz ele usar o login.
    return os.getenv("HF_TOKEN") or None


def main() -> int:
    p = argparse.ArgumentParser(description="Publica a silver completa do CAGED no Hugging Face.")
    p.add_argument("--repo", required=True, help="Destino no formato usuario/nome-do-dataset")
    p.add_argument("--repo-ti", default="Gianpedro/caged-tecnologia",
                   help="Dataset do recorte de TI, referenciado no card")
    p.add_argument("--tabela", nargs="+", choices=mp.TODAS_TABELAS, default=list(mp.TODAS_TABELAS))
    p.add_argument("--privado", action="store_true")
    p.add_argument("--so-espelhar", action="store_true", help="Só baixa do MinIO, não envia")
    p.add_argument("--so-subir", action="store_true", help="Só envia o que já está no espelho")
    args = p.parse_args()

    DIR_LOCAL.mkdir(parents=True, exist_ok=True)

    if not args.so_subir:
        print(f"📥 Espelhando s3://{BUCKET_SILVER} -> {DIR_LOCAL}\n")
        baixados, pulados = espelhar(_fs_minio(), args.tabela, DIR_LOCAL)
        print(f"\n   ✅ {baixados} baixado(s), {pulados} já no espelho")

    if args.so_espelhar:
        return 0

    token = _credencial()
    from huggingface_hub import HfApi, get_token

    if not token and not get_token():
        print("❌ Nenhuma credencial do Hugging Face. Use uma destas:")
        print("   1) ../.venv/Scripts/huggingface-cli login      (recomendado)")
        print("   2) HF_TOKEN=hf_xxxxx no .env da raiz")
        return 1

    arquivos = list(DIR_LOCAL.rglob("*.parquet"))
    if not arquivos:
        print(f"❌ Nenhum parquet em {DIR_LOCAL}. Rode sem --so-subir para espelhar antes.")
        return 1
    total = sum(a.stat().st_size for a in arquivos)

    api = HfApi(token=token)
    print(f"\n📤 {len(arquivos)} arquivo(s), {total / 1e9:.2f} GB")
    print(f"   -> https://huggingface.co/datasets/{args.repo}\n")

    api.create_repo(repo_id=args.repo, repo_type="dataset",
                    private=args.privado, exist_ok=True)

    # O card documenta o recorte para quem baixar os dados sem ter lido o
    # trabalho — e avisa que este NÃO é o dataset de TI.
    (DIR_LOCAL / "README.md").write_text(
        CARTAO.replace("{repo_ti}", args.repo_ti)
              .replace("{url_ti}", f"https://huggingface.co/datasets/{args.repo_ti}")
              .replace("{repo}", args.repo)
              .replace("{tabela_arquivos}", _tabela_de_arquivos(DIR_LOCAL, args.tabela)),
        encoding="utf-8",
    )

    api.upload_large_folder(
        folder_path=str(DIR_LOCAL),
        repo_id=args.repo,
        repo_type="dataset",
        print_report=True,
    )

    print(f"\n🏁 Publicado: https://huggingface.co/datasets/{args.repo}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
