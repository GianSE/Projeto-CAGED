"""
Publica a camada BRONZE no Hugging Face, um ano por vez.

POR QUE PUBLICAR O BRONZE
-------------------------
O MTE distribui os microdados em `.7z` com texto de largura fixa, encoding
misto (UTF-8 no Novo CAGED, Latin-1 no antigo), delimitador que varia entre
arquivos e nomes de coluna com mojibake. Transformar isso em parquet
particionado é o trabalho difícil de refazer — a tradução, com o dicionário em
mãos, é um LEFT JOIN.

Quem baixar daqui recebe o dado COMO A FONTE PUBLICA, sem nenhuma decisão
nossa embutida: nenhum recorte, nenhuma coluna derivada, nenhuma harmonização.
Só a conversão de formato e o particionamento. O dicionário vai junto, então
traduzir é possível sem voltar ao FTP.

ANO A ANO, COM LIMPEZA
----------------------
São 60 GB somando as duas bases, e o espelho local precisa conviver com o
MinIO no mesmo disco. Publicando um ano de cada vez e apagando o espelho
depois, o pico fica no tamanho de um ano.

Uso:
    python -m publicar_bronze --camada caged --repo Gianpedro/bronze_caged
    python -m publicar_bronze --camada rais  --repo Gianpedro/bronze_rais
    python -m publicar_bronze --camada rais  --repo ... --ano-inicio 2015
"""
import argparse
import os
import re
import shutil
import sys
import time
from pathlib import Path

# Antes de qualquer import do huggingface_hub: as barras por arquivo poluem o
# log (a saída vai para arquivo, não terminal) e escondem o relatório que o
# painel lê para montar a barra de progresso.
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")

from extracao_ftp.config_extracao import BUCKET_BRONZE
from silver_caged.publicar_hf import _credencial, _fs_minio

RAIZ = Path(__file__).resolve().parents[1] / "publicacao"

TABELAS = {
    "caged": ["caged_mov", "caged_for", "caged_exc", "caged_old", "caged_ajustes"],
    "rais": ["rais_estab", "rais_vinc"],
}

DESCRICAO = {
    "caged_mov": "Movimentações do Novo CAGED (2020+)",
    "caged_for": "Movimentações declaradas fora do prazo (Novo CAGED)",
    "caged_exc": "Exclusões de movimentações (Novo CAGED)",
    "caged_old": "CAGED antigo — CAGEDEST (2007–2019)",
    "caged_ajustes": "Ajustes e declarações fora do prazo (CAGED antigo)",
    "rais_estab": "Estabelecimentos declarantes (um registro por CNPJ/ano)",
    "rais_vinc": "Vínculos empregatícios (um registro por vínculo/ano)",
}

CARTAO = """---
license: odc-by
language:
  - pt
tags:
  - brazil
  - labor-market
  - microdata
  - {tag}
pretty_name: {titulo}
---

# {titulo}

Microdados **{sigla}** do Ministério do Trabalho e Emprego, convertidos de
`.7z` para **parquet particionado**, sem nenhuma outra alteração.

## O que este dataset é — e o que não é

**É** a fonte oficial em formato analisável. O MTE distribui `.7z` com texto de
largura fixa, encoding misto (UTF-8 no Novo CAGED, Latin-1 no antigo),
delimitador que varia entre arquivos e nomes de coluna com mojibake. Aqui isso
está resolvido: parquet, ZSTD, particionado, com o nome de coluna normalizado.

**Não é** interpretado. Os códigos continuam códigos: sexo é `1`, ocupação é um
CBO, setor é um CNAE. Nenhum recorte, nenhuma coluna derivada, nenhuma
harmonização entre períodos. As decisões analíticas são suas.

Para traduzir, use o `dicionarios.parquet` que acompanha — leia o aviso abaixo
antes.

## Estrutura

```
{{tabela}}/ano=YYYY/{particao}*.parquet
```

{tabela_arquivos}

Além das colunas da fonte, cada linha traz a linhagem: `arquivo_fonte`,
`caminho_fonte`, `data_ingestao` e as colunas de partição.

## Dicionário (`dicionarios.parquet`)

Formato longo: `tabela`, `coluna`, `codigo`, `descricao`, mais a procedência
(`planilha`, `aba`, `caminho_ftp`, `extraido_em`). Cada código diz de qual
planilha oficial do MTE ele saiu, com a URL do FTP.

```python
import duckdb
duckdb.sql("INSTALL httpfs; LOAD httpfs;")

duckdb.sql('''
    SELECT codigo, descricao, caminho_ftp
    FROM read_parquet('hf://datasets/{repo}/dicionarios.parquet')
    WHERE tabela = '{exemplo_tabela}' AND coluna = '{exemplo_coluna}'
''').show()
```

## ⚠️ Leia antes de traduzir

**O mesmo código significa coisas diferentes em períodos diferentes.** Não é
detalhe: é a armadilha que estraga a análise sem dar erro.

Em raça/cor, comparando os dicionários oficiais:

| código | Novo CAGED (2020+) | CAGED antigo / RAIS |
|---|---|---|
| `1` | Branca | **Indígena** |
| `2` | Preta | **Branca** |
| `4` | Amarela | **Preta** |
| `6` | Não informada | **Amarela** |

Em sexo, `2` é *Feminino* no antigo e **não existe** no Novo CAGED, que usa `3`
para Mulher.

Por isso o dicionário tem a coluna `tabela`: **filtre sempre por ela**. E ao
comparar períodos, compare pelas **descrições**, nunca pelos códigos.

O que É seguro juntar por código, conferido item a item: **CBO 2002**, **CNAE
2.0** e **município IBGE** — taxonomias oficiais, idênticas entre as bases.

Valores como `{{ñ`, `-1` e `0000` são marcadores de "não informado" da própria
fonte, e o MTE não os lista no dicionário: a descrição sai nula.

## Como consultar

Restrinja o glob ao período desejado — um glob aberto sobre a tabela inteira
lista centenas de pastas por API e leva a `HTTP 429`.

```python
duckdb.sql('''
    SELECT count(*) FROM read_parquet(
        'hf://datasets/{repo}/{exemplo_tabela}/ano=2019/**/*.parquet',
        hive_partitioning = true)
''').show()
```

Para vários anos, baixe antes com `huggingface_hub.snapshot_download`.

## Origem e licença

`ftp.mtps.gov.br/pdet/microdados` — dados públicos do PDET/MTE, distribuídos
sob **ODC-BY**. Cite a fonte original e este tratamento.
"""


def anos_de(fs, tabela: str) -> list[int]:
    anos = set()
    for c in fs.glob(f"{BUCKET_BRONZE}/{tabela}/**/*.parquet"):
        m = re.search(r"ano=(\d{4})", c)
        if m:
            anos.add(int(m.group(1)))
    return sorted(anos)


def espelhar_ano(fs, tabelas: list[str], ano: int, destino: Path) -> tuple[int, float]:
    """Baixa só as partições daquele ano. Devolve (arquivos, GB)."""
    fs.invalidate_cache()
    baixados, bytes_ = 0, 0
    for tabela in tabelas:
        achados = fs.find(f"{BUCKET_BRONZE}/{tabela}", detail=True)
        alvos = {k: v for k, v in achados.items()
                 if k.endswith(".parquet") and f"ano={ano}/" in k}
        if not alvos:
            continue
        print(f"   📥 PUBLICANDO: {tabela}  ({len(alvos)} arquivo(s) em {ano})")
        for n, (remoto, info) in enumerate(sorted(alvos.items()), start=1):
            local = destino / remoto.split(f"{BUCKET_BRONZE}/", 1)[1]
            tam = info.get("size", 0)
            if local.exists() and local.stat().st_size == tam:
                continue
            local.parent.mkdir(parents=True, exist_ok=True)
            fs.get(remoto, str(local))
            baixados += 1
            bytes_ += tam
            print(f"      [{n}/{len(alvos)}] ⬇️  {local.name}  ({tam / 1e6:.1f} MB)")
    return baixados, bytes_ / 1e9


def escrever_card(destino: Path, camada: str, repo: str, tabelas: list[str]) -> None:
    linhas = ["| Tabela | Conteúdo |", "|---|---|"]
    for t in tabelas:
        linhas.append(f"| `{t}/` | {DESCRICAO.get(t, '—')} |")

    exemplo = "caged_mov" if camada == "caged" else "rais_vinc"
    (destino / "README.md").write_text(
        CARTAO.format(
            tag="caged" if camada == "caged" else "rais",
            sigla="do CAGED" if camada == "caged" else "da RAIS",
            titulo=("CAGED — Microdados Brutos em Parquet" if camada == "caged"
                    else "RAIS — Microdados Brutos em Parquet"),
            particao="mes=M/" if camada == "caged" else "",
            tabela_arquivos="\n".join(linhas),
            repo=repo,
            exemplo_tabela=exemplo,
            exemplo_coluna="sexo" if camada == "caged" else "sexo_trabalhador",
        ),
        encoding="utf-8",
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Publica o bronze no Hugging Face, ano a ano.")
    p.add_argument("--camada", choices=("caged", "rais"), required=True)
    p.add_argument("--repo", required=True)
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    args = p.parse_args()

    destino = RAIZ / f"bronze_{args.camada}"
    destino.mkdir(parents=True, exist_ok=True)
    tabelas = TABELAS[args.camada]
    fs = _fs_minio()

    anos = sorted({a for t in tabelas for a in anos_de(fs, t)})
    anos = [a for a in anos if args.ano_inicio <= a <= args.ano_fim]
    if not anos:
        print("❌ Nenhum ano do bronze na faixa pedida.")
        return 1

    token = _credencial()
    from huggingface_hub import HfApi, get_token

    if not token and not get_token():
        print("❌ Nenhuma credencial do Hugging Face.")
        return 1
    api = HfApi(token=token)
    api.create_repo(repo_id=args.repo, repo_type="dataset", exist_ok=True)

    # Dicionário e card sobem uma vez, no começo: quem topar com o dataset no
    # meio da publicação já encontra a documentação e o de/para.
    from silver_caged.dimensoes import gerar

    gerar(args.camada, destino / "dicionarios.parquet")
    escrever_card(destino, args.camada, args.repo, tabelas)
    for nome in ("README.md", "dicionarios.parquet"):
        api.upload_file(path_or_fileobj=str(destino / nome), path_in_repo=nome,
                        repo_id=args.repo, repo_type="dataset")
    print(f"   📚 dicionário e card publicados\n")

    print(f"🔁 bronze {args.camada}: {len(anos)} ano(s) — {anos[0]} a {anos[-1]}")
    inicio, falhas = time.time(), []

    for n, ano in enumerate(anos, start=1):
        print(f"\n   [{n}/{len(anos)}] ano {ano}")
        try:
            arqs, gb = espelhar_ano(fs, tabelas, ano, destino)
            if not arqs:
                print(f"   ⏭️  {ano}: nada a enviar")
                continue
            print(f"   ⬆️  Enviando {arqs} arquivo(s) · {gb:.2f} GB")
            api.upload_large_folder(folder_path=str(destino), repo_id=args.repo,
                                    repo_type="dataset", print_report=True)
            # Limpa só as pastas do ano — o dicionário e o card ficam.
            for tabela in tabelas:
                pasta = destino / tabela / f"ano={ano}"
                if pasta.exists():
                    shutil.rmtree(pasta)
            print(f"   🧹 espelho local de {ano} removido ({gb:.2f} GB)")
        except Exception as e:
            falhas.append(ano)
            print(f"   ❌ {ano}: {str(e)[:200]}")

    print(f"\n🏁 {len(anos) - len(falhas)}/{len(anos)} ano(s) em "
          f"{(time.time() - inicio) / 60:.0f} min")
    if falhas:
        print(f"   ⚠️  anos com falha: {falhas}")
    print(f"   https://huggingface.co/datasets/{args.repo}")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
