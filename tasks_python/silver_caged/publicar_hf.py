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

# Desliga as barras de progresso por arquivo do huggingface_hub, ANTES de
# qualquer import dele.
#
# Elas redesenham com retorno de carro dezenas de vezes por segundo, e como a
# saída do job vai para um arquivo de log (não para um terminal), cada redesenho
# é gravado: um upload de 6 GB produzia mais de 6 MB de log em minutos. O
# relatório resumido que o painel usa para a barra de progresso sai só a cada
# 60 s e ficava soterrado nesse ruído.
#
# `print_report=True` no upload_large_folder continua valendo — é ele que
# imprime "Processing Files (a / b)", que é o que interessa.
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")

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

## Dimensões (`dicionarios.parquet`)

O fato já traz código **e** descrição lado a lado, então você não precisa deste
arquivo para ler os dados. Ele serve para três coisas: a lista **completa** de
códigos (inclusive os que não aparecem no período), conferir uma tradução
contra o de/para oficial, e trabalhar só com IDs se preferir.

Formato longo — `tabela`, `coluna`, `codigo`, `descricao`, mais a procedência:
`planilha`, `aba` e `caminho_ftp`. Toda linha diz de qual planilha oficial do
MTE ela saiu, então discordar de uma tradução é questão de baixar o arquivo
apontado e abrir a aba indicada:

```
codigo  descricao  aba                caminho_ftp
1       Homem      sexo               ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/Layout Não-identificado Novo Caged Movimentação.xlsx
1       MASCULINO  CAGESTID - layout  ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/CAGEDEST_layout_Atualizado.xls
```

O mesmo código `1` é *Homem* numa planilha e *MASCULINO* na outra — arquivos e
gerações diferentes. É por isso que a procedência acompanha cada linha.

Navegadores modernos removeram o suporte a FTP; para baixar, use o Explorador
de Arquivos do Windows, um cliente como o FileZilla (login anônimo), ou:

```bash
curl -O "ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/CAGEDEST_layout_Atualizado.xls"
```

Para consultar as dimensões, filtre por tabela e coluna:

```python
duckdb.sql('''
    SELECT codigo, descricao
    FROM read_parquet('hf://datasets/{repo}/dicionarios.parquet')
    WHERE tabela = 'caged_mov' AND coluna = 'cbo2002ocupacao'
''').show()
```

**Por que a coluna `tabela` importa.** As duas gerações do CAGED usam sistemas
de código **diferentes para o mesmo conceito**. Em raça/cor, o código `1` é
*Branca* no Novo CAGED e *Indígena* no CAGED antigo; `2` é *Preta* contra
*Branca*. Em sexo, `2` é *Feminino* no antigo e não existe no novo, enquanto
`3` é *Mulher* no novo e não existe no antigo.

Por isso não há um `dim_sexo` único: juntar sem filtrar por `tabela` produz
números plausíveis e errados, e o erro não aparece como falha.

Ao comparar os dois períodos, **compare pelas descrições**, nunca pelos códigos.

## Métrica principal

`saldomovimentacao` (Novo CAGED) e `saldo_mov` (CAGED antigo) valem **+1 na
admissão** e **−1 no desligamento**. A soma é a geração líquida de emprego
formal — a métrica-título do CAGED.

## Como consultar

**Restrinja o glob ao período que você quer.** O `hf://` resolve `**` listando
cada pasta de partição por uma chamada de API, e esta base tem centenas delas —
um glob aberto sobre a tabela inteira leva a `HTTP 429 (rate limit)` antes de
ler qualquer dado. Isso é limite de *listagem*, não dos arquivos.

Um ano (o caso comum — o glob cobre 12 pastas):

```python
import duckdb

duckdb.sql("INSTALL httpfs; LOAD httpfs;")
duckdb.sql('''
    SELECT uf_descricao,
           count(*) FILTER (WHERE saldomovimentacao = 1) AS admissoes,
           sum(saldomovimentacao)                        AS saldo
    FROM read_parquet(
        'hf://datasets/{repo}/caged_mov/ano_particao=2025/**/*.parquet',
        hive_partitioning = true
    )
    GROUP BY 1 ORDER BY saldo DESC
''').show()
```

Um mês específico, sem listagem nenhuma — aponte o arquivo direto:

```python
BASE = "https://huggingface.co/datasets/{repo}/resolve/main"

duckdb.sql(f'''
    SELECT * FROM read_parquet(
        '{{BASE}}/caged_old/ano_particao=2019/mes_particao=6/caged_old_201906_0.parquet'
    ) LIMIT 10
''').show()
```

Vários anos ou a base inteira: **baixe primeiro**, é mais rápido e não esbarra
em rate limit.

```python
from huggingface_hub import snapshot_download

caminho = snapshot_download(
    repo_id="{repo}", repo_type="dataset",
    allow_patterns="caged_mov/*",      # ou "*" para tudo
)
duckdb.sql(f"SELECT count(*) FROM read_parquet('{{caminho}}/caged_mov/**/*.parquet', hive_partitioning=true)")
```

Em todos os casos o filtro por `ano_particao` / `mes_particao` é resolvido pelo
nome das pastas: as partições fora do filtro não são baixadas.

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

        # "PUBLICANDO: <tabela>" e "[n/N]" não são enfeite: é o formato que o
        # painel lê para montar a barra de progressão do job (ver
        # painel/processos.py:_progresso). O log é a única fonte de progresso,
        # então ele precisa falar essa língua.
        print(f"   📥 PUBLICANDO: {tabela}  ({len(arquivos)} arquivo(s))")
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
            # Uma linha por arquivo BAIXADO; os pulados ficam mudos, igual ao
            # construtor da silver. É o que faz a estimativa de tempo do painel
            # medir a velocidade real desta execução, e não contar como
            # trabalho os arquivos que já estavam no espelho.
            print(f"      [{n}/{len(arquivos)}] ⬇️  {local.name}  "
                  f"({tamanho / 1e6:.1f} MB)")

    return baixados, pulados


def _tabela_de_arquivos(destino: Path, tabelas: list[str]) -> str:
    """
    Monta a tabela do card a partir do que está REALMENTE no espelho.

    Fixar a lista no texto faria o card prometer tabelas ausentes numa
    publicação parcial — e ela é parcial por natureza enquanto alguma silver
    ainda está sendo construída.
    """
    linhas = ["| Tabela | Conteúdo | Período | Arquivos | Tamanho |",
              "|---|---|---|---|---|"]
    avisos = []

    for tabela in tabelas:
        pasta = destino / tabela
        if not pasta.exists():
            continue
        arquivos = list(pasta.rglob("*.parquet"))
        if not arquivos:
            continue

        tam = sum(a.stat().st_size for a in arquivos)
        periodo, faltando = _cobertura_anual(pasta)
        marca = " ⚠️" if faltando else ""
        linhas.append(f"| `{tabela}/` | {DESCRICAO_TABELA.get(tabela, '—')} | "
                      f"{periodo}{marca} | {len(arquivos)} | {tam / 1e9:.2f} GB |")

        # Lacuna no meio da série precisa ser declarada, não descoberta: quem
        # somar o período obteria um total menor que o real sem nenhum sinal.
        if faltando:
            lista = ", ".join(faltando[:12]) + (" …" if len(faltando) > 12 else "")
            avisos.append(f"- `{tabela}` — {len(faltando)} competência(s) ausente(s) "
                          f"dentro do período: {lista}")

    if avisos:
        linhas.append("")
        linhas.append("> ⚠️ **Lacunas na série**")
        linhas.append(">")
        linhas += [f"> {a}" for a in avisos]
        linhas.append(">")
        linhas.append("> O período indicado é o que está publicado; o último mês de "
                      "cada tabela avança conforme novas cargas entram.")

    return "\n".join(linhas)


def _cobertura_anual(pasta: Path) -> tuple[str, list[str]]:
    """
    Período coberto e as competências que faltam NO MEIO dele.

    A partição hive é a fonte da verdade: as competências saem das próprias
    pastas ano_particao=/mes_particao=.

    A regra é a mesma já usada no painel (painel/app.py:_cobertura), e a parte
    que importa é o "no meio". Contar "menos de 12 meses no ano = parcial"
    parece razoável e está errado: acusaria as BORDAS naturais da série como
    buraco — o Novo CAGED começa em fevereiro de 2020, e o último ano publicado
    está sempre incompleto porque o ano ainda não acabou. Os dois virariam
    alarme falso num card público, que é pior do que não avisar.

    Buraco de verdade é competência ausente ENTRE a primeira e a última — aí
    sim quem somar o período obtém um total menor sem perceber.

    Partições sem mês numérico (mes_particao=__HIVE_DEFAULT_PARTITION__) são as
    safras anuais do caged_ajustes, anuais já na origem: entram no período, não
    na checagem de lacuna mensal.
    """
    competencias: set[tuple[int, int]] = set()
    anos_sem_mes: set[int] = set()

    for arquivo in pasta.rglob("*.parquet"):
        partes = {p.split("=")[0]: p.split("=")[1] for p in arquivo.parts if "=" in p}
        ano = partes.get("ano_particao", "")
        if not ano.isdigit():
            continue
        mes = partes.get("mes_particao", "")
        if mes.isdigit():
            competencias.add((int(ano), int(mes)))
        else:
            anos_sem_mes.add(int(ano))

    if not competencias:
        if not anos_sem_mes:
            return "—", []
        anos = sorted(anos_sem_mes)
        return (f"{anos[0]}–{anos[-1]}" if anos[0] != anos[-1] else str(anos[0])), []

    ordenadas = sorted(competencias)
    inicio, fim = ordenadas[0], ordenadas[-1]
    esperadas = {
        (a, m)
        for a in range(inicio[0], fim[0] + 1)
        for m in range(1, 13)
        if inicio <= (a, m) <= fim
    }
    faltando = [f"{a}-{m:02d}" for a, m in sorted(esperadas - competencias)]

    todos_anos = sorted({a for a, _ in competencias} | anos_sem_mes)
    periodo = f"{todos_anos[0]}-{inicio[1]:02d} → {todos_anos[-1]}-{fim[1]:02d}"
    return periodo, faltando


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
    p.add_argument("--so-card", action="store_true",
                   help="Regera e envia apenas o README do dataset, sem tocar nos parquets")
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
              .replace("{tabela_arquivos}", _tabela_de_arquivos(DIR_LOCAL, list(mp.TODAS_TABELAS))),
        encoding="utf-8",
    )

    if args.so_card:
        # Corrigir uma frase do card não deveria custar uma varredura de 12 GB:
        # o upload_large_folder reexamina a pasta inteira antes de decidir o que
        # enviar. Um upload_file resolve em segundos.
        #
        # As dimensões vão junto porque são metadado do mesmo tipo — algumas
        # centenas de KB que mudam quando o card muda, e que ficariam órfãs se
        # dependessem do envio pesado.
        leves = ["README.md"]
        if (DIR_LOCAL / "dicionarios.parquet").exists():
            leves.append("dicionarios.parquet")

        for nome in leves:
            print(f"   ⬆️  {nome}")
            api.upload_file(
                path_or_fileobj=str(DIR_LOCAL / nome),
                path_in_repo=nome,
                repo_id=args.repo,
                repo_type="dataset",
                commit_message=f"Atualiza {nome}",
            )
        print(f"\n🏁 Metadados atualizados: https://huggingface.co/datasets/{args.repo}")
        return 0

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
