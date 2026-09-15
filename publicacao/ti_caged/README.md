---
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
> [`Gianpedro/caged-tecnologia`](https://huggingface.co/datasets/Gianpedro/caged-tecnologia) — não some os dois, as linhas de TI estão nos dois.

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

| Tabela | Conteúdo | Período | Arquivos | Tamanho |
|---|---|---|---|---|
| `caged_mov/` | Movimentações do Novo CAGED (2020+) | 2020-01 → 2026-06 | 156 | 0.21 GB |
| `caged_for/` | Movimentações declaradas fora do prazo (Novo CAGED) | 2020-02 → 2026-06 | 154 | 0.01 GB |
| `caged_exc/` | Exclusões de movimentações (Novo CAGED) | 2020-04 → 2026-06 | 150 | 0.00 GB |
| `caged_old/` | CAGED antigo — CAGEDEST (2007–2019) | 2007-01 → 2019-12 | 324 | 0.22 GB |
| `caged_ajustes/` | Ajustes e declarações fora do prazo (CAGED antigo) | 2003-01 → 2019-12 | 204 | 0.01 GB |

`mes_particao=__HIVE_DEFAULT_PARTITION__` aparece em `caged_ajustes`: os
arquivos de 2002 a 2009 são anuais na fonte, sem competência mensal.

## Dimensões (`dicionarios.parquet`)

O fato já traz código **e** descrição lado a lado, então você não precisa deste
arquivo para ler os dados. Ele serve para três coisas: a lista **completa** de
códigos (inclusive os que não aparecem no período), conferir uma tradução
contra o de/para oficial, e trabalhar só com IDs se preferir.

Formato longo — `tabela`, `coluna`, `codigo`, `descricao`, mais a procedência:
`planilha`, `aba`, `caminho_ftp` e `extraido_em` (o MTE revisa as planilhas de
layout; a data diz de qual versão este de/para saiu). Toda linha diz de qual planilha oficial do
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
    FROM read_parquet('hf://datasets/Gianpedro/caged-tecnologia/dicionarios.parquet')
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
        'hf://datasets/Gianpedro/caged-tecnologia/caged_mov/ano_particao=2025/**/*.parquet',
        hive_partitioning = true
    )
    GROUP BY 1 ORDER BY saldo DESC
''').show()
```

Um mês específico, sem listagem nenhuma — aponte o arquivo direto:

```python
BASE = "https://huggingface.co/datasets/Gianpedro/caged-tecnologia/resolve/main"

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
    repo_id="Gianpedro/caged-tecnologia", repo_type="dataset",
    allow_patterns="caged_mov/*",      # ou "*" para tudo
)
duckdb.sql(f"SELECT count(*) FROM read_parquet('{{caminho}}/caged_mov/**/*.parquet', hive_partitioning=true)")
```

Em todos os casos o filtro por `ano_particao` / `mes_particao` é resolvido pelo
nome das pastas: as partições fora do filtro não são baixadas.

## Licença e citação

Os dados originais são públicos (MTE/PDET). Este derivado é distribuído sob
**ODC-BY**: cite a fonte original e este tratamento.
