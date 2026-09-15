---
license: odc-by
language:
  - pt
tags:
  - brazil
  - labor-market
  - microdata
  - caged
pretty_name: CAGED — Microdados Brutos em Parquet
---

# CAGED — Microdados Brutos em Parquet

Microdados **do CAGED** do Ministério do Trabalho e Emprego, convertidos de
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
{tabela}/ano=YYYY/mes=M/*.parquet
```

| Tabela | Conteúdo |
|---|---|
| `caged_mov/` | Movimentações do Novo CAGED (2020+) |
| `caged_for/` | Movimentações declaradas fora do prazo (Novo CAGED) |
| `caged_exc/` | Exclusões de movimentações (Novo CAGED) |
| `caged_old/` | CAGED antigo — CAGEDEST (2007–2019) |
| `caged_ajustes/` | Ajustes e declarações fora do prazo (CAGED antigo) |

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
    FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/dicionarios.parquet')
    WHERE tabela = 'caged_mov' AND coluna = 'sexo'
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

Valores como `{ñ`, `-1` e `0000` são marcadores de "não informado" da própria
fonte, e o MTE não os lista no dicionário: a descrição sai nula.

## Como consultar

Restrinja o glob ao período desejado — um glob aberto sobre a tabela inteira
lista centenas de pastas por API e leva a `HTTP 429`.

```python
duckdb.sql('''
    SELECT count(*) FROM read_parquet(
        'hf://datasets/Gianpedro/bronze_caged/caged_mov/ano=2019/**/*.parquet',
        hive_partitioning = true)
''').show()
```

Para vários anos, baixe antes com `huggingface_hub.snapshot_download`.

## Origem e licença

`ftp.mtps.gov.br/pdet/microdados` — dados públicos do PDET/MTE, distribuídos
sob **ODC-BY**. Cite a fonte original e este tratamento.
