---
license: odc-by
language:
  - pt
tags:
  - brazil
  - labor-market
  - rais
  - microdata
pretty_name: RAIS — Microdados Traduzidos (mercado completo)
size_categories:
  - 100M<n<1B
---

# RAIS — Microdados Traduzidos (Brasil, mercado completo)

Microdados da **RAIS** (Relação Anual de Informações Sociais, Ministério do
Trabalho e Emprego) com os **códigos traduzidos** pelos dicionários oficiais do
próprio MTE.

Traduzir a RAIS é mais trabalhoso que o CAGED: o de/para está espalhado por
**21 planilhas de layout**, uma por período, e várias têm abas de mesmo nome.
Aqui esse cruzamento já está feito, com a origem de cada código registrada.

> **RAIS não é CAGED.** A RAIS é uma declaração **anual** — uma foto do vínculo
> em 31 de dezembro. O CAGED é o **fluxo** de admissões e desligamentos ao longo
> do mês. Para o fluxo, veja [`Gianpedro/caged-microdados-traduzidos`](https://huggingface.co/datasets/Gianpedro/caged-microdados-traduzidos).

## Origem

`ftp.mtps.gov.br/pdet/microdados/RAIS` — dados públicos do PDET/MTE.
Recorte publicado: **2007 em diante**, quando já vigoravam CNAE 2.0 e CBO 2002.
Antes disso a fonte usa CNAE 1.0 e CBO 1994, taxonomias diferentes que não são
comparáveis sem harmonização.

## Tratamento aplicado

1. **Códigos traduzidos** pelos dicionários oficiais do MTE: cada coluna
   codificada ganhou uma `<coluna>_descricao` legível, **mantendo o código
   original ao lado**.
2. **Encoding detectado por arquivo** — o acervo mistura UTF-8 e Latin-1.
3. **Campos numéricos tipados**: a fonte usa vírgula decimal, então
   remuneração, tempo de emprego e idade vêm como número, não texto.

## Estrutura

Particionado em Hive por ano:

```
{{tabela}}/ano_particao=YYYY/*.parquet
```

Sem `mes_particao`: a RAIS não tem competência mensal. Em `rais_vinc` há
**vários arquivos por ano**, um por região da fonte.

| Tabela | Conteúdo | Período | Arquivos | Tamanho |
|---|---|---|---|---|
| `rais_estab/` | Estabelecimentos declarantes da RAIS (um registro por CNPJ/ano) | 2007–2025 | 62 | 0.03 GB |
| `rais_vinc/` | Vínculos empregatícios da RAIS (um registro por vínculo/ano) | 2007–2025 | 678 | 1.18 GB |

## Métricas principais

- `vinculo_ativo_3112` — vínculo ativo em 31/12. É o que se soma para obter o
  **estoque** de empregos formais, e a diferença entre dois anos é o saldo.
- `vl_remun_media_sm` / `vl_remun_dezembro_sm` — remuneração em **salários
  mínimos**, que permite comparar anos sem deflacionar.
- `vl_remun_media_nom` — remuneração nominal em reais **da época**; para série
  temporal, deflacione ou use a versão em salários mínimos.
- `tempo_emprego` — em meses.

`rais_estab` traz um registro por estabelecimento e **não tem ocupação**: uma
empresa não exerce CBO. Recortes por ocupação só são possíveis em `rais_vinc`.

## Dimensões (`dicionarios.parquet`)

Mesmo formato do dataset do CAGED: `tabela`, `coluna`, `codigo`, `descricao`,
mais a procedência (`planilha`, `aba`, `caminho_ftp`, `extraido_em`). Cada
código diz de qual das 21 planilhas de layout ele saiu — inclusive a subpasta,
`estabelecimento/` ou `vínculos/`.

```python
duckdb.sql('''
    SELECT codigo, descricao, caminho_ftp
    FROM read_parquet('hf://datasets/Gianpedro/rais-tecnologia/dicionarios.parquet')
    WHERE tabela = 'rais_vinc' AND coluna = 'escolaridade_apos_2005'
''').show()
```

## Como consultar

**Restrinja o glob ao período que você quer.** O `hf://` resolve `**` listando
cada pasta de partição por uma chamada de API; um glob aberto sobre a tabela
inteira leva a `HTTP 429 (rate limit)` antes de ler qualquer dado.

```python
import duckdb

duckdb.sql("INSTALL httpfs; LOAD httpfs;")
duckdb.sql('''
    SELECT cnae_20_subclasse_descricao AS setor,
           sum(qtd_vinculos_ativos)    AS vinculos
    FROM read_parquet(
        'hf://datasets/Gianpedro/rais-tecnologia/rais_estab/ano_particao=2019/*.parquet',
        hive_partitioning = true
    )
    GROUP BY 1 ORDER BY vinculos DESC LIMIT 20
''').show()
```

Para vários anos ou a base inteira, baixe antes com `snapshot_download` — é
mais rápido e não esbarra em rate limit.

## Licença e citação

Os dados originais são públicos (MTE/PDET). Este derivado é distribuído sob
**ODC-BY**: cite a fonte original e este tratamento.
