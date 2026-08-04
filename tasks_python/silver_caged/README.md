# Silver do CAGED — bronze traduzido pelos dicionários

Constrói `s3://silver/{tabela}` a partir de `s3://bronze/{tabela}` +
`s3://bronze/dicionarios/...`: mesmas colunas do bronze, mais uma
`<coluna>_descricao` para cada código com dicionário disponível, campos
numéricos tipados (a fonte usa vírgula decimal: `"000005,10"`) e a competência
AAAAMM convertida em `<coluna>_data` (DATE).

O código original **nunca é substituído** — a tradução entra como coluna nova,
então dá pra agrupar tanto pelo código quanto pela descrição.

## Duas gerações, dois dicionários

| | Novo CAGED (`caged_mov/for/exc`, 2020+) | CAGED antigo (`caged_old`, `caged_ajustes`, 2002–2019) |
|---|---|---|
| Dicionário | `bronze/dicionarios/novo_caged/*` | `bronze/dicionarios/caged/*` |
| Formato | 2 colunas (Código, Descrição) | 1 coluna `"código:descrição"` |
| Mapeamento coluna→aba | **automático** (nome da coluna = nome da aba) | manual, em `mapeamento.py` (nomes não batem: `cnae_20_subclas` → aba `subclasse`) |

No CAGED antigo, nem todo código tem dicionário tabular publicado — `sexo`,
`raca_cor`, `grau_instrucao`, `tipo_mov_desagregado` etc. só aparecem em texto
livre dentro da planilha de layout (`cagestid_layout`), não numa tabela
código→descrição extraível automaticamente. Esses ficam **sem tradução** na
silver por enquanto (o código continua lá, intacto). O que tem dicionário
tabular (município, CBO, CNAE) é traduzido normalmente.

## Uso

```bash
cd tasks_python

# Ver o que seria traduzido, sem gravar nada
../.venv/Scripts/python -m silver_caged.construir_silver --listar

# Construir tudo
../.venv/Scripts/python -m silver_caged.construir_silver

# Só o Novo CAGED, ou só um recorte de anos
../.venv/Scripts/python -m silver_caged.construir_silver --tabela caged_mov caged_for caged_exc
../.venv/Scripts/python -m silver_caged.construir_silver --tabela caged_old --ano-inicio 2015
```

Cada execução reconstrói a tabela inteira (não é incremental) — o volume do
CAGED cabe inteiro numa passada do DuckDB, então é mais simples do que
controlar reprocessamento por partição.

## Exemplo de leitura

```sql
SELECT uf_descricao, sexo_descricao, racacor_descricao, count(*) 
FROM read_parquet('s3://silver/caged_mov/**/*.parquet', hive_partitioning=1)
WHERE ano_particao = 2025
GROUP BY 1, 2, 3;
```
