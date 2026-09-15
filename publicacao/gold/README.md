---
license: odc-by
language: [pt]
tags: [brazil, labor-market, technology, caged, rais, gold]
---

# Mercado de trabalho em tecnologia — camada gold

Agregados prontos para consumo, derivados dos microdados públicos do **CAGED**
e da **RAIS** (Ministério do Trabalho e Emprego), recortados em tecnologia por
setor (CNAE) **ou** ocupação (CBO).

É a camada que alimenta o dashboard. São os mesmos números da camada silver, já
agregados: 3 MB no lugar de 110 MB por arquivo, com a mesma resposta.

## O que tem aqui

| grupo | tabelas |
|---|---|
| **Estoque (RAIS)** | `rais_estoque_anual`, `rais_estoque_uf`, `rais_estoque_area`, `rais_estoque_perfil`, `rais_remuneracao_ocupacao`, `rais_estoque_municipio`, `rais_setor_vs_ocupacao`, `rais_estabelecimentos` |
| **Território (as duas bases)** | `mapa_uf`, `mapa_municipio`, `geo_municipios` |
| **Modelos** | `previsao_saldo`, `previsao_placar`, `serie_mensal`, `nowcast_*`, `sobrevivencia_*`, `hiato_serie`, `municipios_cluster` |

## Como ler

```python
import duckdb
base = "https://huggingface.co/datasets/Gianpedro/mercado-ti-gold/resolve/main"
duckdb.sql(f"SELECT * FROM read_parquet('{{base}}/rais_estoque_anual.parquet')")
```

## Ressalvas que acompanham o dado

**Localização é a do estabelecimento, não a do trabalho.** A empresa de TI
escolhe onde se registrar por causa do ISS. Barueri aparece com 200 vínculos de
TI por mil habitantes; Guaraciaba (MG), com 10 mil habitantes, saiu de 1 vínculo
em 2022 para 1.428 em 2023. A coluna da RAIS que resolveria isso, `mun_trab`,
vem como "não informado" em praticamente todos os registros.

**A remuneração tem teto de plausibilidade.** A partir de 2023 a fonte traz
registros com a conversão para salário mínimo invertida — multiplicada em vez de
dividida. São 0,018% dos vínculos, e eles sozinhos respondiam por 35% da massa
salarial. Sem o corte em 500 SM, a média saltaria de 6,17 para 8,84 entre 2022 e
2023, o que se leria como valorização de 43% que não houve.

**A parte não explicada do hiato salarial não é medida de discriminação.** Ela
contém tudo que afeta salário e não está no modelo. O que se afirma é o limite
superior da diferença de retorno.

**O estoque é `vinculo_ativo_3112 = SIM`** — vínculos ativos em 31/12. Os que
existiram e terminaram durante o ano entram como `desligados_no_ano`, não no
estoque.

## Fontes

- CAGED e RAIS: FTP público do MTE (`ftp.mtps.gov.br/pdet/microdados`)
- Coordenadas e população dos municípios: IBGE
