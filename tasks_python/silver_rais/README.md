# Silver da RAIS — bronze traduzido pelos dicionários

Mesmo princípio do `silver_caged`: LEFT JOIN nos dicionários
(`bronze/dicionarios/rais_layouts/*`), coluna `<coluna>_descricao` nova ao
lado do código original, ZSTD-3, partição Hive.

## Diferença central em relação ao CAGED: mapeamento ainda não confirmado

O CAGED antigo teve seu mapeamento (`MAPA_CAGED_ANTIGO` em
`silver_caged/mapeamento.py`) escrito **depois** de inspecionar o bronze real
— cada nome de coluna foi conferido antes de entrar no mapa. A RAIS ainda não
tem bronze (a extração — 991 arquivos, ~58 GB compactado — não rodou por
questão de tempo/recurso da máquina), então `silver_rais/mapeamento.py` só
tem o **casamento automático por nome** (mesmo truque que já funciona hoje
para `caged_mov/for/exc`): se a coluna do bronze se chamar exatamente igual a
uma aba do dicionário (`bronze/dicionarios/rais_layouts/{col}.parquet`), ela
é traduzida sozinha.

Isso é seguro (LEFT JOIN — na pior hipótese, uma coluna fica sem tradução,
nunca traduz errado) mas incompleto: nomes como `escolaridade_ou_g_instrucao`
ou `faixas` dificilmente batem com o nome real da coluna no bronze sem
normalização adicional.

### Como completar depois que o bronze existir

```bash
python -m silver_rais.construir_silver --listar
```

Mostra toda coluna do bronze e se casou ou não. Para as que não casaram,
adicione a entrada em `MAPA_MANUAL` (mesmo padrão do CAGED antigo) **depois
de conferir o nome de verdade** — nunca adivinhar. Os estilos disponíveis
(implementados em `silver_caged/dicionarios.py`, reusado aqui):

| Estilo | Uso na RAIS |
|---|---|
| `colon` | abas de uma coluna só (`municipio`, `ocupacao`, `classe_1_0_ou_95`, `subclasse_2_0`, bairros/distritos) |
| `titulo_codigo` | abas com descrição numa coluna e código em outra (`escolaridade_ou_g_instrucao`; `faixas`, que empilha faixa etária e faixa de remuneração lado a lado — precisa de duas entradas, uma por par de colunas) |

## Particionamento: só por ano

A RAIS não tem competência mensal como o CAGED — cada arquivo é o ano
inteiro (`rais_estab`) ou ano + UF/região (`rais_vinc`). A silver particiona
só por `ano_particao` (`PARTITION_BY (ano_particao)`), diferente do CAGED
(`ano_particao, mes_particao`).

## Robustez herdada dos scripts antigos (`bronze_rais/*`, `auditoria_rais/*`)

Três lições dos scripts antigos do projeto já foram incorporadas na
extração (`extracao_ftp/`), não só na silver:

1. **Delimitador não é sempre `;`** — `bronze_rais/correcao_7z.py` documenta
   RAIS com `,`. `transformador.py` agora detecta por arquivo
   (`detectar_delimitador`), em vez de fixar `;` como no CAGED.
2. **Corrupção de verdade em alguns `.7z`** — o mesmo 7-Zip tolerante a CRC
   usado no CAGED (ver `extracao_ftp/extrator.py`) se aplica aqui.
3. **Resgate via stream** — quando nem a extração tolerante recupera nada,
   `extrator.py` cai para `_extrair_stream_forcado` (baseado em
   `bronze_rais/resgate_total_stream.py`): lê o `.7z` como fluxo contínuo
   (`7z e -so`) tentando forçar cada formato conhecido, e trunca a última
   linha (provavelmente cortada pela metade).

`eras_rais.py` (em `auditoria_rais/`) também documenta que a RAIS teve
**várias arquiteturas de coluna diferentes ao longo do tempo** (não é só
encoding) — algo a ter em mente ao estender `MAPA_MANUAL`: o mapeamento de
uma era pode não valer para outra.

## Uso

```bash
cd tasks_python

# 1. Extrair (pesado — 58 GB, rodar sozinho, sem outro job pesado concorrente)
../.venv/Scripts/python -m extracao_ftp.run_extracao --dataset rais --ano-inicio 1985

# 2. Depois de ter bronze, ver o que traduziu automaticamente
../.venv/Scripts/python -m silver_rais.construir_silver --listar

# 3. Construir
../.venv/Scripts/python -m silver_rais.construir_silver --tabela rais_estab rais_vinc
```
