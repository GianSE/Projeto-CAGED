# Extração automatizada CAGED / RAIS (FTP do MTE → MinIO)

Pipeline que baixa os microdados do PDET/MTE direto do FTP, descompacta,
trata e grava em parquet ZSTD-3 no MinIO — um arquivo por vez, apagando
o `.7z` e os extraídos logo em seguida para não encher o disco.

```
FTP (.7z)  ──▶  staging local  ──▶  DuckDB (trata)  ──▶  s3://bronze/*.parquet
                     │                                          │
                     └────────── apagado na hora ◀──────────────┘
```

## Fonte

`ftp.mtps.gov.br/pdet/microdados` — acesso anônimo, sem credencial.

O servidor devolve nomes em **Latin-1** (por isso `ftp.encoding = "latin-1"`;
sem isso o `ftplib` estoura em pastas como `Movimentações`).

## Mapa dos dados

| Caminho no FTP | Padrão do arquivo | Tabela destino | Período |
|---|---|---|---|
| `NOVO CAGED/{ano}/{anomes}/` | `CAGED{MOV,FOR,EXC}{anomes}.7z` | `caged_mov`, `caged_for`, `caged_exc` | 2020– |
| `CAGED/{ano}/` | `CAGEDEST_{MM}{AAAA}.7z` | `caged_old` | 2007–2019 |
| `CAGED_AJUSTES/{ano}/` | `CAGEDEST_AJUSTES_{MM}{AAAA}.7z` | `caged_ajustes` | 2010–2019 |
| `CAGED_AJUSTES/2002a2009/` | `CAGEDEST_AJUSTES_{AAAA}.7z` | `caged_ajustes` | 2002–2009 |
| `RAIS/{ano}/` | `RAIS_VINC_PUB_{REGIÃO}.7z` | `rais_vinc` | 2018– |
| `RAIS/{ano}/` | `RAIS_ESTAB_PUB.7z` | `rais_estab` | 2018– |
| `RAIS/{ano}/` | `{UF}{ano}.7z` | `rais_vinc` | 1985–2017 |
| `RAIS/{ano}/` | `ESTB{ano}.7z` | `rais_estab` | 1985–2017 |
| `RAIS/{ano}/` | `IGNORADO{ano}.7z` | `rais_vinc` (recorte `ignorado`) | 1985–1997 |

Os `IGNORADO*` são vínculos sem UF identificada — o equivalente antigo do
`RAIS_VINC_PUB_NI`. A grafia varia na própria fonte (`IGNORANDOS1985`,
`IGNORADOS1986`, `IGNORADO1988`), por isso o regex tolera as três formas.

A classificação é por **regex no nome do arquivo**, não por caminho fixo — os
anos de transição (2018/2019) convivem com os dois padrões, e assim nenhum
dos dois quebra.

## Layout no MinIO

```
s3://bronze/caged_mov/ano=2025/mes=3/caged_mov_202503.parquet
s3://bronze/caged_old/ano=2019/mes=3/caged_old_201903.parquet
s3://bronze/rais_vinc/ano=2024/rais_vinc_sp.parquet
s3://bronze/rais_estab/ano=2024/rais_estab_2024.parquet
```

Particionamento Hive (`ano=`, `mes=`), que o DuckDB e o Polars leem direto:

```sql
SELECT * FROM read_parquet('s3://bronze/caged_mov/**/*.parquet', hive_partitioning=1)
WHERE ano = 2025;
```

## Tratamento aplicado na bronze

1. `all_varchar` — bronze não adivinha tipo, preserva o dado cru
2. Nomes de coluna normalizados (snake_case, sem acento)
3. `TRIM` em todos os campos — a RAIS vem com padding de espaços (hex 20)
4. String vazia → `NULL`
5. Colunas de linhagem: `ano_particao`, `mes_particao`, `recorte_particao`,
   `arquivo_fonte`, `caminho_fonte`, `data_ingestao`

O `TRIM` roda em SQL, não em Python: os arquivos da RAIS passam de 10 GB
descompactados e o laço linha a linha levaria horas.

### Encoding: detectado por arquivo, nunca assumido

A fonte **não é uniforme**. Conferido nos bytes crus:

```
CAGEDEXC202601.txt   b'compet\xc3\xaanciamov'    -> UTF-8    (Novo CAGED, 2020+)
CAGEDEST_012019.txt  b'Compet\xeancia Declarada' -> Latin-1  (CAGED antigo)
```

Assumir Latin-1 para tudo é o erro perigoso: o parser **não acusa nada** e grava
mojibake em silêncio — `competência` vira `competÃªncia`, e as colunas saem como
`competaanciamov`, `municapio`, `seaao`. Por isso o encoding é farejado em cada
arquivo antes da leitura.

Se ainda assim a leitura falhar (alguns arquivos da RAIS têm null bytes que
quebram o parser), o transformador higieniza fisicamente o arquivo — usando o
encoding detectado — e tenta de novo. O caminho caro só é pago quando necessário.

## Uso

O MinIO roda em container (`docker compose -f minio/docker-compose.yml up -d`),
mas a extração em si roda no host, via `.venv` — sem container nenhum e sem
exigir 7-Zip instalado (na falta do binário `7z`, o `extrator.py` cai para o
`py7zr`, puro Python).

```bash
python -m venv .venv
.venv\Scripts\pip install -r requirements-pipeline.txt
```

A partir da pasta `tasks_python`:

```bash
# Dry-run: mostra o plano sem baixar nada
..\.venv\Scripts\python -m extracao_ftp.run_extracao --listar --ano-inicio 1985

# Escopo moderno (Novo CAGED + RAIS 2020+)
..\.venv\Scripts\python -m extracao_ftp.run_extracao --dataset novo_caged rais --ano-inicio 2020

# Tudo que existe no FTP (~66 GB compactado, 1985–2026)
..\.venv\Scripts\python -m extracao_ftp.run_extracao --ano-inicio 1985

# Testar com poucos itens antes de soltar a carga inteira
..\.venv\Scripts\python -m extracao_ftp.run_extracao --tabela caged_mov --ano-inicio 2025 --limite 2
```

### Opções

| Flag | Efeito |
|---|---|
| `--dataset` | `novo_caged`, `caged`, `caged_ajustes`, `rais` |
| `--tabela` | Filtra o destino (`caged_mov`, `rais_vinc`, …) |
| `--ano-inicio` / `--ano-fim` | Faixa de ano-base |
| `--incluir-parcial` | Inclui as pastas `AAAA Parcial` da RAIS (ignoradas por padrão) |
| `--listar` | Dry-run |
| `--forcar` | Reprocessa o que já existe no MinIO |
| `--limite N` | Processa no máximo N itens |
| `--manter-temp` | Não apaga os temporários (debug — consome muito disco) |
| `--dicionarios` | Extrai também as planilhas de layout junto com os microdados |
| `--so-dicionarios` | Extrai só as planilhas de layout e sai |

## Dicionários (tradução dos códigos)

Os microdados são quase todos códigos: `graudeinstrucao = 7`, `racacor = 3`,
`categoria = 101`. As planilhas de layout do MTE trazem as tabelas de/para.

```bash
python -m extracao_ftp.run_extracao --so-dicionarios
```

Gera 27 planilhas / 293 abas:

```
s3://bronze/_layouts/...                          # planilha original, para conferir
s3://bronze/dicionarios/novo_caged/racacor.parquet
s3://bronze/dicionarios/novo_caged/graudeinstrucao.parquet
s3://bronze/dicionarios/novo_caged/cbo2002ocupacao.parquet
s3://bronze/dicionarios/caged/municipio.parquet
s3://bronze/dicionarios/rais_layouts/...
```

Cada aba vira parquet **sem interpretação** (`col_00`, `col_01`, …, mais
`aba_origem` e `planilha_origem`): o cabeçalho fica na primeira linha, porque
cada planilha começa numa linha diferente e adivinhar erraria. Montar o de/para
final é trabalho da silver:

```sql
SELECT col_00 AS codigo, col_01 AS descricao
FROM read_parquet('s3://bronze/dicionarios/novo_caged/racacor.parquet')
WHERE col_00 <> 'Código';
```

## Retomada

A carga é **idempotente e interrompível**:

- Antes de processar, consulta o MinIO (`head_object`) e pula o que já existe.
- Download usa `REST`, então um `.7z` interrompido continua de onde parou.
- Cada item processado é registrado em `{STAGING_DIR}/logs/manifesto_extracao.csv`
  com linhas gravadas, tempo e status — trilha de auditoria da ingestão.

Pode matar o processo a qualquer momento e rodar o mesmo comando de novo.
