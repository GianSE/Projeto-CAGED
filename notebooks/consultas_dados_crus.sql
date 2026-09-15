-- =============================================================================
-- Consultas sobre o DADO CRU e os DICIONÁRIOS
--
-- As consultas de `consultas_duckdb.sql` leem a camada pronta. Estas leem o
-- que saiu do FTP do Ministério do Trabalho: só códigos, tudo como texto,
-- nenhuma descrição. É aqui que se vê o problema que o pipeline resolve.
--
--   Gianpedro/bronze_caged   microdados crus do CAGED + dicionarios.parquet
--   Gianpedro/bronze_rais    microdados crus da RAIS  + dicionarios.parquet
--
-- Partições preservam o caminho original do FTP:
--   caged_mov/ano=2025/mes=6/*.parquet
--   rais_vinc/ano=2007/rais_vinc_ac.parquet   (um arquivo por UF)
--   rais_vinc/ano=2022/*.parquet              (o MTE passou a publicar por região)
-- =============================================================================

INSTALL httpfs;
LOAD httpfs;


-- 1. Como o dado chega: tudo texto, nada traduzido -----------------------------
-- 36 colunas, todas VARCHAR. O bronze não converte tipo nem interpreta código:
-- guarda o que veio. Qualquer conversão aqui seria uma decisão escondida.
DESCRIBE
SELECT *
FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/caged_mov/ano=2025/mes=6/*.parquet');


-- 2. O problema, em três linhas ------------------------------------------------
-- Sem dicionário, isto é ilegível: sexo = 3? grau de instrução = 7?
SELECT sexo, graudeinstrucao, racacor, cbo2002ocupacao, subclasse, saldomovimentacao
FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/caged_mov/ano=2025/mes=6/*.parquet')
LIMIT 5;


-- 3. O dicionário extraído das planilhas de layout do FTP ----------------------
-- Uma linha por código, com a procedência: de qual planilha, de qual aba, de
-- qual caminho no FTP e quando foi extraído.
SELECT tabela, count(DISTINCT coluna) AS dimensoes, count(*) AS codigos
FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/dicionarios.parquet')
GROUP BY tabela
ORDER BY tabela;


-- 4. Traduzir o dado cru: o que o pipeline faz, em SQL -------------------------
-- Um LEFT JOIN por dimensão. Repare no filtro `d.tabela = 'caged_mov'`: sem ele
-- a tradução pega o código de outra geração do CAGED (ver consulta 6).
SELECT
    s.descricao                                   AS sexo,
    g.descricao                                   AS grau_de_instrucao,
    count(*) FILTER (WHERE m.saldomovimentacao = '1')  AS admissoes,
    count(*) FILTER (WHERE m.saldomovimentacao = '-1') AS desligamentos
FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/caged_mov/ano=2025/mes=6/*.parquet') AS m
LEFT JOIN read_parquet('hf://datasets/Gianpedro/bronze_caged/dicionarios.parquet') AS s
       ON s.tabela = 'caged_mov' AND s.coluna = 'sexo'            AND s.codigo = m.sexo
LEFT JOIN read_parquet('hf://datasets/Gianpedro/bronze_caged/dicionarios.parquet') AS g
       ON g.tabela = 'caged_mov' AND g.coluna = 'graudeinstrucao' AND g.codigo = m.graudeinstrucao
-- GROUP BY por posição: `sexo` também é o nome de uma coluna crua, e o
-- DuckDB daria preferência a ela em vez do apelido da tradução.
GROUP BY 1, 2
ORDER BY admissoes DESC;


-- 5. De onde veio cada tradução (auditabilidade) -------------------------------
-- Quem duvidar de uma descrição confere contra o layout oficial, sem caçar a
-- planilha no FTP.
SELECT DISTINCT tabela, coluna, planilha, aba, caminho_ftp, extraido_em
FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/dicionarios.parquet')
WHERE coluna = 'sexo';


-- 6. Por que o dicionário é POR TABELA ----------------------------------------
-- O mesmo conceito, dois sistemas de código incompatíveis. Traduzir o CAGED
-- antigo com o dicionário do novo transformaria "FEMININO" (2) em nada, e
-- "Mulher" (3) não existiria no antigo.
SELECT tabela, codigo, descricao
FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/dicionarios.parquet')
WHERE coluna = 'sexo' AND tabela IN ('caged_mov', 'caged_old')
ORDER BY tabela, codigo;


-- 7. O erro que isso evita ----------------------------------------------------
-- Juntar SEM filtrar a tabela duplica linhas: o código '1' existe nas duas
-- gerações com descrições diferentes. A contagem infla e ninguém percebe.
SELECT
    count(*)                                  AS linhas_apos_join_errado,
    count(DISTINCT d.descricao)               AS descricoes_conflitantes,
    string_agg(DISTINCT d.descricao, ' | ')   AS quais
FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/caged_mov/ano=2025/mes=6/*.parquet') AS m
JOIN read_parquet('hf://datasets/Gianpedro/bronze_caged/dicionarios.parquet') AS d
  ON d.coluna = 'sexo' AND d.codigo = m.sexo   -- faltou d.tabela = 'caged_mov'
WHERE m.sexo = '1';


-- 8. A RAIS crua, e o mesmo padrão --------------------------------------------
-- 67 colunas, todas texto. O nome do arquivo mudou ao longo da série: por UF
-- até 2018, por região depois. Aqui um arquivo só (Centro-Oeste), para a
-- consulta responder em segundos; trocar por `ano=2022/*.parquet` lê o país.
SELECT
    s.descricao                          AS sexo,
    e.descricao                          AS escolaridade,
    count(*)                             AS vinculos,
    count(*) FILTER (WHERE v.vinculo_ativo_3112 = '1') AS ativos_em_31_12
FROM read_parquet('hf://datasets/Gianpedro/bronze_rais/rais_vinc/ano=2022/rais_vinc_centro_oeste_parte00.parquet') AS v
-- O código vem com zero à esquerda no microdado ('01') e sem ele no layout
-- ('1'). Comparar como NÚMERO resolve. Comparando como texto, só mestrado e
-- doutorado casariam — são os de dois dígitos — e todo o resto viraria NULL
-- sem erro nenhum: a consulta "funciona" e a tradução sai vazia.
LEFT JOIN read_parquet('hf://datasets/Gianpedro/bronze_rais/dicionarios.parquet') AS s
       ON s.tabela = 'rais_vinc' AND s.coluna = 'sexo_trabalhador'
      AND TRY_CAST(s.codigo AS BIGINT) = TRY_CAST(v.sexo_trabalhador AS BIGINT)
LEFT JOIN read_parquet('hf://datasets/Gianpedro/bronze_rais/dicionarios.parquet') AS e
       ON e.tabela = 'rais_vinc' AND e.coluna = 'escolaridade_apos_2005'
      AND TRY_CAST(e.codigo AS BIGINT) = TRY_CAST(v.escolaridade_apos_2005 AS BIGINT)
GROUP BY 1, 2
ORDER BY vinculos DESC
LIMIT 12;


-- 9. O recorte de tecnologia aplicado ao dado cru ------------------------------
-- É a definição da pesquisa em SQL: empresa com CNAE de TI OU ocupação com CBO
-- de TI. As famílias de CBO são os quatro primeiros dígitos, o que captura
-- códigos que o MTE criou depois (arquiteto de soluções, analista de testes).
SELECT
    count(*)                                               AS movimentacoes_ti,
    count(*) FILTER (WHERE saldomovimentacao = '1')        AS admissoes,
    count(*) FILTER (WHERE substr(cbo2002ocupacao, 1, 4) IN
        ('1236','1425','2031','2122','2123','2124','3171','3172')
        OR cbo2002ocupacao IN ('313220','313305','142135'))  AS por_ocupacao,
    count(*) FILTER (WHERE subclasse IN
        ('6201500','6201501','6201502','6202300','6203100',
         '6204000','6209100','6311900','6319400','6399200'))  AS por_setor
FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/caged_mov/ano=2025/mes=6/*.parquet')
WHERE substr(cbo2002ocupacao, 1, 4) IN
        ('1236','1425','2031','2122','2123','2124','3171','3172')
   OR cbo2002ocupacao IN ('313220','313305','142135')
   OR subclasse IN ('6201500','6201501','6201502','6202300','6203100',
                    '6204000','6209100','6311900','6319400','6399200');


-- 10. O cru reproduz a camada publicada? ---------------------------------------
-- O mesmo mês, contado no dado bruto e na silver de TI já tratada. Se os dois
-- números baterem, o recorte publicado é reprodutível a partir da fonte.
WITH cru AS (
    SELECT count(*) AS linhas
    FROM read_parquet('hf://datasets/Gianpedro/bronze_caged/caged_mov/ano=2025/mes=6/*.parquet')
    WHERE substr(cbo2002ocupacao, 1, 4) IN
            ('1236','1425','2031','2122','2123','2124','3171','3172')
       OR cbo2002ocupacao IN ('313220','313305','142135')
       OR subclasse IN ('6201500','6201501','6201502','6202300','6203100',
                        '6204000','6209100','6311900','6319400','6399200')
),
publicado AS (
    SELECT count(*) AS linhas
    FROM read_parquet('hf://datasets/Gianpedro/caged-tecnologia/caged_mov/ano_particao=2025/mes_particao=6/*.parquet')
)
SELECT
    cru.linhas                         AS no_dado_cru,
    publicado.linhas                   AS na_camada_publicada,
    cru.linhas - publicado.linhas      AS diferenca
FROM cru, publicado;
