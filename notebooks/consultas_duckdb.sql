-- =============================================================================
-- Consultas DuckDB sobre os dados publicados no Hugging Face
--
-- Rodam em qualquer DuckDB (CLI, Python, DBeaver) sem baixar nada: o DuckDB lê
-- o parquet direto do Hugging Face, e só os pedaços que a consulta precisa.
-- Três datasets públicos:
--
--   Gianpedro/mercado-ti-gold   agregados prontos (3 MB) — rápido
--   Gianpedro/caged-tecnologia  microdados do CAGED, uma linha por movimentação
--   Gianpedro/rais-tecnologia   microdados da RAIS, uma linha por vínculo
--
-- Da mais simples à mais completa: SELECT, WHERE, GROUP BY, função de janela,
-- JOIN entre tabelas e JOIN entre as duas bases.
-- =============================================================================

INSTALL httpfs;
LOAD httpfs;


-- 1. Olhar a tabela -----------------------------------------------------------
-- Estoque de TI já agregado: uma linha por ano e lente (setor/ocupação).
SELECT *
FROM read_parquet('hf://datasets/Gianpedro/mercado-ti-gold/rais_estoque_anual.parquet')
LIMIT 10;


-- 2. Quais colunas existem no microdado --------------------------------------
-- DESCRIBE não lê os dados, só o esquema: responde na hora.
DESCRIBE
SELECT *
FROM read_parquet('hf://datasets/Gianpedro/caged-tecnologia/caged_mov/ano_particao=2025/**/*.parquet');


-- 3. WHERE + GROUP BY: quantos vínculos de TI existem por ano -----------------
-- O recorte é "setor de TI OU ocupação de TI". A mediana salarial é ponderada
-- pelo estoque de cada grupo, senão o Acre pesaria igual a São Paulo.
SELECT
    ano,
    SUM(estoque_3112)                                                   AS vinculos_ti,
    ROUND(SUM(estoque_3112 * remuneracao_sm_mediana) / SUM(estoque_3112), 2)
                                                                        AS mediana_salarios_minimos
FROM read_parquet('hf://datasets/Gianpedro/mercado-ti-gold/rais_estoque_anual.parquet')
WHERE setor_ti OR ocupacao_ti
GROUP BY ano
ORDER BY ano;


-- 4. Função de janela: crescimento ano a ano ----------------------------------
-- LAG pega o valor do ano anterior sem precisar de self-join.
WITH anual AS (
    SELECT ano, SUM(estoque_3112) AS vinculos_ti
    FROM read_parquet('hf://datasets/Gianpedro/mercado-ti-gold/rais_estoque_anual.parquet')
    WHERE setor_ti OR ocupacao_ti
    GROUP BY ano
)
SELECT
    ano,
    vinculos_ti,
    vinculos_ti - LAG(vinculos_ti) OVER (ORDER BY ano)                      AS variacao,
    ROUND(100.0 * (vinculos_ti / LAG(vinculos_ti) OVER (ORDER BY ano) - 1), 1) AS variacao_pct
FROM anual
ORDER BY ano;


-- 5. Microdado do CAGED: admissões, desligamentos e saldo por UF em 2025 -----
-- Cada linha é uma movimentação: saldomovimentacao = +1 admissão, -1 desligamento.
-- O salário mediano usa só admissões com valor informado.
SELECT
    uf_descricao                                                         AS uf,
    COUNT(*) FILTER (WHERE saldomovimentacao = 1)                        AS admissoes,
    COUNT(*) FILTER (WHERE saldomovimentacao = -1)                       AS desligamentos,
    SUM(saldomovimentacao)                                               AS saldo,
    MEDIAN(salario) FILTER (WHERE saldomovimentacao = 1 AND salario > 0) AS salario_mediano_admissao
FROM read_parquet('hf://datasets/Gianpedro/caged-tecnologia/caged_mov/ano_particao=2025/**/*.parquet')
-- Agrupa pela coluna, não pelo apelido: o microdado já tem uma coluna chamada
-- `uf` (o código numérico), e `GROUP BY uf` pegaria ela em vez do apelido.
GROUP BY uf_descricao
ORDER BY saldo DESC;


-- 6. HAVING: ocupações que mais contrataram em 2025 ---------------------------
-- HAVING filtra DEPOIS de agrupar: corta ocupações com pouca movimentação.
SELECT
    cbo2002ocupacao_descricao                                            AS ocupacao,
    COUNT(*) FILTER (WHERE saldomovimentacao = 1)                        AS admissoes,
    SUM(saldomovimentacao)                                               AS saldo,
    MEDIAN(salario) FILTER (WHERE saldomovimentacao = 1 AND salario > 0) AS salario_mediano
FROM read_parquet('hf://datasets/Gianpedro/caged-tecnologia/caged_mov/ano_particao=2025/**/*.parquet')
GROUP BY ocupacao
HAVING COUNT(*) >= 1000
ORDER BY admissoes DESC
LIMIT 15;


-- 7. JOIN: vínculos de TI por mil habitantes ----------------------------------
-- Cruza o estoque por município com a população do IBGE pelo código IBGE.
-- Tamanho absoluto só mede o tamanho da cidade; por habitante mede especialização.
SELECT
    g.nome                                              AS municipio,
    g.uf,
    m.estoque                                           AS vinculos_ti,
    g.populacao,
    ROUND(1000.0 * m.estoque / g.populacao, 1)          AS vinculos_por_mil_habitantes
FROM read_parquet('hf://datasets/Gianpedro/mercado-ti-gold/mapa_municipio.parquet') AS m
JOIN read_parquet('hf://datasets/Gianpedro/mercado-ti-gold/geo_municipios.parquet') AS g
  ON g.cod6 = m.cod_municipio
WHERE m.ano = 2025
  AND g.populacao > 100000
ORDER BY vinculos_por_mil_habitantes DESC
LIMIT 15;


-- 8. JOIN entre as duas bases: fluxo do CAGED contra estoque da RAIS ---------
-- O CAGED diz quanto cada UF cresceu em 2025; a RAIS, de que tamanho ela é.
-- Dividir um pelo outro mostra quem cresce rápido para o próprio tamanho —
-- coisa que nenhuma das duas bases responde sozinha.
WITH fluxo AS (
    SELECT
        upper(split_part(municipio_descricao, '-', 1)) AS uf,
        SUM(saldomovimentacao)                         AS saldo_2025
    FROM read_parquet('hf://datasets/Gianpedro/caged-tecnologia/caged_mov/ano_particao=2025/**/*.parquet')
    GROUP BY 1
),
estoque AS (
    SELECT uf, SUM(estoque_3112) AS vinculos_2025
    FROM read_parquet('hf://datasets/Gianpedro/mercado-ti-gold/rais_estoque_uf.parquet')
    WHERE ano = 2025 AND (setor_ti OR ocupacao_ti)
    GROUP BY uf
)
SELECT
    e.uf,
    e.vinculos_2025,
    f.saldo_2025,
    ROUND(100.0 * f.saldo_2025 / e.vinculos_2025, 2) AS crescimento_relativo_pct
FROM estoque AS e
JOIN fluxo   AS f USING (uf)
ORDER BY crescimento_relativo_pct DESC;


-- 9. Resultado de modelo: o hiato salarial de gênero em 19 anos ---------------
-- "explicada" = diferença de perfil; "nao_explicada" = diferença comparando
-- perfis equivalentes. A explicada é negativa: pelo perfil, as mulheres
-- deveriam ganhar mais.
SELECT
    ano,
    ROUND(hiato_pct, 1)     AS hiato_bruto_pct,
    ROUND(explicada, 3)     AS explicada_pelo_perfil,
    ROUND(nao_explicada, 3) AS nao_explicada
FROM read_parquet('hf://datasets/Gianpedro/mercado-ti-gold/hiato_serie.parquet')
WHERE comparacao = 'MASCULINO vs FEMININO'
ORDER BY ano;
