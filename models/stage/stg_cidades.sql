-- CTE DEVELOPMENT
{{ config(materialized='view') }}
WITH source AS (
    SELECT
        id_cidades,
        INITCAP(cidade) AS nome_cidade, 
        id_estados,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao  -- RETORNA O PRIMEIRO NÃO NULO DA LISTA
    FROM {{ source('sources', 'CIDADES') }}
)
--
SELECT
    id_cidades,
    nome_cidade,
    id_estados,
    data_inclusao,
    data_atualizacao
FROM source
