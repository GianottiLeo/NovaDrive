{{ config(materialized='incremental', unique_key='venda_id') }}
WITH vendas AS (
    SELECT
        v.id_vendas AS venda_id,
        v.id_veiculos AS veiculo_id,
        v.id_concessionarias AS concessionaria_id,
        v.id_vendedores AS vendedor_id,
        v.id_clientes AS cliente_id,
        v.valor_venda,
        v.data_venda,
        v.data_inclusao,
        v.data_atualizacao
    FROM {{ ref('stg_vendas') }} v
    -- It's generally better to join on the staging table before filtering.
    -- This ensures the join logic is applied to all data.
    JOIN {{ ref('dim_veiculos') }} vei ON v.id_veiculos = vei.veiculo_id
    JOIN {{ ref('dim_concessionarias') }} con ON v.id_concessionarias = con.concessionaria_id
    JOIN {{ ref('dim_vendedores') }} ven ON v.id_vendedores = ven.vendedor_id
    JOIN {{ ref('dim_clientes') }} cli ON v.id_clientes = cli.cliente_id
)

SELECT
    venda_id,
    veiculo_id,
    concessionaria_id,
    vendedor_id,
    cliente_id,
    valor_venda,
    data_venda,
    data_inclusao,
    data_atualizacao
FROM vendas

{% if is_incremental() %}

  -- This is the crucial part for incremental logic.
  -- We filter for new or updated records based on the timestamp.
  -- {{ this }} refers to the destination table being built.
  WHERE data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})

{% endif %}
