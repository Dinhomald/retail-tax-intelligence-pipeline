with source as (
    select * from {{ source('bronze', 'raw_cnpj') }}
)

select
    cnpj,
    razao_social,
    nome_fantasia,
    situacao_cadastral,
    codigo_porte,
    porte,
    uf,
    municipio,
    bairro,
    logradouro,
    numero,
    cep,
    email,
    ddd_telefone_1,
    ddd_telefone_2,
    cnae_fiscal,
    cnae_fiscal_descricao,
    natureza_juridica,
    data_inicio_atividade,
    capital_social,
    ingested_at,
    current_timestamp() as processed_at

from source