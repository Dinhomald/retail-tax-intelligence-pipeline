with bronze as (
    select * from {{ ref('brz_cnpj')}}
)
select 
LPAD(CAST(cnpj AS STRING), 14, '0') as cnpj,
COALESCE(NULLIF(nome_fantasia, ''), razao_social) as nome_fantasia,
data_inicio_atividade as dt_inicio_atividade,
case
    when situacao_cadastral = 2 then true
    else false
end as fl_ativa,
razao_social,
uf,
municipio,
cnae_fiscal,
cnae_fiscal_descricao,
capital_social,
porte,
ingested_at,
processed_at
from bronze