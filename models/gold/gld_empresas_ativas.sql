with silver as (
    select * from {{ ref('slv_cnpj')}}
)
select
uf,
cnae_fiscal_descricao,
count(cnpj) as qt_empresas,
municipio
from silver
where fl_ativa = true
group by uf, cnae_fiscal_descricao, municipio
order by qt_empresas desc