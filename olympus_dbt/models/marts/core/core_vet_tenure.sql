/*
    This script pulls the vet % per process path
*/


with vet_tenure as (
    select * from {{ref('stg_vet_tenure')}}
),

load_table as (
    select 
        warehouse_id,
        balance_date,
        process_path,
        sum(case when tenure='VET' then cumulative_hrs_worked end) as vet_hrs,
        sum(cumulative_hrs_worked) as total_hrs_worked,
        vet_hrs*1.0/nullif(total_hrs_worked,0) as vet_pct
    from vet_tenure
    group by 1,2,3
)

select * 
from load_table
