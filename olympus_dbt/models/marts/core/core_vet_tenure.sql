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
        count(distinct case when cumulative_hrs_worked >= 400 then employee_id end) as vet_emps,
        count(distinct employee_id) as total_emps,
        vet_emps*1.0/nullif(total_emps,0) as vet_pct
    from vet_tenure
    group by 1,2,3
)

select * 
from load_table
