
/*
    This script aggregates all the non-AR stow olympus metrics into one view -- master_stow_non_ar
*/

with master_stow_non_ar as (
    select * 
    from {{ref('master_stow_non_ar')}}
),


load_table as (
    select
        region_id,
        warehouse_id,
        balance_date,
        bay_type,
        bin_type,
        container_type,
        act_vol,
        act_hrs,
        act_uph,
        cube,
        capacity,
        fullness_pct,
        fullness_type,
        vet_emps,
        total_emps,
        vet_pct,
        uit_hrs,
        total_direct_hrs,
        uit_pct,
        faststart_lost_hrs,
        strongfinish_lost_hrs
    from master_stow_non_ar
)

select * 
from load_table
