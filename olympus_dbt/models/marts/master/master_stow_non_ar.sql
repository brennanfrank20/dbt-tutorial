
/*
    This script aggregates all the non-AR stow olympus metrics into one table -- master_stow_non_ar
*/

with stow_tbb_bee as (
    select * from {{ref('core_pick_tbb_bee')}}
),

fullness as ( -- fullness has more granularities than stow tbb (it also has mod and is_locked dimensions)
    SELECT 
        warehouse_id,
        balance_date,
        bay_type,
        bin_type,
        bin_height_category,
        sum(cube)               as cube,
        sum(capacity)           as capacity
    FROM {{ref('core_fullness')}}
    where is_locked = 'N'
    group by 1,2,3,4,5
),

vet_tenure as (
    select * from {{ref('core_vet_tenure')}}
),


load_table as (
    select
        s.region_id,
        s.warehouse_id,
        s.balance_date,
        s.bay_type,
        s.bin_type,
        s.bin_height_category,
        s.container_type,
        s.volume as act_vol,
        s.secs/3600.0 as act_hrs,
        f.cube,
        f.capacity,
        f.cube*1.0/nullif(f.capacity,0) as fullness_pct,
        case
            when f.cube = 0 then 'low'
            when f.capacity = 0 then 'low'
            when f.cube / f.capacity < 0.5 then 'low'
            when f.cube / f.capacity < 0.8 then 'med'
            when f.cube / f.capacity < 0.9 then 'high'
            when f.cube / f.capacity < 0.95 then 'veryhigh'
            when f.cube / f.capacity < 1 then 'reallyhigh'
            when f.cube / f.capacity < 1.1 then 'reallyreallyhigh'
            else 'extremelyhigh' 
        end::varchar(55)                                            as fullness_type,
        vet_emps,
        total_emps,
        vet_pct

    from stow_tbb_bee s
    left join fullness f on s.warehouse_id = f.warehouse_id and s.balance_date = f.balance_date and s.bay_type = f.bay_type and s.bin_type = f.bin_type and s.bin_height_category = f.bin_height_category
    left join vet_tenure v on s.warehouse_id = v.warehouse_id and s.balance_date = v.balance_date and v.process_path = 'stow'
)

select * 
from load_table
