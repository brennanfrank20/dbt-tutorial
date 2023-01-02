
/*
    This script aggregates all the non-AR stow olympus metrics into one view -- master_stow_non_ar
*/

with stow_tbb_bee as (
    select * from {{ref('core_stow_tbb_bee')}}
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

uit as (
    select * from {{ref('stg_uit')}}
),


load_table as (
    select
        tbb.region_id,
        tbb.warehouse_id,
        tbb.balance_date,
        tbb.bay_type,
        tbb.bin_type,
        tbb.bin_height_category,
        tbb.container_type,
        tbb.volume                                                          as act_vol,
        tbb.secs/3600.0                                                     as act_hrs,
        act_vol*1.0/nullif(act_hrs,0)                                       as act_uph,
        ful.cube,
        ful.capacity,
        ful.cube*1.0/nullif(ful.capacity,0)                                 as fullness_pct,
        case
            when ful.cube = 0 then 'low'
            when ful.capacity = 0 then 'low'
            when fullness_pct < 0.5 then 'low'
            when fullness_pct < 0.8 then 'med'
            when fullness_pct < 0.9 then 'high'
            when fullness_pct < 0.95 then 'veryhigh'
            when fullness_pct < 1 then 'reallyhigh'
            when fullness_pct < 1.1 then 'reallyreallyhigh'
            else 'extremelyhigh' 
        end::varchar(55)                                                     as fullness_type,
        vet.vet_emps,
        vet.total_emps,
        vet.vet_pct,
        uit.unknown_idle_time_hours                                          as uit_hrs,
        uit.total_direct_hours                                               as uit_tot_hrs,
        uit_hrs*1.0/nullif(uit_tot_hrs,0)                                    as uit_pct

    from stow_tbb_bee tbb
    left join fullness ful on tbb.warehouse_id = ful.warehouse_id and tbb.balance_date = ful.balance_date and tbb.bay_type = ful.bay_type and tbb.bin_type = ful.bin_type and tbb.bin_height_category = ful.bin_height_category
    left join vet_tenure vet on tbb.warehouse_id = vet.warehouse_id and tbb.balance_date = vet.balance_date and vet.process_path = 'stow'
    left join uit on tbb.warehouse_id = uit.warehouse_id and tbb.balance_date = uit.balance_date and uit.process_path = 'stow'
)

select * 
from load_table
