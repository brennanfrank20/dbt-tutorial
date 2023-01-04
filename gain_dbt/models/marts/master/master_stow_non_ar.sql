
/*
    This script aggregates all the non-AR stow olympus metrics into one table -- master_stow_non_ar
*/

with stow_tbb_bee as ( -- bin height category (formerly stow type) is not included in legacy master table, nor in ASP
    select 
        region_id,
        balance_date,
        warehouse_id,
        bay_type,
        bin_type,
        container_type,
        -- , bin_height_category
        sum(secs) as secs,
        sum(volume) as volume
    from {{ref('core_stow_tbb_bee')}}
    group by 1,2,3,4,5,6
),

fullness as ( -- fullness has more granularities than stow tbb (it also has bin height category, mod, and is_locked dimensions)
    SELECT 
        warehouse_id,
        balance_date,
        bay_type,
        bin_type,
        sum(cube)               as cube,
        sum(capacity)           as capacity
    FROM {{ref('core_fullness')}}
    where is_locked = 'N'
    group by 1,2,3,4
),

vet_tenure as (
    select * 
    from {{ref('core_vet_tenure')}}
    where process_path='stow'
),

uit as (
    select *
    from {{ref('stg_uit')}}
    where process_path='stow'
),

faststart_strongfinish as (
    select * 
    from {{ref('stg_faststart_strongfinish')}}
    where process_path='non_ar_stow'
),

load_table as (
    select
        tbb.region_id,
        tbb.warehouse_id,
        tbb.balance_date,
        tbb.bay_type,
        tbb.bin_type,
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
        uit.total_direct_hours                                               as total_direct_hrs,
        uit_hrs*1.0/nullif(total_direct_hrs,0)                               as uit_pct,
        fssf.faststart_lost_hrs,
        fssf.strongfinish_lost_hrs

    from stow_tbb_bee tbb
    left join fullness ful on tbb.warehouse_id = ful.warehouse_id and tbb.balance_date = ful.balance_date and tbb.bay_type = ful.bay_type and tbb.bin_type = ful.bin_type 
    left join vet_tenure vet on tbb.warehouse_id = vet.warehouse_id and tbb.balance_date = vet.balance_date 
    left join uit on tbb.warehouse_id = uit.warehouse_id and tbb.balance_date = uit.balance_date 
    left join faststart_strongfinish fssf on tbb.warehouse_id = fssf.warehouse_id and tbb.balance_date = fssf.balance_date
)

select * 
from load_table
