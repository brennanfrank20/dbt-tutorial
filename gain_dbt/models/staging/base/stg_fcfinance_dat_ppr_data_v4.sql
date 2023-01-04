
/*
    staging table for fcfinance DAT_PPR_DATA_V4 actual volume and hours for each process ID
*/

-- config statements override anything set in dbt_project.yml file
{{ config(materialized='table', sort=['balance_date']) }}


with ppr as (

    select 
        w.region_id,
        fc as warehouse_id,
        balancedate as balance_date,
        process_id,
        volume as act_vol,
        hours as act_hrs
    from {{ source('wwbt_bm_sp_tables', 'fcfinance_dat_ppr_data_v4') }} f
    join {{ ref('stg_warehouses') }} w on f.fc = w.warehouse_id      
    where true
        and process_id in (
            15,16,17,18,29,30,31,32,         -- stow
            48,49,50,51,                     -- ixd_sort
            60,61,62,63,                     -- pick
            69,70,71,72,                     -- afe flow sort
            74,75,76,77,                     -- afe pack chuting
            79,80,81,82,83,84,85,86,87,88,   -- pack
            96,                              -- ship
            97,98,99,100                     -- relo crets
        )
        and batch_id = 'Actual'
        and "interval" = 'Daily'
        and balancedate = '2022-12-15' -- just for testing
)

select *
from ppr
