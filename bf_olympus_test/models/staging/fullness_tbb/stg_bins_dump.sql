-- bins dump staging table

/*
    Welcome to your second dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


-- Example with multiple sort keys
-- this config statement overrides anything you have set in your dbt_project.yml file
{{ config(materialized='table', sort=['warehouse_id', 'snapshot_day']) }}

-- Use the `ref` function to select from other models

select
    TRUNC(bd.snapshot_day)     AS snapshot_day
    , bd.region_id
    , bd.warehouse_id
    , bd.pick_area_name
    , bd.bin_id
    , COALESCE(bd.bin_type_name, 'OTHER') as bin_type_name
    , bd.bin_bay
    , bd.bin_module
    , bd.bin_height
    , CONCAT(SUBSTRING(bd.warehouse_id, 1, 4) , CONCAT(SUBSTRING(bd.bin_id, 1, 8), SUBSTRING(bd.bin_id, 10, 3)))    AS rack
    , SUBSTRING(bd.bin_id, 9, 1) AS shelf
    , TRUNC(bd.snapshot_day) + bd.warehouse_id + bd.bin_id as mrg_key

from {{ source('olympus_bins', 'bins_dump_v2') }} bd  -- aft_cap_conf_ddl.bins_dump_v2 bd
join {{ ref('stg_warehouses') }} w on bd.warehouse_id = w.warehouse_id
where true 
    and bd.region_id = 1
    and bd.snapshot_day = '2022-12-15'
group by 1,2,3,4,5,6,7,8,9,10,11
