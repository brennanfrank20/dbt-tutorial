
/*
    bins dump staging table
*/



-- config statements override anything set in dbt_project.yml file
{{ config(materialized='table', sort=['warehouse_id', 'snapshot_day']) }}

select
    TRUNC(bd.snapshot_day)     AS snapshot_day
    , w.region_id
    , bd.warehouse_id
    , bd.pick_area_name
    , bd.bin_id
    , bd.is_locked
    , bd.bin_usage_name
    , COALESCE(bd.bin_type_name, 'OTHER') as bin_type_name
    , bd.bin_bay
    , bd.bin_module
    , bd.bin_height
    , CONCAT(SUBSTRING(bd.warehouse_id, 1, 4) , CONCAT(SUBSTRING(bd.bin_id, 1, 8), SUBSTRING(bd.bin_id, 10, 3)))    AS rack
    , SUBSTRING(bd.bin_id, 9, 1) AS shelf
    , TRUNC(bd.snapshot_day) + bd.warehouse_id + bd.bin_id as mrg_key

from {{ source('aft_cap_conf_ddl', 'bins_dump_v2') }} bd  -- aft_cap_conf_ddl.bins_dump_v2 bd
join {{ ref('stg_warehouses') }} w on bd.warehouse_id = w.warehouse_id
where true 
    and bd.region_id = 1
    and bd.snapshot_day = '2022-12-15'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
