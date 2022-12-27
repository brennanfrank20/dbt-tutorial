
-- Use the `ref` function to select from other models

select
    TRUNC(bd.snapshot_day)     AS snapshot_day
    , bd.region_id
    , bd.warehouse_id
    , bd.pick_area_name
    , bd.bin_id
    , bd.bin_type_name
    , bd.bin_bay
    , bd.bin_module
    , bd.bin_height
    , CONCAT(SUBSTRING(bd.warehouse_id, 1, 4) , CONCAT(SUBSTRING(bd.bin_id, 1, 8), SUBSTRING(bd.bin_id, 10, 3)))    AS rack
    , SUBSTRING(bd.bin_id, 9, 1) AS shelf

from aft_cap_conf_ddl.bins_dump_v2 bd
join {{ ref('warehouses') }} w on bd.warehouse_id = w.warehouse_id
where true 
    and bd.region_id=3
    and bd.snapshot_day='2022-12-15'
group by 1,2,3,4,5,6,7,8,9,10,11