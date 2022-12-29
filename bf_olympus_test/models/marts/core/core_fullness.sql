
/*
    This script creates a bin_tbl, which pulls from bins_dump, and joins the bay types from capacity utilization
*/





with capacity_utilization as (
    select * from {{ref('stg_capacity_utilization')}} 
),

bin_tbl as (
    select * from {{ref('core_bin_tbl')}} 
)



SELECT
    TRUNC(cu.snapshot_day) AS snapshot_day
    , cu.region_id
    , cu.warehouse_id
    , cu.container_id
    , cu.bin_id
    , cu.bin_type
    , cu.bin_usage
    , cu.drop_zone
    , cu.pick_area
    , cu.unique_asin_count
    , cu.max_unique_asin_count
    , cu.total_units
    , cu.total_inventory_volume
    , cu.gross_bin_volume
    , cu.target_utilization
    , cu.gross_bin_volume * target_utilization / 100 AS capacity
    , cu.shelf
    , cu.bay_id
    , cu.bay_type
    , cu.aisle_number
    , cu.aisle
    , cu.pick_mod
    , cu.floor
    , bn.is_locked
    , bn.bin_height_category
FROM capacity_utilization cu
left join bin_tbl bn on cu.mrg_key = bn.mrg_key