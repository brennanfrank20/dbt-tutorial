
/*
    This script creates a bin_tbl, which pulls from bins_dump, and joins the bay types from capacity utilization
*/





with capacity_utilization as (
    select * from {{ref('stg_capacity_utilization')}} 
),

bin_tbl as (
    select * from {{ref('core_bin_tbl')}} 
),


load_table as (
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
        , bns.is_locked
        , bns.bin_height_category
    FROM capacity_utilization cu
    left join bin_tbl bns on cu.warehouse_id = bns.warehouse_id AND cu.bin_id = bns.bin_id AND TRUNC(cu.snapshot_day) = TRUNC(bns.snapshot_day)
) -- end load_table



SELECT
	 TRUNC(snapshot_day) AS balance_date
	, warehouse_id
	, COALESCE(bay_type, 'OTHER')                             AS bay_type
	, COALESCE(bin_type, 'OTHER')                             AS bin_type
	, COALESCE(bin_height_category, 'OTHER')                  AS bin_height_category
	, SUBSTRING(bin_id, 1, 5)                                 AS mod
	, COALESCE(is_locked, '0')                                AS is_locked
	, SUM(total_inventory_volume)/ NULLIF(sum(capacity), 0)   AS fullness
	, AVG(target_utilization)                                 AS target_utilization
	, SUM(total_inventory_volume)                             AS cube
	, SUM(capacity)                                           AS capacity
	, SUM(gross_bin_volume)                                   AS gross_bin_volume
	, SUM(total_units)                                        AS units
from load_table
group by 1, 2, 3, 4, 5, 6, 7
