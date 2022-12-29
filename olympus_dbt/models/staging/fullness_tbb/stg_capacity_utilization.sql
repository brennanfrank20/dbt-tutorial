
-- capacity utilization staging table

    {{ config(materialized='table', sort=['region_id', 'snapshot_day']) }}


    select 
    	TRUNC(cu.snapshot_day) AS snapshot_day
		, cu.region_id
		, cu.warehouse_id
		, cu.container_id
		, cu.bin_id
		, coalesce(cu.bin_type, 'OTHER') as bin_type
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
		, coalesce(cu.bay_type, 'OTHER') as bay_type
		, cu.aisle_number
		, cu.aisle
        , cu.floor
		, cu.pick_mod
        , TRUNC(cu.snapshot_day) + cu.warehouse_id + cu.bin_id as mrg_key
    from {{ source('olympus_capacity', 'capacity_utilization_v2') }} cu  --"aft-cap-conf".capacity_utilization_v2 AS cu
    join {{ ref('stg_warehouses') }} w on cu.warehouse_id = w.warehouse_id   
    where true
        and cu.region_id = 1
        AND cu.snapshot_day = '2022-12-15'





