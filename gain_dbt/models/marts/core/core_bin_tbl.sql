
/*
    This script creates a bin_tbl, which pulls from bins_dump, and joins the bay types from capacity utilization
*/


with bins_dump as (
    select * from {{ref('stg_bins_dump')}}
),

bays_dump as (
    select * from {{ref('stg_bays_dump')}} 
),

----- this combines the bin level data with the bay level (capacity) data -----
bin_tbl as (
    SELECT
          bin.snapshot_day
        , bin.region_id
        , bin.warehouse_id
        , bin.pick_area_name
        , cu.bay_type
        , bin.bin_id
        , bin.bin_type_name
        , bin.bin_usage_name
        , bin.is_locked
        , bin.cumulative_height
        , bin.total_height
        , CASE
            WHEN bin.bin_type_name LIKE '%KIVA%' THEN 'Floor'
            WHEN bin.total_height > 110 THEN 'Air'
            WHEN bin.cumulative_height > 65 THEN 'High_Floor'
            ELSE 'Floor'
          END AS bin_height_category

        , bin.mrg_key

    FROM (
        SELECT
              a.snapshot_day
            , a.region_id
            , a.warehouse_id
            , a.pick_area_name
            , a.bin_id
            , a.bin_height
            , a.rack
            , a.shelf
            , a.bin_type_name
            , a.bin_usage_name
            , a.is_locked
            , a.mrg_key
            , SUM(a.bin_height) OVER (PARTITION BY a.rack ORDER BY a.shelf ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_height
            , SUM(a.bin_height) OVER (PARTITION BY a.rack ) AS total_height
        FROM bins_dump a
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    ) bin
    LEFT JOIN bays_dump cu ON bin.mrg_key = cu.mrg_key
)

select * 
from bin_tbl
