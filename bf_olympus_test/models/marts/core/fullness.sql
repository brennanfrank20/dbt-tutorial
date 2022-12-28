with warehouses as (
    select * from {{ref('warehouses')}}
),

bins_dump as (
    select * from {{ref('bins_dump')}}
),

capacity_utilization as (
    select * from {{ref('capacity_utilization')}}
),

final_cte as (
    SELECT
          bin.snapshot_day
        , bin.region_id
        , bin.warehouse_id
        , bin.pick_area_name
        , cu.bay_type
        , bin.bin_id
        , bin.bin_type_name
        , bin.cumulative_height
        , bin.total_height
        , bin.mrg_key
        , CASE
            WHEN bin.bin_type_name LIKE '%KIVA%' THEN 'Floor'
            WHEN bin.total_height > 110 THEN 'Air'
            WHEN bin.cumulative_height > 65 THEN 'High_Floor'
            ELSE 'Floor'
          END AS pick_type
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
            , a.mrg_key
            , SUM(a.bin_height) OVER (PARTITION BY a.rack ORDER BY a.shelf ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_height
            , SUM(a.bin_height) OVER (PARTITION BY a.rack ) AS total_height
        FROM bins_dump a
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ) bin
    LEFT JOIN capacity_utilization cu ON bin.mrg_key = cu.mrg_key
)

select * from final_cte