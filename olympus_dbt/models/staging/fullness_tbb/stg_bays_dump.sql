


-- capacity utilization BAYS ONLY staging table

{{ config(materialized='table', sort=['snapshot_day']) }}


select
    TRUNC(cu.snapshot_day) AS snapshot_day
    , cu.warehouse_id
    , cu.bin_id
    , cu.bay_type
    , TRUNC(cu.snapshot_day) + cu.warehouse_id + cu.bin_id as mrg_key
FROM {{ source('olympus_capacity', 'capacity_utilization_v2') }} cu
join {{ ref('stg_warehouses') }} w on cu.warehouse_id = w.warehouse_id   
where true
    and cu.region_id = 1
    AND cu.snapshot_day = '2022-12-15'
group by 1,2,3,4,5