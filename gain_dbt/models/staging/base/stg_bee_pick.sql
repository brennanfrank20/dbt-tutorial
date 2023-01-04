
/*
    bin edit entries pick raw data with lag functions
*/

-- config statements override anything set in dbt_project.yml file
{{ config(materialized='table', sort=['snapshot_day', 'region_id']) }}


select    
    w.region_id
    , edit.warehouse_id
    , edit.person
    , edit.old_bin_id
    , edit.new_bin_id AS container
    , edit.quantity
    , edit.snapshot_day
    , edit.entry_date
    , edit.distributor_order_id
    , TRUNC(edit.entry_date) + edit.warehouse_id + edit.old_bin_id                                             AS mrg_key
    , LAG(edit.entry_date, 1) OVER (ORDER BY edit.warehouse_id, edit.person, edit.entry_date)                  AS previous_pick_time_utc
    , LAG(edit.warehouse_id, 1) OVER (ORDER BY edit.warehouse_id, edit.person, edit.entry_date)                AS previous_warehouse_id
    , LAG(edit.new_bin_id, 1) OVER (ORDER BY edit.warehouse_id, edit.person, edit.entry_date)                  AS previous_container
    , LAG(edit.old_bin_id, 1) OVER (ORDER BY edit.warehouse_id, edit.person, edit.entry_date)                  AS previous_bin
    , LAG(edit.person, 1) OVER (ORDER BY edit.warehouse_id, edit.person, edit.entry_date)                      AS previous_user_id
from {{ source('aftbi_ddl', 'bin_edit_entries') }} edit -- aftbi_ddl.bin_edit_entries
join {{ ref('stg_warehouses') }} w on edit.warehouse_id = w.warehouse_id  
where true
    and edit.region_id = 1 -- just for testing
    AND edit.snapshot_day = '2022-12-15' -- just for testing
    and edit.operation = 'm'
    and edit.distributor_order_id = 'FCPickCompleteServi'
    and SUBSTRING(edit.old_bin_id, 1, 2) = 'P-'
    and edit.new_description_code = 0


