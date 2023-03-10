
/*
    staging table for FC (warehouse) level data
*/

-- config statements override anything set in dbt_project.yml file
{{ config(materialized='table') }}

with warehouses as (

    select regionid as region_id,
           physical_country,
           warehouse_id,
           site_type
    from ( 
        select warehouse_id, site_type, physical_country,
        ----- our warehouse table uses region code instead of region ID -----
            case region_id
            when 'NA' then 1
            when 'EU' then 2
            when 'FE' then 3
            when 'IN' then 4
            when 'SA' then 5
        	when 'ECCF' then 5
            end as regionid
        from {{ source('test_brs_storedproc', 'dat_brs_ranking_group_eb_update') }} -- test_brs_storedproc.dat_brs_ranking_group_eb_update
        where scenario_id=41
        group by 1,2,3,4
    ) x 
    
    where regionid = 1
    
    group by 1,2,3,4

)

select *
from warehouses
where true
    and warehouse_id is not null
    and warehouse_id = 'BWI1'