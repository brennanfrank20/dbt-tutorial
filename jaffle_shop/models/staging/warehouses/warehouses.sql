
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}


with warehouses as (

    ----- FCs with region partitioning -----
    select regionid as region_id,
           physical_country,
           warehouse_id,
           site_type
    from (
        select warehouse_id,site_type,physical_country,
            case region_id
            when 'NA' then 1
            when 'EU' then 2
            when 'FE' then 3
            when 'IN' then 4
            when 'SA' then 5
        	when 'ECCF' then 5
            end as regionid
        from test_brs_storedproc.dat_brs_ranking_group_eb_update
        where scenario_id=41 --and upper(site_type) in ('TSSL', 'TNS')
        group by 1,2,3,4
    ) x where regionid in (3) --and warehouse_id='TYO2'
    group by 1,2,3,4

)

select *
from warehouses
where warehouse_id is not null