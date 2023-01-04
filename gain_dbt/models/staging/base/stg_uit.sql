
/*
    Staging table for Unknown Idle Time (UIT) data, pulling from BMI (wwaces_ddl.daily_warehouse_fact)
    - check bucketing to ensure process path capturing all/correct hours 
*/

-- config statements override anything set in dbt_project.yml file
{{ config(materialized='table', sort=['balance_date']) }}


SELECT 
    w.region_id,
    warehouseid                                             AS warehouse_id,
    TRUNC(recorddate)                                       AS balance_date,
    case 
        -- pick and stow are same here for AR and non-AR
        when scope LIKE '%Pick%' then 'pick'
        when scope LIKE '%Transfer-In/Stow#Each%' OR scope LIKE '%Stow#Each#Prime%' then 'stow'
        when laborprocess='RC Sort' and scope='RC Sort#Each' then 'ixd_sort'
        when scope LIKE '%Pack%' AND scope NOT IN (
            'GiftWrap Pack#Indirect',
            'GiftWrap Pack#Multi',
            'GiftWrap Pack#Single',
            'GiftWrap Pack#unknown',
            'Pack Support',
            'Pack Support Master Sessions',
            'Pack Support Master Sessions#Indirect',
            'Pack Support#Indirect',
            'Pack#Indirect',
            'V-Returns Pack'
        ) then 'pack'
        when scope in (
            'Pack#Chuting-AFE#Multi', 'Pack#Chuting-AFE#Single', 'Pack#AFE2#Multi', 'Pack#AFE2#Single', 'Pack#AFE1#Multi', 'Pack#AFE1#Single'
        ) then 'afe_pack_chuting'
        when scope in (
            'Sort Induct#AFE1', 'Sort Induct#AFE2', 'Sort Induct#AFE', 'Sort Rebin#AFE', 'Sort Rebin#AFE1', 'Sort Rebin#AFE2'
        ) then 'afe_flow_sort'
        when coreprocess = 'C-Returns' then 'relo_crets'
    end as process_path,
    SUM(total_productivemillis::FLOAT) / 3600000            AS total_productive_hours,
    SUM(totalmillis_without_indirect::FLOAT) / 3600000      AS total_direct_hours,
    SUM(unknown_idle_millis)::FLOAT8 / 3600000              AS unknown_idle_time_hours,
    SUM(total_unitcount)                                    as total_units,
    SUM(total_productivemillis)*1.0/nullif(SUM(totalmillis_without_indirect),0) AS productive_time_percentage
    -- sum(faststart_lostmillis)/3600000.0                     as faststart_lost_hrs,
    -- sum(strongfinish_lostmillis)/3600000.00                 as strongfinish_lost_hrs,
FROM {{ source('wwaces_ddl', 'daily_warehouse_fact') }} f
join {{ ref('stg_warehouses') }} w on f.warehouseid = w.warehouse_id

WHERE true
    and recorddate = '2022-12-15'
GROUP BY 1,2,3,4