
/*
    Staging table for Fast Start/Strong Finish data, pulling from BMI (wwaces_ddl.daily_warehouse_fact)
    - check bucketing to ensure process path capturing all/correct hours 
*/

-- config statements override anything set in dbt_project.yml file
{{ config(materialized='table', sort=['balance_date']) }}


SELECT 
    w.region_id,
    warehouseid                                             AS warehouse_id,
    TRUNC(recorddate)                                       AS balance_date,
    case 
        -- pick and stow differ here for AR and non-AR
        when scope LIKE '%Pick%'
            and coreprocess IN ('Pick','Transfer Out')
            and financefctype IN ('AR Sortable','AR Non-Sort','Quick Deploy') 
        then 'ar_pick'
        when scope LIKE '%Pick%'
           and coreprocess IN ('Pick','Transfer Out')
           and financefctype IN ('Legacy Sortable','Legacy Non-Sort','Mixed','Softline','Mixed Medium','Mixed Large','Quick Deploy','Returns','Special Handling')
        then 'non_ar_pick'
        when scope LIKE '%Stow%'
            and coreprocess IN ('Stow', 'Transfer In')
            and financefctype IN ('AR Sortable', 'AR Non-Sort', 'Quick Deploy')
            and scope NOT IN (
                '3PEPick', '3PETransPick', '3PEVretsPick',
                'Facer/Split/Pickoff Amba", "Facer/Split/Pickoff Ambassador',
                'Facer/Splitter/Pickoff T',
                'Facer/Splitter/Pickoff Training', 'Non-Scan Pick & Stage', 'Pick & Stage',
                'Pick Direct Master Sessions#Indirect', 'Pick Direct Support',
                'Pick Direct Support#Indi', 'Pick Direct Support#Indirect', 'Pick Support',
                'Pick Support Master Sess',
                'Pick Support Master Sessions',
                'Pick Support Master Sessions#Indirect', 'Pick Support#Indirect', 'Pick#Cubiscan',
                'Pick#Cubiscan#unknown', 'Pick#Exception',
                'Pick#Exception#unknown', 'Pick#Indirect', 'Pick/Stage - Support MAS',
                'Pick/Stage - Support MASTER',
                'Pick/Stage - Water Spide',
                'Pick/Stage - Water Spider', 'RC Pick Master Sessions', 'RC Pick Master Sessions#',
                'RC Pick Support',
                'RC Pick Support#Indirect',
                'Smalls Pickoff', 'Sort - Pick to Buffer', 'Splitter/Facer/Pick-Off', 'Transfer-Out Pick#Case',
                'Transfer-Out Pick#Each',
                'Transfer-Out Pick#Indire', 'Transfer-Out Pick#Indirect', 'Transfer-Out Pick#Pallet',
                'Transfer-Out Pick#unknow',
                'Transfer-Out Pick#unknown", "V-Returns Pick', 'V-Returns Pick Support', 'WHD Pick to Sp00',
                'Z-Facer/Splitter/Pickoff'
            )
        then 'ar_stow'
        when scope LIKE '%Stow%'
            and coreprocess IN ('Stow','Transfer In')
            and financefctype IN ('Legacy Sortable','Legacy Non-Sort','Mixed','Softline','Mixed Medium','Mixed Large','Quick Deploy','Returns','Special Handling')
            and scope NOT IN (
                '3PEPick', '3PETransPick', '3PEVretsPick', 'Facer/Split/Pickoff Amba", "Facer/Split/Pickoff Ambassador',
                'Facer/Splitter/Pickoff T',
                'Facer/Splitter/Pickoff Training', 'Non-Scan Pick & Stage', 'Pick & Stage',
                'Pick Direct Master Sessions#Indirect', 'Pick Direct Support',
                'Pick Direct Support#Indi', 'Pick Direct Support#Indirect', 'Pick Support', 'Pick Support Master Sess',
                'Pick Support Master Sessions',
                'Pick Support Master Sessions#Indirect', 'Pick Support#Indirect', 'Pick#Cubiscan',
                'Pick#Cubiscan#unknown', 'Pick#Exception',
                'Pick#Exception#unknown', 'Pick#Indirect', 'Pick/Stage - Support MAS', 'Pick/Stage - Support MASTER',
                'Pick/Stage - Water Spide',
                'Pick/Stage - Water Spider', 'RC Pick Master Sessions', 'RC Pick Master Sessions#', 'RC Pick Support',
                'RC Pick Support#Indirect',
                'Smalls Pickoff', 'Sort - Pick to Buffer', 'Splitter/Facer/Pick-Off', 'Transfer-Out Pick#Case',
                'Transfer-Out Pick#Each',
                'Transfer-Out Pick#Indire', 'Transfer-Out Pick#Indirect', 'Transfer-Out Pick#Pallet',
                'Transfer-Out Pick#unknow',
                'Transfer-Out Pick#unknown", "V-Returns Pick', 'V-Returns Pick Support', 'WHD Pick to Sp00',
                'Z-Facer/Splitter/Pickoff'
            )
        then 'non_ar_stow'
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
    sum(faststart_lostmillis)/3600000.0                     as faststart_lost_hrs,
    sum(strongfinish_lostmillis)/3600000.00                 as strongfinish_lost_hrs
FROM {{ source('wwaces_ddl', 'daily_warehouse_fact') }} f
join {{ ref('stg_warehouses') }} w on f.warehouseid = w.warehouse_id

WHERE true
    and recorddate = '2022-12-15'
GROUP BY 1,2,3,4