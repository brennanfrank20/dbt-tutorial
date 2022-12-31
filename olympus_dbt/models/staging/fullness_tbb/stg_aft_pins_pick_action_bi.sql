
/*
    staging table for aft pins pick action bi data
*/

-- config statements override anything set in dbt_project.yml file
{{ config(materialized='table', sort=['region_id']) }}

SELECT
      w.region_id
    , bi.warehouseid                AS warehouse_id
    , bi.processpath
    , bi.destinationcontainertype
    , bi.sourcebintype
    , bi.sourcecontainerscannableid
    , bi.userid
    , bi.timestamputc
    , bi.quantityaffected
    , test_cbrs_sp.identify_container_types(bi.destinationcontainerscannableid) as container_type

    -- , CASE
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tsx)'             THEN 'BLACK_TOTE'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tsblue|ts000020)' THEN 'BLUE_CART'
    --     WHEN TRUE
    --     AND LOWER(bi.destinationcontainerscannableid) ~ '^(tscag|tsout|tspic|tshigh|ts000080)'
    --     OR (
    --         bi.warehouseid IN('STR1')
    --         AND LOWER(bi.destinationcontainerscannableid) ~ '^(ts00008)'
    --     )                                                                 THEN 'CAGE'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(csx)'             THEN 'CASE'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tsot|ts000030)'   THEN 'COLOURED_TOTE'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tsdmg)'           THEN 'DAMAGES'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tsgoh)'           THEN 'GOH_CAGE'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(pax)'             THEN 'PALLET'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tspup)'           THEN 'PUP_CAGE'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tssort)'          THEN 'SILVER_CART'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(sc00)'            THEN 'SPECIAL_CART'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tsvna)'           THEN 'VNA_CAGE'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tswhd)'           THEN 'WAREHOUSE_DEALS'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(tspt)'            THEN 'YELLOW_CART'
    --     WHEN LOWER(bi.destinationcontainerscannableid) ~ '^(ts0)'             THEN 'YELLOW_TOTE'
    --     ELSE                                                                       'OTHER'
    -- END                           AS container_type_calculated
    , bi.itemweightingrams
    , bi.timebetweenscanssec
    , TRUNC(bi.timestamputc) + bi.warehouseid + bi.sourcecontainerscannableid as mrg_key


FROM {{ source('aft_pick', 'pins_pick_action_bi_v2') }} bi    --  "aft-pick-progress-platform".pins_pick_action_bi_v2 
join {{ ref('stg_warehouses') }} w on bi.warehouseid = w.warehouse_id   

     WHERE TRUE
        and bi.region_id=1 -- just for testing
        and bi.warehouseid='BWI1' -- just for testing
         AND bi.actiontype = 'PICKED'
         AND NOT LOWER(bi.processpath) ~ '^(pp(1fracs|bulkdeadwoodremoval|conso|bulkconsol|capacityconsol|dynamicconsol|fastsellingasinconsol|hazmatconsol|sameasinconsol|cubis|damag|deadw|destr|dmg|donat|dynamiccon|exc|excep|fracs|mechfracs|nonconsetdownfracs|singleopfracs|teamliftreachfracs|rebinhotpickfrac|singlefracs|38fracs|gradi|inboundproblemsolve|liqui|masst|mbfracs|nonconexc|nsreactivedeadwoodremoval|pendingresearch|probl|qa|rejec|sameasinconsolidation|space|tran|trans|stran|hazmatpodtran|pstran|6fttran|unsel|vret))'
         AND SUBSTRING(bi.sourcecontainerscannableid, 1, 5) LIKE 'P-%'
         AND (bi.Partitioned_date_column) ='2022-12-15' -- just for testing
         AND bi.timebetweenscanssec::int BETWEEN 0 AND 4000