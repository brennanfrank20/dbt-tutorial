
/*
    This script defines logic for pick time between bin
    It pulls from bin edit entries table (slightly different than aft-pick-progress-platform)
*/


with bee_pick as (
    select * from {{ref('stg_bee_pick')}}
),

bin_tbl as (
    select * from {{ref('core_bin_tbl')}} 
),


load_table as (

    SELECT 
        region_id
        , TRUNC(b.entry_date)    AS balance_date
        , b.warehouse_id
        , b.container_type
        , b.mod
        , b.bay_type
        , b.bin_type_name        AS bin_type
        , b.pick_area_name       AS pick_area
        , b.bin_height_category
        , b.person
        , SUM(b.cycle_time_secs) AS cycle_time_secs
        , SUM(b.units)           AS units
    FROM (
        SELECT
            region_id
            , a.entry_date
            , a.warehouse_id
            , a.person
            , CASE
                    WHEN LOWER(a.container) ~ '^(tsx)' THEN 'BLACK_TOTE'
                    WHEN LOWER(a.container) ~ '^(tsblue|ts000020)' THEN 'BLUE_CART'
                    WHEN TRUE
                            AND LOWER(a.container) ~ '^(tscag|tsout|tspic|tshigh|ts000080)'
                        OR (
                                    a.warehouse_id IN ('STR1')
                                AND LOWER(a.container) ~ '^(ts00008)'
                            ) THEN 'CAGE'
                    WHEN LOWER(a.container) ~ '^(csx)' THEN 'CASE'
                    WHEN LOWER(a.container) ~ '^(tsot|ts000030)' THEN 'COLOURED_TOTE'
                    WHEN LOWER(a.container) ~ '^(tsdmg)' THEN 'DAMAGES'
                    WHEN LOWER(a.container) ~ '^(tsgoh)' THEN 'GOH_CAGE'
                    WHEN LOWER(a.container) ~ '^(pax)' THEN 'PALLET'
                    WHEN LOWER(a.container) ~ '^(tspup)' THEN 'PUP_CAGE'
                    WHEN LOWER(a.container) ~ '^(tssort)' THEN 'SILVER_CART'
                    WHEN LOWER(a.container) ~ '^(sc00)' THEN 'SPECIAL_CART'
                    WHEN LOWER(a.container) ~ '^(tsvna)' THEN 'VNA_CAGE'
                    WHEN LOWER(a.container) ~ '^(tswhd)' THEN 'WAREHOUSE_DEALS'
                    WHEN LOWER(a.container) ~ '^(tspt)' THEN 'YELLOW_CART'
                    WHEN LOWER(a.container) ~ '^(ts0)' THEN 'YELLOW_TOTE'
                    ELSE 'OTHER'
            END                                                         AS container_type
            , substring(a.old_bin_id, 1, 5)                             AS mod
            , a.bay_type
            , a.bin_type_name
            , a.pick_area_name
            , a.bin_height_category
            , datediff(seconds, a.previous_pick_time_utc, a.entry_date) AS cycle_time_secs
            , a.quantity                                                AS units
        from (
                select
                    bns.region_id
                    , bee.warehouse_id
                    , bee.person
                    , bee.old_bin_id
                    , bee.container
                    , bee.quantity
                    , bee.entry_date
                    , bee.previous_user_id
                    , bee.previous_warehouse_id
                    , bee.previous_pick_time_utc
                    , bns.bay_type
                    , bns.bin_type_name
                    , bns.pick_area_name
                    , bns.bin_height_category
                from bee_pick bee
                left join bin_tbl bns ON bee.mrg_key = bns.mrg_key
        ) a
        where true
            and a.warehouse_id = previous_warehouse_id
            and a.person = previous_user_id
            and cycle_time_secs between 0 and 4000
    ) b
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10


)

 select 
    region_id
    , balance_date
    , warehouse_id
    , container_type
    , bay_type
    , bin_type
    , bin_height_category
    , SUM(cycle_time_secs) AS secs
    , SUM(units)           AS volume
from load_table
group by 1,2,3,4,5,6,7
