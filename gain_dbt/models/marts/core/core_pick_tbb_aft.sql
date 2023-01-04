
/*
    This script defines the logic for pick time between bin
    It pulls from "aft-pick-progress-platform".pins_pick_action_bi (not to be confused with the bin_edit_entries-sourced TBB)
    We use this as the primary source for pick time between bin, and bin edit entries as secondary, using a coalesce in the master table 
*/

with aft_pins_pick_action_bi as (
    select * from {{ref('stg_aft_pins_pick_action_bi')}} 
),

bin_tbl as (
    select * from {{ref('core_bin_tbl')}} 
),


load_table as (

        SELECT
            b.region_id
            , TRUNC(b.timestamputc) AS balance_date
            , b.warehouse_id
            , b.destinationcontainertype  		AS container_type
            , b.container_type_calculated 		AS container_type_calculated
            , b.processpath               		AS process_path
            , b.userid                    		AS person
            , b.bin_height_category
            , b.bay_type
            , b.sourcebintype             		AS bin_type
            , b.pick_area_name            		AS pick_area
            , b.userid + TRUNC(b.timestamputc)	AS mrg_key
            , SUM(b.timebetweenscanssec)  		AS secs
            , SUM(b.quantityaffected)     		AS volume
        FROM (
            SELECT
                bns.region_id
                , bi.warehouse_id
                , bi.processpath
                , bi.destinationcontainertype
                , bi.sourcebintype
                , bi.sourcecontainerscannableid
                , bi.userid
                , bi.timestamputc
                , bi.quantityaffected
                , bi.container_type_calculated
                , bi.itemweightingrams
                , bns.bay_type
                , bns.pick_area_name
                , bns.bin_height_category
                , bi.timebetweenscanssec
                , bi.mrg_key
            FROM aft_pins_pick_action_bi bi
            LEFT JOIN bin_tbl bns ON bi.mrg_key = bns.mrg_key
        ) b
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 , 11, 12
)


SELECT
    region_id
    , TRUNC(a.balance_date)                          AS balance_date
    , a.warehouse_id
    , COALESCE(a.container_type_calculated, 'OTHER') AS container_type
    , COALESCE(a.bay_type, 'OTHER')                  AS bay_type
    , COALESCE(a.bin_type, 'OTHER')                  AS bin_type
    , COALESCE(a.bin_height_category, 'OTHER')       AS bin_height_category
    , SUM(a.secs)::int                               AS secs
    , SUM(a.volume)::int                             AS volume

from load_table a
group by 1,2,3,4,5,6,7
