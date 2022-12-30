
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
)


SELECT
        bi.warehouse_id
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