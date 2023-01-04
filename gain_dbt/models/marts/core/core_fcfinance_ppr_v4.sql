
/*
    This script creates a rollup of the raw PPR actuals per process path 
*/



with core_ppr as (
        SELECT
          w.region_id,
          ppr.balance_date,
          ppr.warehouse_id,
          case
              when process_id in (29,30,31,32,15,16,17,18) and site_type in ('TNS','TSSL') then 'non_ar_stow'
              when process_id in (60,61,62,63) and site_type in ('TNS','TSSL') then 'non_ar_pick'
              when process_id in (48,49,50,51) then 'ixd_sort'
              when process_id in (97,98,99,100) then 'relo_crets'
              when process_id in (74,75,76,77) then 'afe_pack_chuting'  --pack chuting and flow sort id's were inter-changed
              when process_id in (69,70,71,72) then 'afe_flow_sort'
              when process_id in (60,61,62,63) and site_type in ('ARS','ARQD') then 'ar_pick'
              when process_id in (29,30,31,32,15,16,17,18) and site_type in ('ARS','ARQD') then 'ar_stow'
              when process_id in (79,80,81,82,83,84,85,86,87,88) then 'pack'
              when process_id in (96) then 'ship'
          end as process_path,
          sum(ppr.act_vol) as act_volume,
          sum(ppr.act_hrs) as act_hours,
          act_volume*1.0/nullif(act_hours,0) as act_uph
    from {{ ref('stg_fcfinance_dat_ppr_data_v4') }} ppr
    join {{ ref('stg_warehouses') }} w on ppr.warehouse_id = w.warehouse_id
    group by 1,2,3,4
)

select * 
from core_ppr
where act_uph is not null