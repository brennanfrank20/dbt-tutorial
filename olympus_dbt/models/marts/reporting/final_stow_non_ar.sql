
/*
    This script takes the master cbrs stow non ar daily data and multiplies it out for weekly and monthly intervals 

*/





-- Daily
select
    'Daily' as intervl,
    region_id,
    warehouse_id,
    ranking_group,
    balance_date,
    bay_type,
    bin_type,
    container_type,
    fullness_type,

    sum(act_vol) as act_volume,
    sum(act_hrs) as act_hours,
    act_volume*1.0/nullif(act_hours,0) as act_uph,
    sum(cube) as total_cube,
    sum(capacity) as total_capacity,
    total_cube*1.0/nullif(total_capacity,0) as fullness_pct,

    sum(vet_emps) as vet_employees,
    sum(total_emps) as total_employees,
    vet_employees*1.0/nullif(total_employees,0) as vet_pct,    
    avg(vet_target) as vet_target,

    sum(uit_hrs) as uit_hours,
    sum(total_direct_hrs) as total_direct_hours,
    uit_hours*1.0/nullif(total_direct_hours,0) as uit_pct,
    sum(faststart_lost_hrs) as faststart_lost_hours,
    sum(strongfinish_lost_hrs) as strongfinish_lost_hours,
    
    avg(stt_upt_target) as stt_upt_target,
    avg(stt_uit_target) as stt_uit_target,
    avg(stt_acu_target) as stt_acu_target,
    avg(stt_cct_target) as stt_cct_target,
    avg(stt_apc_target) as stt_apc_target,
    avg(stt_bpa_target) as stt_bpa_target,
    avg(stt_tlst_target) as stt_tlst_target,
    avg(stt_bincol_target) as stt_bincol_target,
    avg(stt_mix_target) as stt_mix_target,

    sum(cbrs_hours) as cbrs_hours,
    sum(cbrs_hours_act_stt) as cbrs_hours_act_stt,
    sum(cbrs_hours_stt_upt) as cbrs_hours_stt_upt,
    sum(cbrs_hours_stt_acu) as cbrs_hours_stt_acu,
    sum(cbrs_hours_stt_uit) as cbrs_hours_stt_uit,
    sum(cbrs_hours_stt_cct) as cbrs_hours_stt_cct,
    sum(cbrs_hours_stt_apc) as cbrs_hours_stt_apc,
    sum(cbrs_hours_stt_bpa) as cbrs_hours_stt_bpa,
    sum(cbrs_hours_stt_tlst) as cbrs_hours_stt_tlst,
    sum(cbrs_hours_stt_bincol) as cbrs_hours_stt_bincol,
    sum(cbrs_hours_stt_mix) as cbrs_hours_stt_mix

from {{ref('master_cbrs_stow_non_ar')}}
-- where balance_date between current_date-9 and current_date-3
group by 1,2,3,4,5,6,7,8,9

union all
-- Weekly
select
    'Weekly' as intervl,
    region_id,
    warehouse_id,
    ranking_group,
    balance_date,
    bay_type,
    bin_type,
    container_type,
    fullness_type,

    sum(act_vol) as act_volume,
    sum(act_hrs) as act_hours,
    act_volume*1.0/nullif(act_hours,0) as act_uph,
    sum(cube) as total_cube,
    sum(capacity) as total_capacity,
    total_cube*1.0/nullif(total_capacity,0) as fullness_pct,

    sum(vet_emps) as vet_employees,
    sum(total_emps) as total_employees,
    vet_employees*1.0/nullif(total_employees,0) as vet_pct,    
    avg(vet_target) as vet_target,

    sum(uit_hrs) as uit_hours,
    sum(total_direct_hrs) as total_direct_hours,
    uit_hours*1.0/nullif(total_direct_hours,0) as uit_pct,
    sum(faststart_lost_hrs) as faststart_lost_hours,
    sum(strongfinish_lost_hrs) as strongfinish_lost_hours,
    
    avg(stt_upt_target) as stt_upt_target,
    avg(stt_uit_target) as stt_uit_target,
    avg(stt_acu_target) as stt_acu_target,
    avg(stt_cct_target) as stt_cct_target,
    avg(stt_apc_target) as stt_apc_target,
    avg(stt_bpa_target) as stt_bpa_target,
    avg(stt_tlst_target) as stt_tlst_target,
    avg(stt_bincol_target) as stt_bincol_target,
    avg(stt_mix_target) as stt_mix_target,

    sum(cbrs_hours) as cbrs_hours,
    sum(cbrs_hours_act_stt) as cbrs_hours_act_stt,
    sum(cbrs_hours_stt_upt) as cbrs_hours_stt_upt,
    sum(cbrs_hours_stt_acu) as cbrs_hours_stt_acu,
    sum(cbrs_hours_stt_uit) as cbrs_hours_stt_uit,
    sum(cbrs_hours_stt_cct) as cbrs_hours_stt_cct,
    sum(cbrs_hours_stt_apc) as cbrs_hours_stt_apc,
    sum(cbrs_hours_stt_bpa) as cbrs_hours_stt_bpa,
    sum(cbrs_hours_stt_tlst) as cbrs_hours_stt_tlst,
    sum(cbrs_hours_stt_bincol) as cbrs_hours_stt_bincol,
    sum(cbrs_hours_stt_mix) as cbrs_hours_stt_mix

from {{ref('master_cbrs_stow_non_ar')}}
-- where balance_date between date_add('week', -4, date_trunc('week', current_date+1)-1 ) and date_trunc('week', current_date+1)-2
group by 1,2,3,4,5,6,7,8,9


union all
-- Monthly
select
    'Monthly' as intervl,
    region_id,
    warehouse_id,
    ranking_group,
    balance_date,
    bay_type,
    bin_type,
    container_type,
    fullness_type,

    sum(act_vol) as act_volume,
    sum(act_hrs) as act_hours,
    act_volume*1.0/nullif(act_hours,0) as act_uph,
    sum(cube) as total_cube,
    sum(capacity) as total_capacity,
    total_cube*1.0/nullif(total_capacity,0) as fullness_pct,

    sum(vet_emps) as vet_employees,
    sum(total_emps) as total_employees,
    vet_employees*1.0/nullif(total_employees,0) as vet_pct,    
    avg(vet_target) as vet_target,

    sum(uit_hrs) as uit_hours,
    sum(total_direct_hrs) as total_direct_hours,
    uit_hours*1.0/nullif(total_direct_hours,0) as uit_pct,
    sum(faststart_lost_hrs) as faststart_lost_hours,
    sum(strongfinish_lost_hrs) as strongfinish_lost_hours,
    
    avg(stt_upt_target) as stt_upt_target,
    avg(stt_uit_target) as stt_uit_target,
    avg(stt_acu_target) as stt_acu_target,
    avg(stt_cct_target) as stt_cct_target,
    avg(stt_apc_target) as stt_apc_target,
    avg(stt_bpa_target) as stt_bpa_target,
    avg(stt_tlst_target) as stt_tlst_target,
    avg(stt_bincol_target) as stt_bincol_target,
    avg(stt_mix_target) as stt_mix_target,

    sum(cbrs_hours) as cbrs_hours,
    sum(cbrs_hours_act_stt) as cbrs_hours_act_stt,
    sum(cbrs_hours_stt_upt) as cbrs_hours_stt_upt,
    sum(cbrs_hours_stt_acu) as cbrs_hours_stt_acu,
    sum(cbrs_hours_stt_uit) as cbrs_hours_stt_uit,
    sum(cbrs_hours_stt_cct) as cbrs_hours_stt_cct,
    sum(cbrs_hours_stt_apc) as cbrs_hours_stt_apc,
    sum(cbrs_hours_stt_bpa) as cbrs_hours_stt_bpa,
    sum(cbrs_hours_stt_tlst) as cbrs_hours_stt_tlst,
    sum(cbrs_hours_stt_bincol) as cbrs_hours_stt_bincol,
    sum(cbrs_hours_stt_mix) as cbrs_hours_stt_mix

from {{ref('master_cbrs_stow_non_ar')}}
-- where balance_date between date_add('month', -13, date_trunc('month', current_date)) and date_trunc('month', current_date)-1
group by 1,2,3,4,5,6,7,8,9