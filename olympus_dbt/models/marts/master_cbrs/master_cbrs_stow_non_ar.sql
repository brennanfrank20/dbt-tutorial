
/*
    This script combines the master_stow_non_ar table with its ASP counterpart to pull targets and cbrs hours

*/





   
select

    t0.region_id,
    t0.warehouse_id,
    n.ranking_grp as ranking_group,
    t0.balance_date,
    t0.bay_type,
    t0.bin_type,
    t0.container_type,
    t0.fullness_type,

    t0.act_vol,
    t0.act_hrs,
    t0.act_uph,
    t0.cube,
    t0.capacity,
    t0.fullness_pct,

    t0.vet_emps,
    t0.total_emps,
    t0.vet_pct,    
    vet_target,

    t0.uit_hrs,
    t0.total_direct_hrs,
    t0.uit_pct,
    t0.faststart_lost_hrs,
    t0.strongfinish_lost_hrs,
    

    -- UPT
    -- sum(t0.container_volume) * 1.0 / nullif(sum(t0.containers), 0) * 1.0 as stt_upt_act,
    stt_upt_target,
    -- UIT
    -- sum(t0.uit_hrs) * 1.0 / nullif(sum(t0.uit_tot_hrs), 0) * 1.0 as stt_uit_act,
    stt_uit_target,
    -- ACU
    -- sum(t0.cube) * 1.0 / 1728*1.0 / nullif(sum(t0.units), 0) * 1.0 as stt_acu_act,
    stt_acu_target,
    -- CCT
    -- sum(t0.changeovertimeminutes) * 1.0 / nullif(sum(t0.changeoverinstances), 0) * 1.0 as stt_cct_act,
    stt_cct_target,
    -- APC
    -- sum(t0.ct_aisles) * 1.0 / nullif(sum(t0.ct_cages), 0) * 1.0 as stt_apc_act,
    stt_apc_target,
    -- BPA
    -- sum(t0.ct_bins_stowed) * 1.0 / nullif(sum(t0.ct_aisles), 0) * 1.0 as stt_bpa_act,
    stt_bpa_target,
    -- TLST
    -- sum(t0.total_seconds_lost) * 1.0 / nullif(sum(t0.total_seconds), 0) * 1.0 as stt_tlst_act,
    stt_tlst_target,
    -- Bin Col
    -- sum(t0.total_collisions) * 1.0 / nullif(sum(t0.total_collision_transactions), 0) * 1000000.0 as stt_bincol_act,
    stt_bincol_target,
    -- Mixed %
    -- sum(t0.mixed_cages) * 1.0 / nullif(sum(t0.cages), 0) * 1.0 as stt_mix_act,
    stt_mix_target,

    cbrs_hours,
    cbrs_hours_act_stt,
    cbrs_hours_stt_upt,
    cbrs_hours_stt_acu,
    cbrs_hours_stt_uit,
    cbrs_hours_stt_cct,
    cbrs_hours_stt_apc,
    cbrs_hours_stt_bpa,
    cbrs_hours_stt_tlst,
    cbrs_hours_stt_bincol,
    cbrs_hours_stt_mix


from {{ref('master_stow_non_ar')}} t0
left join {{ source('cbrs_uph_impacts', 'non_ar_stow') }} n on true
    and n.scenario_id=41
    and n.batch_id = 'cbrs_master'
    and t0.warehouse_id = n.warehouse_id
    and trunc(t0.balance_date::date) = trunc(n.start_date::date)
    and t0.bay_type = n.baytype
    and t0.bin_type = n.bintype
    and t0.container_type = n.containertype
    and t0.fullness_type = n.fullness_type
where true


