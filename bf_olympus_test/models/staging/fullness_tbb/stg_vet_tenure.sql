
/*
    VET Data / Tenure, pulling from daily_employee_labor_hours. check bucketing to ensure process path capturing all hours
*/

{{ config(materialized='table', sort=['balance_date']) }}




 select * from (
    select
        warehouse_id,
        balance_date,
        employee_id,
        employee_login,
        case when process_name in ('Picking', 'Pick', 'Transfer Out Pick', 'Pick Support', 'V-Returns Pick', 'RC Pick Library') then 'pick'
            when process_name in ('Stow to Prime', 'Stow to Prime Spt', 'C-Returns Stow', 'Case Stow To Reserve') then 'stow'
            when process_name in ('RC Sort') then 'ixd_sort'
            when process_name in ('C-Returns Processed', 'C-Returns Support') then 'relo_crets'
            when process_name in ('Flow Sortation', 'Sort-Flow') then 'afe_flow_sort'
            when process_name in ('Chuting') then 'afe_pack_chuting'
            when process_name in ('Pack Multis', 'Pack Singles', 'Pack Support', 'V-Returns Pack') then 'pack'
            when process_name in ('Ship Dock', 'Shipping') then 'ship'
        end as process_path,
        sum(hrs_worked) over (partition by employee_login order by balance_date rows between unbounded preceding and current row) as cumulative_hrs_worked,
        CASE WHEN cumulative_hrs_worked >= 400 THEN 'VET' ELSE 'NH' END AS tenure,
        employee_login + balance_date as mrg_key
    from (
        SELECT
            a.warehouse_id,
            a.balance_date,
            a.employee_id,
            c.employee_login,
            b.process_name,
            SUM(a.post_time_seconds)*1.0/3600 as hrs_worked

        from {{ source('olympus_vet_tenure', 'o_daily_employee_labor_hours') }} a -- aftbi_ddl_ext.o_daily_employee_labor_hours a
        join {{ ref('stg_warehouses') }} w on a.warehouse_id = w.warehouse_id
        join {{ source('olympus_transactions', 'o_labor_processes') }} b ON a.process_id = b.process_id
        LEFT JOIN (
            with employee as (select *, row_number() over (partition by emplid order by event_date desc) rnk from {{ source('olympus_employees', 'employee') }})
                select employee_login, emplid as employee_id
                from employee
                where rnk = 1
        ) AS c ON a.employee_id = c.employee_id
        WHERE TRUE
            and a.warehouse_id='BWI1'
            and a.balance_date between '2022-12-15'::date - 500 and '2022-12-15'::date
            AND a.size_category = 'Total'
        GROUP BY 1,2,3,4,5
    ) hrs
) x where process_path is not null