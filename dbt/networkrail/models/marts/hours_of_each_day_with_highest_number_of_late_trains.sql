with fct_movements as (
    select * from {{ref('fct_movements')}}
),

late_variation_status_records_in_each_hour as (
    select date(actual_timestamp_utc) as actual_date,
        time_trunc(time(actual_timestamp_utc), hour) as actual_hour,
        variation_status,
        count(1) as record_count
    from fct_movements
    where variation_status like 'LATE'
    group by 1,
        2,
        3
    order by 4 desc
),
row_number_assigned as (
    select actual_date,
        actual_hour,
        variation_status,
        record_count,
        row_number() over (
            partition by actual_date
            order by record_count desc
        ) as rn
    from late_variation_status_records_in_each_hour
),
final as (
    select actual_date,
        actual_hour,
        variation_status,
        record_count
    from row_number_assigned
    where rn = 1
)
select * from final