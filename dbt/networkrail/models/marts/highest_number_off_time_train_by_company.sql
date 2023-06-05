with movements_companies as 
(
    select * from {{ref('fct_movements')}}
) , 
final as 
(
    SELECT   company_name , count(variation_status) as count_late_train
    FROM  movements_companies
    WHERE variation_status like 'LATE'
    -- and date(actual_timestamp_utc) >= date_add(current_date(), interval -3 day) 
    GROUP BY company_name
    ORDER BY 2 desc
    LIMIT 1
)

select company_name, count_late_train from final