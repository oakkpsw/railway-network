with movements_companies as 
(
    select * from {{ref('fct_movements')}}
) , 
final as 
(
    SELECT  company_name , count(variation_status) as count_on_time_train
    FROM  movements_companies
    WHERE variation_status like 'ON TIME'
    -- AND date(actual_timestamp_utc) >= date_add(current_date(), interval -3 day) 
    GROUP BY company_name
    ORDER BY 2 desc
    LIMIT 1
)

select company_name ,  count_on_time_train from final