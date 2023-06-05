with movements_companies as 
(
    select * from {{ref('fct_movements')}}
) , 
final as 
(
    SELECT  train_guid , count(variation_status) as count_off_route
    FROM  movements_companies
    WHERE variation_status like "OFF ROUTE"
    GROUP BY train_guid
    ORDER BY 2 desc
    LIMIT 1
)

select * from final