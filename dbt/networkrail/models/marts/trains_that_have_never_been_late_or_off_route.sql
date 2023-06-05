with fct_movements AS (
        select * from {{ref('fct_movements')}}

),
agg_status_for_each_train AS (
    SELECT train_guid,
        company_name,
        array_agg(variation_status) AS variation_statuses
    FROM fct_movements
    GROUP BY  1,
        2
),
final AS (
    SELECT train_guid,
        company_name
    FROM agg_status_for_each_train
    WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST(variation_statuses) AS status
            WHERE status IN ('LATE', 'OFF ROUTE')
        )
)

SELECT train_guid , company_name FROM final 