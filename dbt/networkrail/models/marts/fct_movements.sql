with 
movements_joined_operating_companies as 
(
    select * from {{ref('int_networkrail__movements_joined_operating_compaines')}}


) ,
final as (

    select
        event_type
        , actual_timestamp_utc
        , event_source
        , train_guid
        , toc_guid
        , company_name
        , variation_status

    from movements_joined_operating_companies

)

select * from final