-- stg_stripe__payments.sql

with

source as (

    select * from {{ source('ecommerce','customers') }}

),

renamed as (

    select
        -- ids
        id as customer_id,

        -- strings
        Referral as referral
        Last_Ad_Seen as last_ad_seen

        -- numerics
        Time_on_site as time_on_site,
        Pages_viewed as pages_viewed,
        Cart_Value as cart_value
        Browser_Refresh_Rate as browser_refresh_rate
        Platform_Num as platforum_number


        -- booleans
        case
            when Clicked_Ad = 1 then true
            else false
        end as is_clicked_ad,
        case
            when Purchase = 1 then true
            else false
        end as is_purchased,


        -- dates
        Date_Accessed as date_accessed

    from source

)

select * from renamed