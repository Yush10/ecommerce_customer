with

source as (

    select * from {{ source('ecommerce','cpc') }}

),

renamed as (

    select
        -- ids
        Plat_Num as Platform_Number,

        -- strings
        Platform as Platform,

        -- numerics
        Average_CPC as Avg_CPC,

    from source

)

select * from renamed