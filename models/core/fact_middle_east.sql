{{      config(materialized='table')    }}

with africa_data as (
    select *, 
        'North Africa' as place 
    from {{ ref('stg_africa') }}
), 

asia_data as (
    select *, 
        'West Asia' as place
    from {{ ref('stg_asia') }}
), 

middle_east as (
    select * from africa_data
    union all
    select * from asia_data
)

select * from middle_east

