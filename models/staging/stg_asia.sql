{{      config(materialized='view')    }}

with AsiaData as 
(
    SELECT *
    FROM {{ source('staging','rankings_table_partitioned') }}
    WHERE region = "Asia"
    AND region_code = 142
    AND sub_region_code = 145
)

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['year', 'country_code']) }} as country_id,

    -- info
    cast({{ get_normal_rank('year', 'rank_order') }} as integer) as real_rank,
    cast(name as string) as name,
    cast(location as string) as country,
    cast(year as integer) as year,
    cast(coalesce(stats_pc_intl_students, 0) as integer) as intl_students,
    {{ get_diversity_rank('stats_pc_intl_students') }} as diversity_description,
    -- ranks
    cast(scores_overall_rank as integer) as scores_overall_rank,
    cast(scores_teaching_rank as integer) as scores_teaching_rank,
    cast(scores_research_rank as integer) as scores_research_rank,
    cast(scores_citations_rank as integer) as scores_citations_rank,
    -- stats
    cast(stats_number_students as integer) as stats_number_students,
    cast(stats_student_staff_ratio as numeric) as stats_student_staff_ratio,
    --academics
    cast(subjects_offered as string) as subjects_offered,

from AsiaData

{% if var('is_test_run', default=true) %}

  limit 10

{% endif %}