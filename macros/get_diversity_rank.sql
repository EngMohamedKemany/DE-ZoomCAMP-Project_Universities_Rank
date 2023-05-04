{# This macro returns the description of the percentage of international students #}

{% macro get_diversity_rank(stats_pc_intl_students) %}

    case 
        when {{ stats_pc_intl_students }} Between 0 and 50 then 'Mono-cultural'
        when {{ stats_pc_intl_students }} is null then 'Mono-cultural'
        else 'Multi-cultural'
    end

{% endmacro %}