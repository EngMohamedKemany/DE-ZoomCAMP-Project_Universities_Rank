{# This macro fixes a bug in the ranks from 2017 #}

{% macro get_normal_rank(year, rank_order) %}

    case 
        when {{ year }} >= 2017 then {{ rank_order }}/10
        else {{ rank_order }}
    end

{% endmacro %}