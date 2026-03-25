{% macro mask_column(column_name) %}
    case
        when is_member('sa_demo_group') then {{ column_name }}
        else 'REDACTED'
    end
{% endmacro %}