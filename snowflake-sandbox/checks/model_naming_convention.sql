select
    name,
    case when fqn[2] == 'staging' then name ilike 'stg_%'
        else regexp_matches(name, '^(int|fct|dim)_*', 'i') end as meet_criteria

from {{ info_schema('models') }}

where case when fqn[2] == 'staging' then name not ilike 'stg_%'
    else not regexp_matches(name, '^(int|fct|dim)_*', 'i') end