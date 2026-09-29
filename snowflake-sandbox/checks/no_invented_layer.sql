{% do log(env_var('DBT_CLOUD_INVOCATION_CONTEXT') == 'ci', True) %}

select
    name,
    fqn[2]

from {{ info_schema('models') }}

where lower(fqn[2]) not in ('staging', 'intermediate', 'mart')