-- `dbt run` uses Iceberg table format while `dbt test` seems fixed at Hive table format
-- Iceberg requires precision 6 timestamp values and supports timezones
-- Hive requires precision 3 timestamp values and doesn't support timezones
-- https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html#engine-versions-reference-0003-timestamp-changes
-- https://github.com/dbt-labs/dbt-athena/blob/8e2aa424256e354103d619cc07f9a79d85fadf98/dbt-athena/src/dbt/include/athena/macros/utils/timestamps.sql#L10-L19
-- https://stackoverflow.com/questions/50832977/converting-to-timestamp-with-time-zone-failed-on-athena
{%- macro utc_to_timezone(
    column_name, timezone="Australia/Sydney", model_table_type=null
) -%}
    {%- if model.config.table_type == "iceberg" or model_table_type == "iceberg" -%}
        cast({{ column_name }} as timestamp(6)) at time zone '{{ timezone }}'
    {%- else -%}
        cast(
            replace(
                cast(at_timezone({{ column_name }}, '{{ timezone }}') as varchar),
                '{{ timezone }}',
                ''
            ) as timestamp(3)
        )
    {%- endif -%}
{%- endmacro -%}
