{% macro unload_to_s3(model_name, s3_bucket, s3_path) %}
    insert overwrite directory 's3://{{ s3_bucket }}/{{ s3_path }}/{{ model_name }}' 
    using csv options (header 'true', delimiter ',', file_name '{{ model_name }}.csv')
    select * from {{ this }}
{% endmacro %}