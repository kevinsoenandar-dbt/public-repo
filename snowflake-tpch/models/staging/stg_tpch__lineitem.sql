{#
  Note: l_shipdate is renamed to `event_date` so the cost-optimization package
  detects this column as the event_time signal for the microbatch tier. The TPC-H
  source date columns are not in the package's recognized name list otherwise.
#}

with source as (

    select
        l_orderkey      as order_id,
        l_partkey       as part_id,
        l_suppkey       as supplier_id,
        l_linenumber    as line_number,
        l_quantity      as quantity,
        l_extendedprice as extended_price,
        l_discount      as discount,
        l_tax           as tax,
        l_returnflag    as return_flag,
        l_linestatus    as line_status,
        l_shipdate      as event_date,        -- renamed for event_time detection
        l_commitdate    as commit_date,
        l_receiptdate   as receipt_date,
        l_shipinstruct  as ship_instructions,
        l_shipmode      as ship_mode,
        l_comment       as lineitem_comment

    from {{ source('tpch', 'lineitem') }}

)

select * from source
