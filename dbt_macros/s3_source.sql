{#
    =============================================================================
    S3 Source Macros
    Alteryx Equivalent: Input Data tool (S3/cloud sources)
    Trino Compatible: Yes (requires Hive connector with S3 support)
    =============================================================================

    These macros help read data from S3-compatible storage (AWS S3, MinIO, etc.)
    into Trino/Starburst, replicating Alteryx S3 Input functionality.
#}

{#
    Macro: s3_source
    Description: References an S3 external table source with optional metadata columns.

    Alteryx Equivalent: Input Data tool (S3 connection)

    Arguments:
        source_name: DBT source name (typically 's3_raw')
        table_name: Table name in sources.yml
        include_metadata: Include S3 file metadata columns (default: true)

    Example Usage:
        {{ s3_source(
            source_name='s3_raw',
            table_name='customers'
        ) }}
#}

{% macro s3_source(source_name, table_name, include_metadata=true) %}

with source_data as (
    select * from {{ source(source_name, table_name) }}
)

{% if include_metadata %}
, with_metadata as (
    select
        *,
        "$path" as _source_file,
        "$file_modified_time" as _source_modified_at
    from source_data
)

select * from with_metadata
{% else %}
select * from source_data
{% endif %}

{% endmacro %}


{#
    Macro: s3_incremental_source
    Description: Reads S3 source with incremental loading based on file modification time.

    Alteryx Equivalent: Input Data tool with date/time filtering

    Arguments:
        source_name: DBT source name
        table_name: Table name in sources.yml
        incremental_column: Column to use for incremental loading (default: _source_modified_at)
        lookback_hours: Hours to look back for incremental (default: 24)

    Example Usage:
        {{ s3_incremental_source(
            source_name='s3_raw',
            table_name='events',
            lookback_hours=48
        ) }}
#}

{% macro s3_incremental_source(source_name, table_name, incremental_column='_source_modified_at', lookback_hours=24) %}

with source_data as (
    select
        *,
        "$path" as _source_file,
        "$file_modified_time" as _source_modified_at
    from {{ source(source_name, table_name) }}
)

{% if is_incremental() %}
, filtered as (
    select *
    from source_data
    where {{ incremental_column }} > (
        select coalesce(max({{ incremental_column }}), timestamp '1970-01-01')
        from {{ this }}
    )
    -- Also include recent files within lookback window for late-arriving data
    or {{ incremental_column }} >= current_timestamp - interval '{{ lookback_hours }}' hour
)

select * from filtered
{% else %}
select * from source_data
{% endif %}

{% endmacro %}


{#
    Macro: s3_source_with_schema
    Description: Reads S3 source with explicit column selection and type casting.

    Alteryx Equivalent: Input Data tool with Select tool for schema definition

    Arguments:
        source_name: DBT source name
        table_name: Table name in sources.yml
        columns: List of dicts with 'name' and optional 'type', 'alias' keys
        include_metadata: Include S3 file metadata columns (default: true)

    Example Usage:
        {{ s3_source_with_schema(
            source_name='s3_raw',
            table_name='orders',
            columns=[
                {'name': 'order_id', 'type': 'varchar'},
                {'name': 'order_date', 'type': 'date'},
                {'name': 'amount', 'type': 'decimal(10,2)', 'alias': 'order_amount'}
            ]
        ) }}
#}

{% macro s3_source_with_schema(source_name, table_name, columns, include_metadata=true) %}

with source_data as (
    select
        {%- for col in columns %}
        {% if col.type is defined %}
        cast("{{ col.name }}" as {{ col.type }})
        {%- else %}
        "{{ col.name }}"
        {%- endif %}
        {%- if col.alias is defined %} as {{ col.alias }}{% else %} as "{{ col.name }}"{% endif %}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
        {%- if include_metadata %},
        "$path" as _source_file,
        "$file_modified_time" as _source_modified_at
        {%- endif %}
    from {{ source(source_name, table_name) }}
)

select * from source_data

{% endmacro %}


{#
    Macro: s3_validate_source
    Description: Validates S3 source data with quality checks.

    Alteryx Equivalent: Input Data + Data Cleansing tools

    Arguments:
        source_name: DBT source name
        table_name: Table name in sources.yml
        filter_invalid: Remove rows that fail validation (default: true)
        required_columns: List of column names that must not be null

    Example Usage:
        {{ s3_validate_source(
            source_name='s3_raw',
            table_name='customers',
            required_columns=['customer_id', 'email']
        ) }}
#}

{% macro s3_validate_source(source_name, table_name, filter_invalid=true, required_columns=[]) %}

with source_data as (
    select
        *,
        "$path" as _source_file,
        "$file_modified_time" as _source_modified_at
    from {{ source(source_name, table_name) }}
),

validated as (
    select
        *,
        -- Validation flags
        case
            when "$path" is null then false
            {%- for col in required_columns %}
            when "{{ col }}" is null then false
            {%- endfor %}
            else true
        end as _is_valid,
        -- Validation reason
        case
            when "$path" is null then 'missing_file_path'
            {%- for col in required_columns %}
            when "{{ col }}" is null then 'null_{{ col }}'
            {%- endfor %}
            else null
        end as _validation_error
    from source_data
)

{% if filter_invalid %}
select
    {# Select all columns except validation columns if filtering #}
    {{ dbt_utils.star(from=source(source_name, table_name)) }},
    _source_file,
    _source_modified_at
from validated
where _is_valid = true
{% else %}
select * from validated
{% endif %}

{% endmacro %}


{#
    Macro: s3_union_partitions
    Description: Unions multiple S3 partitions with partition column extraction.

    Alteryx Equivalent: Wildcard Input + RegEx Parse for partition extraction

    Arguments:
        source_name: DBT source name
        table_name: Table name in sources.yml
        partition_column: Name for extracted partition column
        partition_pattern: Regex pattern to extract partition from $path

    Example Usage:
        {{ s3_union_partitions(
            source_name='s3_raw',
            table_name='events',
            partition_column='event_date',
            partition_pattern='year=(\d{4})/month=(\d{2})/day=(\d{2})'
        ) }}
#}

{% macro s3_union_partitions(source_name, table_name, partition_column='partition_value', partition_pattern=none) %}

with source_data as (
    select
        *,
        "$path" as _source_file,
        "$file_modified_time" as _source_modified_at
        {%- if partition_pattern %},
        regexp_extract("$path", '{{ partition_pattern }}', 1) as {{ partition_column }}
        {%- endif %}
    from {{ source(source_name, table_name) }}
)

select * from source_data

{% endmacro %}


{#
    Macro: s3_deduplicate_files
    Description: Deduplicates data when files may have been reprocessed.

    Alteryx Equivalent: Unique tool after S3 Input

    Arguments:
        source_name: DBT source name
        table_name: Table name in sources.yml
        unique_key: Column or list of columns that define uniqueness
        prefer_latest: Keep the record from the most recently modified file (default: true)

    Example Usage:
        {{ s3_deduplicate_files(
            source_name='s3_raw',
            table_name='customers',
            unique_key='customer_id'
        ) }}
#}

{% macro s3_deduplicate_files(source_name, table_name, unique_key, prefer_latest=true) %}

with source_data as (
    select
        *,
        "$path" as _source_file,
        "$file_modified_time" as _source_modified_at
    from {{ source(source_name, table_name) }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by {% if unique_key is string %}{{ unique_key }}{% else %}{{ unique_key | join(', ') }}{% endif %}
            order by _source_modified_at {% if prefer_latest %}desc{% else %}asc{% endif %}
        ) as _rn
    from source_data
)

select
    {{ dbt_utils.star(from=source(source_name, table_name)) }},
    _source_file,
    _source_modified_at
from ranked
where _rn = 1

{% endmacro %}
