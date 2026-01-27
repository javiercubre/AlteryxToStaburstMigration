{#
    =============================================================================
    Filter Helper Macros
    Alteryx Equivalent: Filter tool
    Trino Compatible: Yes
    =============================================================================

    These macros help construct WHERE clause conditions for filtering data,
    replicating Alteryx Filter tool functionality.
#}

{#
    Macro: filter_expression
    Description: Applies a filter condition to a relation.

    Alteryx Equivalent: Filter tool (Custom mode)

    Arguments:
        relation: The source relation
        condition: SQL WHERE condition expression

    Example Usage:
        {{ filter_expression(
            relation=ref('stg_orders'),
            condition="order_amount > 100 and status = 'completed'"
        ) }}
#}

{% macro filter_expression(relation, condition) %}

select *
from {{ relation }}
where {{ condition }}

{% endmacro %}


{#
    Macro: filter_in_list
    Description: Filters rows where column value is in a list.

    Alteryx Equivalent: Filter tool with "OR" conditions

    Arguments:
        relation: The source relation
        column: Column to filter on
        values: List of values to include
        include: If true, keep matching rows; if false, exclude them (default: true)

    Example Usage:
        {{ filter_in_list(
            relation=ref('stg_orders'),
            column='status',
            values=['pending', 'processing', 'shipped']
        ) }}
#}

{% macro filter_in_list(relation, column, values, include=true) %}

select *
from {{ relation }}
where {{ column }} {% if not include %}not {% endif %}in (
    {%- for val in values %}
    '{{ val }}'{% if not loop.last %}, {% endif %}
    {%- endfor %}
)

{% endmacro %}


{#
    Macro: filter_between
    Description: Filters rows where column value is between two values.

    Alteryx Equivalent: Filter tool with range condition

    Arguments:
        relation: The source relation
        column: Column to filter on
        lower_bound: Lower bound value (inclusive)
        upper_bound: Upper bound value (inclusive)
        include_bounds: Whether bounds are inclusive (default: true)

    Example Usage:
        {{ filter_between(
            relation=ref('stg_sales'),
            column='order_date',
            lower_bound="date '2024-01-01'",
            upper_bound="date '2024-12-31'"
        ) }}
#}

{% macro filter_between(relation, column, lower_bound, upper_bound, include_bounds=true) %}

select *
from {{ relation }}
{% if include_bounds %}
where {{ column }} between {{ lower_bound }} and {{ upper_bound }}
{% else %}
where {{ column }} > {{ lower_bound }} and {{ column }} < {{ upper_bound }}
{% endif %}

{% endmacro %}


{#
    Macro: filter_null
    Description: Filters rows based on null/not null values.

    Alteryx Equivalent: Filter tool with IsNull condition

    Arguments:
        relation: The source relation
        column: Column to check
        keep_nulls: If true, keep null rows; if false, keep non-null rows (default: false)

    Example Usage:
        {{ filter_null(ref('stg_customers'), 'email', keep_nulls=false) }}
#}

{% macro filter_null(relation, column, keep_nulls=false) %}

select *
from {{ relation }}
where {{ column }} is {% if not keep_nulls %}not {% endif %}null

{% endmacro %}


{#
    Macro: filter_empty
    Description: Filters rows based on empty/non-empty string values.

    Alteryx Equivalent: Filter tool with IsEmpty condition

    Arguments:
        relation: The source relation
        column: Column to check
        keep_empty: If true, keep empty rows; if false, keep non-empty rows (default: false)
        include_null: Whether to treat null as empty (default: true)

    Example Usage:
        {{ filter_empty(ref('stg_customers'), 'phone', keep_empty=false) }}
#}

{% macro filter_empty(relation, column, keep_empty=false, include_null=true) %}

select *
from {{ relation }}
{% if keep_empty %}
where trim(coalesce({{ column }}, '')) = ''
{% else %}
where trim(coalesce({{ column }}, '')) <> ''
{% endif %}

{% endmacro %}


{#
    Macro: filter_like
    Description: Filters rows using LIKE pattern matching.

    Alteryx Equivalent: Filter tool with Contains/StartsWith/EndsWith

    Arguments:
        relation: The source relation
        column: Column to search
        pattern: LIKE pattern (use % and _ wildcards)
        case_sensitive: Whether match is case-sensitive (default: true)

    Example Usage:
        {{ filter_like(ref('stg_products'), 'product_name', '%widget%', case_sensitive=false) }}
#}

{% macro filter_like(relation, column, pattern, case_sensitive=true) %}

select *
from {{ relation }}
{% if case_sensitive %}
where {{ column }} like '{{ pattern }}'
{% else %}
where lower({{ column }}) like lower('{{ pattern }}')
{% endif %}

{% endmacro %}


{#
    Macro: filter_regex
    Description: Filters rows using regular expression matching.

    Alteryx Equivalent: Filter tool with REGEX_Match condition

    Arguments:
        relation: The source relation
        column: Column to search
        pattern: Regular expression pattern
        match: If true, keep matching rows; if false, keep non-matching (default: true)

    Example Usage:
        {{ filter_regex(ref('stg_orders'), 'order_id', '^ORD-[0-9]{6}$') }}
#}

{% macro filter_regex(relation, column, pattern, match=true) %}

select *
from {{ relation }}
where {% if not match %}not {% endif %}regexp_like({{ column }}, '{{ pattern }}')

{% endmacro %}


{#
    Macro: filter_date_range
    Description: Filters rows within a date range with common presets.

    Alteryx Equivalent: Filter tool with date conditions

    Arguments:
        relation: The source relation
        date_column: Date column to filter
        range_type: 'last_n_days', 'this_month', 'this_quarter', 'this_year',
                    'ytd', 'mtd', 'custom'
        n_days: Number of days for 'last_n_days' (default: 30)
        start_date: Start date for 'custom' range
        end_date: End date for 'custom' range

    Example Usage:
        {{ filter_date_range(ref('stg_orders'), 'order_date', range_type='last_n_days', n_days=90) }}
#}

{% macro filter_date_range(relation, date_column, range_type='last_n_days', n_days=30, start_date=none, end_date=none) %}

select *
from {{ relation }}
where
{% if range_type == 'last_n_days' %}
    {{ date_column }} >= current_date - interval '{{ n_days }}' day
{% elif range_type == 'this_month' %}
    {{ date_column }} >= date_trunc('month', current_date)
    and {{ date_column }} < date_trunc('month', current_date) + interval '1' month
{% elif range_type == 'this_quarter' %}
    {{ date_column }} >= date_trunc('quarter', current_date)
    and {{ date_column }} < date_trunc('quarter', current_date) + interval '3' month
{% elif range_type == 'this_year' %}
    {{ date_column }} >= date_trunc('year', current_date)
{% elif range_type == 'ytd' %}
    {{ date_column }} >= date_trunc('year', current_date)
    and {{ date_column }} <= current_date
{% elif range_type == 'mtd' %}
    {{ date_column }} >= date_trunc('month', current_date)
    and {{ date_column }} <= current_date
{% elif range_type == 'custom' %}
    {{ date_column }} >= {{ start_date }}
    and {{ date_column }} <= {{ end_date }}
{% else %}
    1=1
{% endif %}

{% endmacro %}


{#
    Macro: filter_multiple
    Description: Applies multiple filter conditions with AND/OR logic.

    Alteryx Equivalent: Filter tool with complex conditions

    Arguments:
        relation: The source relation
        conditions: List of condition strings
        logic: 'and' or 'or' (default: 'and')

    Example Usage:
        {{ filter_multiple(
            relation=ref('stg_orders'),
            conditions=[
                "status = 'active'",
                "amount > 100",
                "region in ('North', 'South')"
            ],
            logic='and'
        ) }}
#}

{% macro filter_multiple(relation, conditions, logic='and') %}

select *
from {{ relation }}
where (
    {%- for condition in conditions %}
    ({{ condition }}){% if not loop.last %} {{ logic }} {% endif %}
    {%- endfor %}
)

{% endmacro %}


{#
    Macro: filter_duplicates
    Description: Filters to show only duplicate or unique rows.

    Alteryx Equivalent: Filter + Unique tool combination

    Arguments:
        relation: The source relation
        key_columns: Columns that define uniqueness
        keep_type: 'duplicates' (rows appearing more than once) or 'unique' (rows appearing exactly once)

    Example Usage:
        {{ filter_duplicates(ref('stg_orders'), ['customer_id', 'order_date'], 'duplicates') }}
#}

{% macro filter_duplicates(relation, key_columns, keep_type='duplicates') %}

with counted as (
    select
        *,
        count(*) over (partition by {{ key_columns | join(', ') }}) as _occurrence_count
    from {{ relation }}
)

select
    {%- set columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from counted
{% if keep_type == 'duplicates' %}
where _occurrence_count > 1
{% else %}
where _occurrence_count = 1
{% endif %}

{% endmacro %}
