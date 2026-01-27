{#
    =============================================================================
    Imputation Macros
    Alteryx Equivalent: Imputation tool
    Trino Compatible: Yes
    =============================================================================

    These macros provide missing value imputation functionality including
    mean, median, mode, and custom imputation strategies.
#}

{#
    Macro: impute_with_value
    Description: Replace null values with a specified value.

    Alteryx Equivalent: Imputation tool (user-specified value mode)

    Arguments:
        column: Column to impute
        value: Value to use for imputation

    Example Usage:
        {{ impute_with_value('score', 0) }}
#}

{% macro impute_with_value(column, value) %}
coalesce({{ column }}, {{ value }})
{% endmacro %}


{#
    Macro: impute_with_mean
    Description: Replace null values with the mean of the column.

    Alteryx Equivalent: Imputation tool (average mode)

    Arguments:
        relation: Source relation
        column: Column to impute
        group_by: Optional columns for grouped imputation

    Example Usage:
        {{ impute_with_mean(ref('stg_sales'), 'quantity') }}
        {{ impute_with_mean(ref('stg_sales'), 'quantity', group_by=['region']) }}
#}

{% macro impute_with_mean(relation, column, group_by=none) %}

{% if group_by %}
with avg_values as (
    select
        {{ group_by | join(', ') }},
        avg({{ column }}) as _impute_value
    from {{ relation }}
    where {{ column }} is not null
    group by {{ group_by | join(', ') }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    coalesce(t.{{ column }}, a._impute_value) as {{ column }}
        {%- else %}
    t.{{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }} t
left join avg_values a on
    {%- for gc in group_by %}
    t.{{ gc }} = a.{{ gc }}{% if not loop.last %} and {% endif %}
    {%- endfor %}
{% else %}
select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    coalesce({{ column }}, (select avg({{ column }}) from {{ relation }} where {{ column }} is not null)) as {{ column }}
        {%- else %}
    {{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}
{% endif %}

{% endmacro %}


{#
    Macro: impute_with_median
    Description: Replace null values with the median of the column.

    Alteryx Equivalent: Imputation tool (median mode)

    Arguments:
        relation: Source relation
        column: Column to impute
        group_by: Optional columns for grouped imputation

    Example Usage:
        {{ impute_with_median(ref('stg_orders'), 'order_amount') }}
#}

{% macro impute_with_median(relation, column, group_by=none) %}

{% if group_by %}
with median_values as (
    select
        {{ group_by | join(', ') }},
        approx_percentile({{ column }}, 0.5) as _impute_value
    from {{ relation }}
    where {{ column }} is not null
    group by {{ group_by | join(', ') }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    coalesce(t.{{ column }}, m._impute_value) as {{ column }}
        {%- else %}
    t.{{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }} t
left join median_values m on
    {%- for gc in group_by %}
    t.{{ gc }} = m.{{ gc }}{% if not loop.last %} and {% endif %}
    {%- endfor %}
{% else %}
select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    coalesce({{ column }}, (select approx_percentile({{ column }}, 0.5) from {{ relation }} where {{ column }} is not null)) as {{ column }}
        {%- else %}
    {{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}
{% endif %}

{% endmacro %}


{#
    Macro: impute_with_mode
    Description: Replace null values with the most frequent value.

    Alteryx Equivalent: Imputation tool (mode)

    Arguments:
        relation: Source relation
        column: Column to impute
        group_by: Optional columns for grouped imputation

    Example Usage:
        {{ impute_with_mode(ref('stg_customers'), 'country') }}
#}

{% macro impute_with_mode(relation, column, group_by=none) %}

{% if group_by %}
with mode_values as (
    select
        {{ group_by | join(', ') }},
        {{ column }} as _impute_value,
        row_number() over (partition by {{ group_by | join(', ') }} order by count(*) desc) as _rn
    from {{ relation }}
    where {{ column }} is not null
    group by {{ group_by | join(', ') }}, {{ column }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    coalesce(t.{{ column }}, m._impute_value) as {{ column }}
        {%- else %}
    t.{{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }} t
left join mode_values m on
    {%- for gc in group_by %}
    t.{{ gc }} = m.{{ gc }}{% if not loop.last %} and {% endif %}
    {%- endfor %}
    and m._rn = 1
{% else %}
with mode_value as (
    select {{ column }} as _impute_value
    from {{ relation }}
    where {{ column }} is not null
    group by {{ column }}
    order by count(*) desc
    limit 1
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    coalesce(t.{{ column }}, m._impute_value) as {{ column }}
        {%- else %}
    t.{{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }} t
cross join mode_value m
{% endif %}

{% endmacro %}


{#
    Macro: impute_forward_fill
    Description: Fill null values with the previous non-null value (LOCF).

    Alteryx Equivalent: Multi-Row Formula for forward fill

    Arguments:
        relation: Source relation
        column: Column to impute
        order_by: Columns to order by (determines "previous")
        partition_by: Optional partition columns

    Example Usage:
        {{ impute_forward_fill(
            relation=ref('stg_timeseries'),
            column='value',
            order_by=['date'],
            partition_by=['product_id']
        ) }}
#}

{% macro impute_forward_fill(relation, column, order_by, partition_by=none) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    coalesce(
        {{ column }},
        last_value({{ column }}) ignore nulls over (
            {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
            order by {{ order_by | join(', ') }}
            rows between unbounded preceding and 1 preceding
        )
    ) as {{ column }}
        {%- else %}
    {{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: impute_backward_fill
    Description: Fill null values with the next non-null value.

    Alteryx Equivalent: Multi-Row Formula for backward fill

    Arguments:
        relation: Source relation
        column: Column to impute
        order_by: Columns to order by
        partition_by: Optional partition columns

    Example Usage:
        {{ impute_backward_fill(ref('stg_data'), 'price', ['date']) }}
#}

{% macro impute_backward_fill(relation, column, order_by, partition_by=none) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    coalesce(
        {{ column }},
        first_value({{ column }}) ignore nulls over (
            {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
            order by {{ order_by | join(', ') }}
            rows between 1 following and unbounded following
        )
    ) as {{ column }}
        {%- else %}
    {{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: impute_linear_interpolation
    Description: Linear interpolation for missing numeric values.

    Alteryx Equivalent: Custom formula for interpolation

    Arguments:
        relation: Source relation
        value_column: Column to interpolate
        order_column: Column defining order (usually date/time)
        partition_by: Optional partition columns

    Example Usage:
        {{ impute_linear_interpolation(
            relation=ref('stg_metrics'),
            value_column='measurement',
            order_column='timestamp'
        ) }}
#}

{% macro impute_linear_interpolation(relation, value_column, order_column, partition_by=none) %}

with with_neighbors as (
    select
        *,
        last_value({{ value_column }}) ignore nulls over (
            {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
            order by {{ order_column }}
            rows between unbounded preceding and 1 preceding
        ) as _prev_value,
        last_value({{ order_column }}) ignore nulls over (
            {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
            order by {{ order_column }}
            rows between unbounded preceding and 1 preceding
        ) as _prev_order,
        first_value({{ value_column }}) ignore nulls over (
            {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
            order by {{ order_column }}
            rows between 1 following and unbounded following
        ) as _next_value,
        first_value({{ order_column }}) ignore nulls over (
            {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
            order by {{ order_column }}
            rows between 1 following and unbounded following
        ) as _next_order
    from {{ relation }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == value_column %}
    coalesce(
        {{ value_column }},
        case
            when _prev_value is not null and _next_value is not null
            then _prev_value + (
                (_next_value - _prev_value) *
                (date_diff('second', _prev_order, {{ order_column }}) * 1.0 /
                 nullif(date_diff('second', _prev_order, _next_order), 0))
            )
            when _prev_value is not null then _prev_value
            when _next_value is not null then _next_value
        end
    ) as {{ value_column }}
        {%- else %}
    {{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from with_neighbors

{% endmacro %}


{#
    Macro: impute_multiple_columns
    Description: Apply imputation to multiple columns at once.

    Alteryx Equivalent: Imputation tool with multiple fields

    Arguments:
        relation: Source relation
        imputation_config: Dictionary of column: {method: 'mean'|'median'|'mode'|'value', value: x}

    Example Usage:
        {{ impute_multiple_columns(
            relation=ref('stg_data'),
            imputation_config={
                'age': {'method': 'median'},
                'income': {'method': 'mean'},
                'country': {'method': 'mode'},
                'status': {'method': 'value', 'value': "'Unknown'"}
            }
        ) }}
#}

{% macro impute_multiple_columns(relation, imputation_config) %}

with
{% for col, config in imputation_config.items() %}
    {% if config.method == 'mean' %}
_{{ col }}_stat as (
    select avg({{ col }}) as _impute_val from {{ relation }} where {{ col }} is not null
),
    {% elif config.method == 'median' %}
_{{ col }}_stat as (
    select approx_percentile({{ col }}, 0.5) as _impute_val from {{ relation }} where {{ col }} is not null
),
    {% elif config.method == 'mode' %}
_{{ col }}_stat as (
    select {{ col }} as _impute_val
    from {{ relation }}
    where {{ col }} is not null
    group by {{ col }}
    order by count(*) desc
    limit 1
),
    {% endif %}
{% endfor %}
_source as (select * from {{ relation }})

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name in imputation_config %}
            {%- set config = imputation_config[col.name] %}
            {%- if config.method == 'value' %}
    coalesce({{ col.name }}, {{ config.value }}) as {{ col.name }}
            {%- else %}
    coalesce(t.{{ col.name }}, _{{ col.name }}_stat._impute_val) as {{ col.name }}
            {%- endif %}
        {%- else %}
    t.{{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from _source t
{% for col, config in imputation_config.items() %}
    {% if config.method in ['mean', 'median', 'mode'] %}
cross join _{{ col }}_stat
    {% endif %}
{% endfor %}

{% endmacro %}
