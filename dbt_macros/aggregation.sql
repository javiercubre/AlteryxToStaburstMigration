{#
    =============================================================================
    Aggregation Macros
    Alteryx Equivalent: Summarize tool
    Trino Compatible: Yes
    =============================================================================

    These macros provide comprehensive aggregation functionality matching
    all Alteryx Summarize tool capabilities.
#}

{#
    Macro: summarize
    Description: General-purpose aggregation matching Alteryx Summarize tool.

    Alteryx Equivalent: Summarize tool

    Arguments:
        relation: Source relation
        group_by: List of columns to group by
        aggregations: List of [column, agg_function, alias] tuples
                     Supported functions: sum, count, count_distinct, min, max, avg,
                     first, last, concat, stddev, variance, median

    Example Usage:
        {{ summarize(
            relation=ref('stg_orders'),
            group_by=['customer_id', 'region'],
            aggregations=[
                ['order_amount', 'sum', 'total_revenue'],
                ['order_id', 'count', 'order_count'],
                ['order_date', 'min', 'first_order'],
                ['order_date', 'max', 'last_order'],
                ['order_amount', 'avg', 'avg_order_value']
            ]
        ) }}
#}

{% macro summarize(relation, group_by, aggregations) %}

select
    {{ group_by | join(',\n    ') }},
    {%- for agg in aggregations %}
        {%- set col = agg[0] %}
        {%- set func = agg[1] | lower %}
        {%- set alias = agg[2] %}
        {%- if func == 'sum' %}
    sum({{ col }}) as {{ alias }}
        {%- elif func == 'count' %}
    count({{ col }}) as {{ alias }}
        {%- elif func == 'count_distinct' %}
    count(distinct {{ col }}) as {{ alias }}
        {%- elif func == 'count_null' %}
    sum(case when {{ col }} is null then 1 else 0 end) as {{ alias }}
        {%- elif func == 'count_non_null' %}
    count({{ col }}) as {{ alias }}
        {%- elif func == 'min' %}
    min({{ col }}) as {{ alias }}
        {%- elif func == 'max' %}
    max({{ col }}) as {{ alias }}
        {%- elif func == 'avg' or func == 'average' %}
    avg({{ col }}) as {{ alias }}
        {%- elif func == 'first' %}
    min_by({{ col }}, row_number() over ()) as {{ alias }}
        {%- elif func == 'last' %}
    max_by({{ col }}, row_number() over ()) as {{ alias }}
        {%- elif func == 'concat' %}
    array_join(array_agg({{ col }}), ',') as {{ alias }}
        {%- elif func == 'concat_distinct' %}
    array_join(array_agg(distinct {{ col }}), ',') as {{ alias }}
        {%- elif func == 'stddev' %}
    stddev({{ col }}) as {{ alias }}
        {%- elif func == 'stddev_pop' %}
    stddev_pop({{ col }}) as {{ alias }}
        {%- elif func == 'variance' %}
    variance({{ col }}) as {{ alias }}
        {%- elif func == 'variance_pop' %}
    var_pop({{ col }}) as {{ alias }}
        {%- elif func == 'median' %}
    approx_percentile({{ col }}, 0.5) as {{ alias }}
        {%- else %}
    {{ func }}({{ col }}) as {{ alias }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}
group by {{ group_by | join(', ') }}

{% endmacro %}


{#
    Macro: count_records
    Description: Simple record count.

    Alteryx Equivalent: Count Records tool, Summarize (Count)

    Arguments:
        relation: Source relation
        group_by: Optional grouping columns
        alias: Name for count column

    Example Usage:
        {{ count_records(ref('stg_orders'), group_by=['status']) }}
#}

{% macro count_records(relation, group_by=none, alias='record_count') %}

select
    {%- if group_by %}
    {{ group_by | join(', ') }},
    {%- endif %}
    count(*) as {{ alias }}
from {{ relation }}
{%- if group_by %}
group by {{ group_by | join(', ') }}
{%- endif %}

{% endmacro %}


{#
    Macro: sum_column
    Description: Sums a column with optional grouping.

    Alteryx Equivalent: Summarize (Sum)

    Arguments:
        relation: Source relation
        column: Column to sum
        group_by: Optional grouping columns
        alias: Name for sum column

    Example Usage:
        {{ sum_column(ref('stg_sales'), 'revenue', group_by=['region']) }}
#}

{% macro sum_column(relation, column, group_by=none, alias=none) %}

{%- set result_alias = alias if alias else column ~ '_sum' -%}

select
    {%- if group_by %}
    {{ group_by | join(', ') }},
    {%- endif %}
    sum({{ column }}) as {{ result_alias }}
from {{ relation }}
{%- if group_by %}
group by {{ group_by | join(', ') }}
{%- endif %}

{% endmacro %}


{#
    Macro: aggregate_all
    Description: Applies multiple aggregations to all numeric columns.

    Alteryx Equivalent: Summarize with multiple functions on all fields

    Arguments:
        relation: Source relation
        group_by: Grouping columns
        agg_functions: List of aggregation functions to apply

    Example Usage:
        {{ aggregate_all(ref('stg_metrics'), ['date'], ['sum', 'avg', 'min', 'max']) }}
#}

{% macro aggregate_all(relation, group_by, agg_functions=['sum', 'avg', 'min', 'max']) %}

select
    {{ group_by | join(', ') }}
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- set group_lower = group_by | map('lower') | list %}
    {%- for col in all_columns %}
        {%- if col.name|lower not in group_lower and col.is_numeric() %}
            {%- for func in agg_functions %},
    {{ func }}({{ col.name }}) as {{ col.name }}_{{ func }}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}
from {{ relation }}
group by {{ group_by | join(', ') }}

{% endmacro %}


{#
    Macro: group_concat
    Description: Concatenates values within a group.

    Alteryx Equivalent: Summarize (Concat)

    Arguments:
        relation: Source relation
        group_by: Grouping columns
        concat_column: Column to concatenate
        delimiter: Separator between values
        alias: Name for concatenated column
        distinct_values: Only include distinct values
        order_by: Optional ordering within group

    Example Usage:
        {{ group_concat(
            relation=ref('stg_tags'),
            group_by=['product_id'],
            concat_column='tag_name',
            delimiter=', ',
            alias='all_tags',
            distinct_values=true
        ) }}
#}

{% macro group_concat(relation, group_by, concat_column, delimiter=',', alias='concatenated', distinct_values=false, order_by=none) %}

select
    {{ group_by | join(', ') }},
    array_join(
        array_agg({% if distinct_values %}distinct {% endif %}{{ concat_column }}{% if order_by %} order by {{ order_by | join(', ') }}{% endif %}),
        '{{ delimiter }}'
    ) as {{ alias }}
from {{ relation }}
group by {{ group_by | join(', ') }}

{% endmacro %}


{#
    Macro: first_last_value
    Description: Gets first and/or last value within a group.

    Alteryx Equivalent: Summarize (First/Last)

    Arguments:
        relation: Source relation
        group_by: Grouping columns
        value_column: Column to get first/last from
        order_by: Column(s) determining order
        get_first: Include first value (default: true)
        get_last: Include last value (default: true)

    Example Usage:
        {{ first_last_value(
            relation=ref('stg_transactions'),
            group_by=['account_id'],
            value_column='balance',
            order_by=['transaction_date']
        ) }}
#}

{% macro first_last_value(relation, group_by, value_column, order_by, get_first=true, get_last=true) %}

select
    {{ group_by | join(', ') }}
    {%- if get_first %},
    min_by({{ value_column }}, {{ order_by | join(', ') }}) as first_{{ value_column }}
    {%- endif %}
    {%- if get_last %},
    max_by({{ value_column }}, {{ order_by | join(', ') }}) as last_{{ value_column }}
    {%- endif %}
from {{ relation }}
group by {{ group_by | join(', ') }}

{% endmacro %}


{#
    Macro: percentile_agg
    Description: Calculates percentile values within groups.

    Alteryx Equivalent: Summarize (Percentile)

    Arguments:
        relation: Source relation
        group_by: Grouping columns
        value_column: Column to calculate percentile on
        percentiles: List of percentile values (0-1)

    Example Usage:
        {{ percentile_agg(
            relation=ref('stg_orders'),
            group_by=['region'],
            value_column='order_amount',
            percentiles=[0.25, 0.5, 0.75, 0.9, 0.99]
        ) }}
#}

{% macro percentile_agg(relation, group_by, value_column, percentiles=[0.5]) %}

select
    {{ group_by | join(', ') }}
    {%- for p in percentiles %},
    approx_percentile({{ value_column }}, {{ p }}) as {{ value_column }}_p{{ (p * 100) | int }}
    {%- endfor %}
from {{ relation }}
group by {{ group_by | join(', ') }}

{% endmacro %}


{#
    Macro: count_by_value
    Description: Counts occurrences of each value in a column.

    Alteryx Equivalent: Summarize (Group By + Count)

    Arguments:
        relation: Source relation
        column: Column to count values of
        alias: Name for count column
        include_null: Whether to include null as a value

    Example Usage:
        {{ count_by_value(ref('stg_orders'), 'status') }}
#}

{% macro count_by_value(relation, column, alias='count', include_null=true) %}

select
    {{ column }},
    count(*) as {{ alias }}
from {{ relation }}
{% if not include_null %}
where {{ column }} is not null
{% endif %}
group by {{ column }}
order by count(*) desc

{% endmacro %}


{#
    Macro: statistics_summary
    Description: Calculates comprehensive statistics for a numeric column.

    Alteryx Equivalent: Basic Data Profile, Summarize with multiple stats

    Arguments:
        relation: Source relation
        column: Numeric column to analyze
        group_by: Optional grouping columns

    Example Usage:
        {{ statistics_summary(ref('stg_sales'), 'revenue', group_by=['year']) }}
#}

{% macro statistics_summary(relation, column, group_by=none) %}

select
    {%- if group_by %}
    {{ group_by | join(', ') }},
    {%- endif %}
    count(*) as record_count,
    count({{ column }}) as non_null_count,
    count(*) - count({{ column }}) as null_count,
    min({{ column }}) as min_value,
    max({{ column }}) as max_value,
    avg({{ column }}) as mean_value,
    approx_percentile({{ column }}, 0.5) as median_value,
    stddev({{ column }}) as std_dev,
    variance({{ column }}) as variance,
    sum({{ column }}) as total_sum,
    approx_percentile({{ column }}, 0.25) as percentile_25,
    approx_percentile({{ column }}, 0.75) as percentile_75
from {{ relation }}
{%- if group_by %}
group by {{ group_by | join(', ') }}
{%- endif %}

{% endmacro %}


{#
    Macro: weighted_average
    Description: Calculates weighted average.

    Alteryx Equivalent: Weighted Average tool / Summarize pattern

    Arguments:
        relation: Source relation
        value_column: Column containing values
        weight_column: Column containing weights
        group_by: Optional grouping columns
        alias: Name for result column

    Example Usage:
        {{ weighted_average(
            relation=ref('stg_grades'),
            value_column='score',
            weight_column='credits',
            group_by=['student_id'],
            alias='gpa'
        ) }}
#}

{% macro weighted_average(relation, value_column, weight_column, group_by=none, alias='weighted_avg') %}

select
    {%- if group_by %}
    {{ group_by | join(', ') }},
    {%- endif %}
    sum({{ value_column }} * {{ weight_column }}) / nullif(sum({{ weight_column }}), 0) as {{ alias }}
from {{ relation }}
{%- if group_by %}
group by {{ group_by | join(', ') }}
{%- endif %}

{% endmacro %}
