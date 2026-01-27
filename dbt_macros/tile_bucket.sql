{#
    =============================================================================
    Tile and Bucket Macros
    Alteryx Equivalent: Tile tool
    Trino Compatible: Yes
    =============================================================================

    These macros provide data tiling, binning, and bucketing functionality
    similar to the Alteryx Tile tool.
#}

{#
    Macro: tile_equal_records
    Description: Divides data into N tiles with approximately equal record counts.

    Alteryx Equivalent: Tile tool (Equal Records mode)

    Arguments:
        relation: The source relation
        num_tiles: Number of tiles to create
        order_by: Columns to order by before tiling
        tile_column: Name for the tile column (default: 'tile')
        partition_by: Optional partition columns for tiling within groups

    Example Usage:
        {{ tile_equal_records(
            relation=ref('stg_customers'),
            num_tiles=10,
            order_by=['lifetime_value'],
            tile_column='value_decile'
        ) }}
#}

{% macro tile_equal_records(relation, num_tiles, order_by, tile_column='tile', partition_by=none) %}

select
    *,
    ntile({{ num_tiles }}) over (
        {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
        order by {{ order_by | join(', ') }}
    ) as {{ tile_column }}
from {{ relation }}

{% endmacro %}


{#
    Macro: tile_equal_sum
    Description: Divides data into N tiles with approximately equal sum of a value column.

    Alteryx Equivalent: Tile tool (Equal Sum mode)

    Arguments:
        relation: The source relation
        num_tiles: Number of tiles to create
        sum_column: Column to sum for equal distribution
        order_by: Columns to order by
        tile_column: Name for the tile column

    Example Usage:
        {{ tile_equal_sum(
            relation=ref('stg_orders'),
            num_tiles=5,
            sum_column='revenue',
            order_by=['revenue'],
            tile_column='revenue_quintile'
        ) }}
#}

{% macro tile_equal_sum(relation, num_tiles, sum_column, order_by, tile_column='tile') %}

with running_totals as (
    select
        *,
        sum({{ sum_column }}) over (order by {{ order_by | join(', ') }}) as _running_sum,
        sum({{ sum_column }}) over () as _total_sum
    from {{ relation }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
    {{ col.name }},
    {%- endfor %}
    ceil(_running_sum / nullif(_total_sum / {{ num_tiles }}.0, 0)) as {{ tile_column }}
from running_totals

{% endmacro %}


{#
    Macro: tile_smart
    Description: Smart tiling that handles edge cases and provides tile metadata.

    Alteryx Equivalent: Tile tool (Smart Tile mode)

    Arguments:
        relation: The source relation
        num_tiles: Number of tiles
        order_by: Columns to order by
        tile_column: Name for tile column
        include_sequence: Include sequence number within tile (default: false)

    Example Usage:
        {{ tile_smart(
            relation=ref('stg_scores'),
            num_tiles=4,
            order_by=['score'],
            tile_column='quartile',
            include_sequence=true
        ) }}
#}

{% macro tile_smart(relation, num_tiles, order_by, tile_column='tile', include_sequence=false) %}

select
    *,
    ntile({{ num_tiles }}) over (order by {{ order_by | join(', ') }}) as {{ tile_column }}
    {%- if include_sequence %},
    row_number() over (
        partition by ntile({{ num_tiles }}) over (order by {{ order_by | join(', ') }})
        order by {{ order_by | join(', ') }}
    ) as {{ tile_column }}_sequence
    {%- endif %}
from {{ relation }}

{% endmacro %}


{#
    Macro: bucket_fixed_width
    Description: Creates buckets of fixed width.

    Alteryx Equivalent: Tile tool (Fixed Width mode)

    Arguments:
        column: Column to bucket
        bucket_width: Width of each bucket
        min_value: Minimum value (optional, uses column min)
        bucket_column: Name for bucket column (default: uses formula)

    Example Usage:
        {{ bucket_fixed_width('age', 10) }}
        -- Returns bucket label like '20-30', '30-40', etc.
#}

{% macro bucket_fixed_width(column, bucket_width, min_value=0) %}
concat(
    cast(floor(({{ column }} - {{ min_value }}) / {{ bucket_width }}) * {{ bucket_width }} + {{ min_value }} as varchar),
    '-',
    cast(floor(({{ column }} - {{ min_value }}) / {{ bucket_width }}) * {{ bucket_width }} + {{ min_value }} + {{ bucket_width }} as varchar)
)
{% endmacro %}


{#
    Macro: bucket_fixed_width_numeric
    Description: Returns numeric bucket ID for fixed-width buckets.

    Arguments:
        column: Column to bucket
        bucket_width: Width of each bucket
        min_value: Minimum value (default: 0)

    Example Usage:
        {{ bucket_fixed_width_numeric('income', 10000) }} as income_bucket
#}

{% macro bucket_fixed_width_numeric(column, bucket_width, min_value=0) %}
floor(({{ column }} - {{ min_value }}) / {{ bucket_width }}) + 1
{% endmacro %}


{#
    Macro: bucket_custom_ranges
    Description: Creates buckets based on custom-defined ranges.

    Alteryx Equivalent: Tile tool (Manual mode)

    Arguments:
        column: Column to bucket
        ranges: List of [lower_bound, upper_bound, label] tuples
        default_label: Label for values outside all ranges

    Example Usage:
        {{ bucket_custom_ranges(
            'score',
            ranges=[
                [0, 59, 'F'],
                [60, 69, 'D'],
                [70, 79, 'C'],
                [80, 89, 'B'],
                [90, 100, 'A']
            ],
            default_label='Invalid'
        ) }}
#}

{% macro bucket_custom_ranges(column, ranges, default_label='Other') %}
case
    {%- for range in ranges %}
    when {{ column }} >= {{ range[0] }} and {{ column }} <= {{ range[1] }} then '{{ range[2] }}'
    {%- endfor %}
    else '{{ default_label }}'
end
{% endmacro %}


{#
    Macro: quantile_buckets
    Description: Creates buckets based on quantiles (percentiles).

    Alteryx Equivalent: Tile tool for percentile-based grouping

    Arguments:
        relation: Source relation
        column: Column to bucket
        quantiles: List of percentile cutpoints (e.g., [0.25, 0.5, 0.75])
        bucket_column: Name for bucket column
        labels: Optional list of labels (should be len(quantiles) + 1)

    Example Usage:
        {{ quantile_buckets(
            relation=ref('stg_incomes'),
            column='income',
            quantiles=[0.25, 0.5, 0.75],
            bucket_column='income_quartile',
            labels=['Low', 'Below Median', 'Above Median', 'High']
        ) }}
#}

{% macro quantile_buckets(relation, column, quantiles, bucket_column='quantile_bucket', labels=none) %}

with percentile_values as (
    select
        {%- for q in quantiles %}
        approx_percentile({{ column }}, {{ q }}) as _p{{ loop.index }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ relation }}
    where {{ column }} is not null
)

select
    t.*,
    case
        {%- for q in quantiles %}
        when t.{{ column }} <= p._p{{ loop.index }}
            then {% if labels %}'{{ labels[loop.index0] }}'{% else %}{{ loop.index }}{% endif %}
        {%- endfor %}
        else {% if labels %}'{{ labels[-1] }}'{% else %}{{ quantiles | length + 1 }}{% endif %}
    end as {{ bucket_column }}
from {{ relation }} t
cross join percentile_values p

{% endmacro %}


{#
    Macro: bin_numeric
    Description: Bins numeric values into specified number of equal-width bins.

    Alteryx Equivalent: Tile tool (Equal Interval mode)

    Arguments:
        relation: Source relation
        column: Column to bin
        num_bins: Number of bins to create
        bin_column: Name for bin column

    Example Usage:
        {{ bin_numeric(ref('stg_measurements'), 'temperature', 10, 'temp_bin') }}
#}

{% macro bin_numeric(relation, column, num_bins, bin_column='bin') %}

with stats as (
    select
        min({{ column }}) as _min_val,
        max({{ column }}) as _max_val,
        (max({{ column }}) - min({{ column }})) / {{ num_bins }}.0 as _bin_width
    from {{ relation }}
    where {{ column }} is not null
)

select
    t.*,
    case
        when t.{{ column }} = s._max_val then {{ num_bins }}
        else least(floor((t.{{ column }} - s._min_val) / nullif(s._bin_width, 0)) + 1, {{ num_bins }})
    end as {{ bin_column }}
from {{ relation }} t
cross join stats s

{% endmacro %}


{#
    Macro: percentile_rank
    Description: Calculates the percentile rank of each row.

    Alteryx Equivalent: Tile tool output with percentage

    Arguments:
        relation: Source relation
        order_by: Columns to order by
        rank_column: Name for the rank column
        partition_by: Optional partition columns

    Example Usage:
        {{ percentile_rank(ref('stg_sales'), ['revenue'], 'revenue_percentile') }}
#}

{% macro percentile_rank(relation, order_by, rank_column='percentile_rank', partition_by=none) %}

select
    *,
    percent_rank() over (
        {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
        order by {{ order_by | join(', ') }}
    ) * 100 as {{ rank_column }}
from {{ relation }}

{% endmacro %}


{#
    Macro: cume_dist
    Description: Calculates cumulative distribution of values.

    Alteryx Equivalent: Related to tile tool percentage calculations

    Arguments:
        relation: Source relation
        order_by: Columns to order by
        dist_column: Name for the distribution column
        partition_by: Optional partition columns

    Example Usage:
        {{ cume_dist(ref('stg_scores'), ['score'], 'cumulative_pct') }}
#}

{% macro cume_dist(relation, order_by, dist_column='cume_dist', partition_by=none) %}

select
    *,
    cume_dist() over (
        {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
        order by {{ order_by | join(', ') }}
    ) as {{ dist_column }}
from {{ relation }}

{% endmacro %}


{#
    Macro: assign_tile_by_value
    Description: Assigns tiles based on specific value boundaries.

    Alteryx Equivalent: Tile tool with manual cutpoints

    Arguments:
        column: Column to tile
        cutpoints: List of boundary values [c1, c2, c3, ...]
                   Creates tiles: <=c1, c1<x<=c2, c2<x<=c3, >c3

    Example Usage:
        {{ assign_tile_by_value('revenue', [1000, 5000, 10000, 50000]) }}
#}

{% macro assign_tile_by_value(column, cutpoints) %}
case
    when {{ column }} <= {{ cutpoints[0] }} then 1
    {%- for i in range(1, cutpoints | length) %}
    when {{ column }} <= {{ cutpoints[i] }} then {{ i + 1 }}
    {%- endfor %}
    else {{ cutpoints | length + 1 }}
end
{% endmacro %}
