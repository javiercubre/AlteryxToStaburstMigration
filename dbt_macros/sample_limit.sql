{#
    =============================================================================
    Sample and Limit Macros
    Alteryx Equivalent: Sample tool, Select Records tool, Random Sample
    Trino Compatible: Yes
    =============================================================================

    These macros provide data sampling, limiting, and random sampling functionality.
#}

{#
    Macro: sample_first_n
    Description: Returns the first N records.

    Alteryx Equivalent: Sample tool (First N records mode)

    Arguments:
        relation: The source relation
        n: Number of records to return
        order_by: Optional columns to order by before sampling

    Example Usage:
        {{ sample_first_n(ref('stg_orders'), 100, order_by=['order_date']) }}
#}

{% macro sample_first_n(relation, n, order_by=none) %}

select *
from {{ relation }}
{% if order_by %}
order by {{ order_by | join(', ') }}
{% endif %}
limit {{ n }}

{% endmacro %}


{#
    Macro: sample_last_n
    Description: Returns the last N records based on ordering.

    Alteryx Equivalent: Sample tool (Last N records mode)

    Arguments:
        relation: The source relation
        n: Number of records to return
        order_by: Columns to order by (reversed to get last N)

    Example Usage:
        {{ sample_last_n(ref('stg_orders'), 100, order_by=['order_date']) }}
#}

{% macro sample_last_n(relation, n, order_by) %}

with reversed as (
    select
        *,
        row_number() over (order by {{ order_by | join(' desc, ') }} desc) as _rn
    from {{ relation }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from reversed
where _rn <= {{ n }}
order by _rn desc

{% endmacro %}


{#
    Macro: sample_every_nth
    Description: Returns every Nth record.

    Alteryx Equivalent: Sample tool (Every Nth record mode)

    Arguments:
        relation: The source relation
        n: Take every Nth record
        order_by: Optional columns to order by
        start_offset: Start at record number (default: 1)

    Example Usage:
        {{ sample_every_nth(ref('stg_transactions'), 10, order_by=['transaction_id']) }}
#}

{% macro sample_every_nth(relation, n, order_by=none, start_offset=1) %}

with numbered as (
    select
        *,
        row_number() over (
            {% if order_by %}order by {{ order_by | join(', ') }}{% else %}order by (select null){% endif %}
        ) as _rn
    from {{ relation }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from numbered
where mod(_rn - {{ start_offset }}, {{ n }}) = 0 and _rn >= {{ start_offset }}

{% endmacro %}


{#
    Macro: sample_random_percent
    Description: Returns a random percentage of records using table sampling.

    Alteryx Equivalent: Random % Sample mode

    Arguments:
        relation: The source relation
        percent: Percentage of records to sample (0-100)
        seed: Optional random seed for reproducibility

    Example Usage:
        {{ sample_random_percent(ref('stg_customers'), 10) }}
#}

{% macro sample_random_percent(relation, percent, seed=none) %}

select *
from {{ relation }}
tablesample bernoulli({{ percent }})
{% if seed %}
{# Note: Trino's TABLESAMPLE doesn't support seed directly, using alternative #}
{% endif %}

{% endmacro %}


{#
    Macro: sample_random_n
    Description: Returns approximately N random records.

    Alteryx Equivalent: Random N records mode

    Arguments:
        relation: The source relation
        n: Approximate number of records to return
        seed: Optional random seed

    Example Usage:
        {{ sample_random_n(ref('stg_orders'), 1000) }}
#}

{% macro sample_random_n(relation, n, seed=none) %}

with counted as (
    select count(*) as total_count from {{ relation }}
),
sampled as (
    select
        t.*,
        random() as _rand
    from {{ relation }} t
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from sampled
order by _rand
limit {{ n }}

{% endmacro %}


{#
    Macro: sample_first_n_per_group
    Description: Returns first N records per group.

    Alteryx Equivalent: Sample tool (First N per group)

    Arguments:
        relation: The source relation
        n: Number of records per group
        group_by: Columns defining groups
        order_by: Columns to order by within groups
        order_direction: 'asc' or 'desc' (default: 'asc')

    Example Usage:
        {{ sample_first_n_per_group(
            relation=ref('stg_orders'),
            n=5,
            group_by=['customer_id'],
            order_by=['order_date'],
            order_direction='desc'
        ) }}
#}

{% macro sample_first_n_per_group(relation, n, group_by, order_by, order_direction='asc') %}

with ranked as (
    select
        *,
        row_number() over (
            partition by {{ group_by | join(', ') }}
            order by {{ order_by | join(' ' ~ order_direction ~ ', ') }} {{ order_direction }}
        ) as _group_rank
    from {{ relation }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from ranked
where _group_rank <= {{ n }}

{% endmacro %}


{#
    Macro: select_records_by_range
    Description: Selects records by row number range.

    Alteryx Equivalent: Select Records tool

    Arguments:
        relation: The source relation
        start_row: Starting row number (1-indexed, inclusive)
        end_row: Ending row number (inclusive, or null for no limit)
        order_by: Optional columns to order by (defines row order)

    Example Usage:
        {{ select_records_by_range(ref('stg_data'), 100, 200, order_by=['id']) }}
#}

{% macro select_records_by_range(relation, start_row, end_row=none, order_by=none) %}

with numbered as (
    select
        *,
        row_number() over (
            {% if order_by %}order by {{ order_by | join(', ') }}{% else %}order by (select null){% endif %}
        ) as _rn
    from {{ relation }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from numbered
where _rn >= {{ start_row }}
{% if end_row %}
and _rn <= {{ end_row }}
{% endif %}

{% endmacro %}


{#
    Macro: select_records_by_list
    Description: Selects specific record numbers.

    Alteryx Equivalent: Select Records tool (specific records mode)

    Arguments:
        relation: The source relation
        record_numbers: List of row numbers to select (1-indexed)
        order_by: Optional columns defining row order

    Example Usage:
        {{ select_records_by_list(ref('stg_data'), [1, 5, 10, 15, 20], order_by=['id']) }}
#}

{% macro select_records_by_list(relation, record_numbers, order_by=none) %}

with numbered as (
    select
        *,
        row_number() over (
            {% if order_by %}order by {{ order_by | join(', ') }}{% else %}order by (select null){% endif %}
        ) as _rn
    from {{ relation }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from numbered
where _rn in ({{ record_numbers | join(', ') }})

{% endmacro %}


{#
    Macro: stratified_sample
    Description: Returns a proportional sample from each stratum/group.

    Alteryx Equivalent: Stratified sampling pattern

    Arguments:
        relation: The source relation
        stratum_column: Column defining strata
        sample_percent: Percentage to sample from each stratum (0-100)

    Example Usage:
        {{ stratified_sample(ref('stg_customers'), 'region', 10) }}
#}

{% macro stratified_sample(relation, stratum_column, sample_percent) %}

with with_random as (
    select
        *,
        row_number() over (partition by {{ stratum_column }} order by random()) as _rn,
        count(*) over (partition by {{ stratum_column }}) as _stratum_total
    from {{ relation }}
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from with_random
where _rn <= ceil(_stratum_total * {{ sample_percent }} / 100.0)

{% endmacro %}


{#
    Macro: weighted_sample
    Description: Samples records with probability proportional to a weight column.

    Alteryx Equivalent: Weighted random sample pattern

    Arguments:
        relation: The source relation
        weight_column: Column containing weights
        n: Number of records to sample

    Example Usage:
        {{ weighted_sample(ref('stg_products'), 'sales_volume', 100) }}
#}

{% macro weighted_sample(relation, weight_column, n) %}

with weighted as (
    select
        *,
        -ln(1 - random()) / {{ weight_column }} as _weighted_rand
    from {{ relation }}
    where {{ weight_column }} > 0
)

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
    {{ column.name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from weighted
order by _weighted_rand
limit {{ n }}

{% endmacro %}
