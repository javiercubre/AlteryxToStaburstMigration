{#
    =============================================================================
    Select and Transform Macros
    Alteryx Equivalent: Select tool, Auto Field, Sort tool
    Trino Compatible: Yes
    =============================================================================

    These macros provide column selection, renaming, reordering, and sorting
    functionality similar to Alteryx Select and Sort tools.
#}

{#
    Macro: select_columns
    Description: Selects specific columns from a relation.

    Alteryx Equivalent: Select tool (choosing which columns to include)

    Arguments:
        relation: The source relation
        columns: List of column names to include
        include: If true, include listed columns; if false, exclude them

    Example Usage:
        {{ select_columns(ref('stg_orders'), ['order_id', 'customer_id', 'amount']) }}
#}

{% macro select_columns(relation, columns, include=true) %}

select
    {% if include %}
    {{ columns | join(',\n    ') }}
    {% else %}
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- set excluded = columns | map('lower') | list %}
    {%- for column in all_columns %}
        {%- if column.name|lower not in excluded %}
    {{ column.name }}{% if not loop.last %},{% endif %}
        {%- endif %}
    {%- endfor %}
    {% endif %}
from {{ relation }}

{% endmacro %}


{#
    Macro: rename_columns
    Description: Renames columns in a relation.

    Alteryx Equivalent: Select tool (rename functionality)

    Arguments:
        relation: The source relation
        renames: Dictionary of old_name: new_name pairs

    Example Usage:
        {{ rename_columns(
            relation=ref('stg_orders'),
            renames={'cust_id': 'customer_id', 'amt': 'amount', 'ord_dt': 'order_date'}
        ) }}
#}

{% macro rename_columns(relation, renames) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
        {%- if column.name in renames %}
    {{ column.name }} as {{ renames[column.name] }}
        {%- else %}
    {{ column.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: reorder_columns
    Description: Reorders columns, with specified columns first.

    Alteryx Equivalent: Select tool (reordering with arrows)

    Arguments:
        relation: The source relation
        first_columns: List of columns to appear first (in order)
        include_rest: Whether to include remaining columns (default: true)

    Example Usage:
        {{ reorder_columns(
            relation=ref('stg_orders'),
            first_columns=['order_id', 'order_date', 'customer_id']
        ) }}
#}

{% macro reorder_columns(relation, first_columns, include_rest=true) %}

select
    {%- for col in first_columns %}
    {{ col }},
    {%- endfor %}
    {% if include_rest %}
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- set first_lower = first_columns | map('lower') | list %}
    {%- for column in all_columns %}
        {%- if column.name|lower not in first_lower %}
    {{ column.name }}{% if not loop.last %},{% endif %}
        {%- endif %}
    {%- endfor %}
    {% endif %}
from {{ relation }}

{% endmacro %}


{#
    Macro: select_and_rename
    Description: Selects specific columns with optional renaming.

    Alteryx Equivalent: Select tool (select and rename in one step)

    Arguments:
        relation: The source relation
        columns: Dictionary of source_column: alias pairs (use same name for no rename)

    Example Usage:
        {{ select_and_rename(
            relation=ref('stg_raw'),
            columns={
                'customer_id': 'customer_id',
                'cust_name': 'customer_name',
                'ord_amt': 'order_amount'
            }
        ) }}
#}

{% macro select_and_rename(relation, columns) %}

select
    {%- for source_col, alias in columns.items() %}
    {{ source_col }}{% if source_col != alias %} as {{ alias }}{% endif %}{% if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: sort_data
    Description: Sorts data by specified columns.

    Alteryx Equivalent: Sort tool

    Arguments:
        relation: The source relation
        order_by: List of columns to sort by (or list of [column, direction] pairs)
        nulls_position: 'first', 'last', or none (default: none)

    Example Usage:
        {{ sort_data(
            relation=ref('stg_orders'),
            order_by=[['order_date', 'desc'], ['customer_id', 'asc']],
            nulls_position='last'
        ) }}
#}

{% macro sort_data(relation, order_by, nulls_position=none) %}

select *
from {{ relation }}
order by
    {%- for item in order_by %}
        {%- if item is iterable and item is not string %}
    {{ item[0] }} {{ item[1] | default('asc') }}{% if nulls_position %} nulls {{ nulls_position }}{% endif %}
        {%- else %}
    {{ item }}{% if nulls_position %} nulls {{ nulls_position }}{% endif %}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}

{% endmacro %}


{#
    Macro: add_row_number
    Description: Adds sequential row number to each row.

    Alteryx Equivalent: RecordID tool

    Arguments:
        relation: The source relation
        alias: Name for the row number column (default: 'row_num')
        partition_by: Optional partition columns
        order_by: Optional order columns
        start_value: Starting value (default: 1)

    Example Usage:
        {{ add_row_number(
            relation=ref('stg_customers'),
            alias='customer_sequence',
            order_by=['created_at']
        ) }}
#}

{% macro add_row_number(relation, alias='row_num', partition_by=none, order_by=none, start_value=1) %}

select
    row_number() over (
        {% if partition_by %}partition by {{ partition_by | join(', ') }}{% endif %}
        {% if order_by %}order by {{ order_by | join(', ') }}{% endif %}
    ){% if start_value != 1 %} + {{ start_value - 1 }}{% endif %} as {{ alias }},
    *
from {{ relation }}

{% endmacro %}


{#
    Macro: auto_type_columns
    Description: Applies type inference/conversion to columns.

    Alteryx Equivalent: Auto Field tool

    Arguments:
        relation: The source relation
        type_overrides: Dictionary of column: type pairs for manual type specification
        string_to_number: Try converting string columns to numbers (default: false)
        string_to_date: Try converting string columns to dates (default: false)

    Example Usage:
        {{ auto_type_columns(
            relation=ref('raw_data'),
            type_overrides={'order_id': 'bigint', 'amount': 'decimal(18,2)'},
            string_to_number=true
        ) }}
#}

{% macro auto_type_columns(relation, type_overrides={}, string_to_number=false, string_to_date=false) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
        {%- if column.name in type_overrides %}
    try_cast({{ column.name }} as {{ type_overrides[column.name] }}) as {{ column.name }}
        {%- elif string_to_number and column.is_string() %}
    coalesce(try_cast({{ column.name }} as double), try_cast({{ column.name }} as bigint), {{ column.name }}) as {{ column.name }}
        {%- elif string_to_date and column.is_string() %}
    coalesce(try_cast({{ column.name }} as date), try_cast({{ column.name }} as timestamp), {{ column.name }}) as {{ column.name }}
        {%- else %}
    {{ column.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: prefix_columns
    Description: Adds a prefix to all or selected column names.

    Alteryx Equivalent: Select tool with dynamic rename

    Arguments:
        relation: The source relation
        prefix: Prefix to add
        columns: Optional list of columns to prefix (all if not specified)
        exclude: Optional list of columns to exclude from prefixing

    Example Usage:
        {{ prefix_columns(ref('stg_orders'), 'ord_', exclude=['id']) }}
#}

{% macro prefix_columns(relation, prefix, columns=none, exclude=[]) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- set cols_to_prefix = columns if columns else all_columns | map(attribute='name') | list %}
    {%- for column in all_columns %}
        {%- if column.name in cols_to_prefix and column.name not in exclude %}
    {{ column.name }} as {{ prefix }}{{ column.name }}
        {%- else %}
    {{ column.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: suffix_columns
    Description: Adds a suffix to all or selected column names.

    Arguments:
        relation: The source relation
        suffix: Suffix to add
        columns: Optional list of columns to suffix
        exclude: Optional list of columns to exclude

    Example Usage:
        {{ suffix_columns(ref('stg_customers'), '_raw', exclude=['customer_id']) }}
#}

{% macro suffix_columns(relation, suffix, columns=none, exclude=[]) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- set cols_to_suffix = columns if columns else all_columns | map(attribute='name') | list %}
    {%- for column in all_columns %}
        {%- if column.name in cols_to_suffix and column.name not in exclude %}
    {{ column.name }} as {{ column.name }}{{ suffix }}
        {%- else %}
    {{ column.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: drop_columns
    Description: Drops specified columns from a relation.

    Alteryx Equivalent: Select tool (unchecking columns)

    Arguments:
        relation: The source relation
        columns_to_drop: List of column names to remove

    Example Usage:
        {{ drop_columns(ref('stg_orders'), ['internal_id', 'temp_field', 'debug_info']) }}
#}

{% macro drop_columns(relation, columns_to_drop) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- set drop_lower = columns_to_drop | map('lower') | list %}
    {%- for column in all_columns %}
        {%- if column.name|lower not in drop_lower %}
    {{ column.name }}{% if not loop.last %},{% endif %}
        {%- endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: limit_rows
    Description: Limits the number of rows returned.

    Alteryx Equivalent: Sample tool (First N records)

    Arguments:
        relation: The source relation
        n: Number of rows to return
        offset: Number of rows to skip (default: 0)

    Example Usage:
        {{ limit_rows(ref('stg_orders'), 1000, offset=0) }}
#}

{% macro limit_rows(relation, n, offset=0) %}

select *
from {{ relation }}
{% if offset > 0 %}
offset {{ offset }}
{% endif %}
limit {{ n }}

{% endmacro %}
