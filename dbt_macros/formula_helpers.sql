{#
    =============================================================================
    Formula Helper Macros
    Alteryx Equivalent: Formula tool, Multi-Field Formula
    Trino Compatible: Yes
    =============================================================================

    These macros provide common formula patterns used in Alteryx Formula tool,
    converted to Trino SQL syntax.
#}

{#
    Macro: iif
    Description: Inline if-then-else (CASE WHEN equivalent).

    Alteryx Equivalent: IIF([condition], true_result, false_result)

    Arguments:
        condition: Boolean condition
        true_value: Value when condition is true
        false_value: Value when condition is false

    Example Usage:
        select {{ iif('amount > 1000', "'high'", "'low'") }} as priority
#}

{% macro iif(condition, true_value, false_value) %}
case when {{ condition }} then {{ true_value }} else {{ false_value }} end
{% endmacro %}


{#
    Macro: switch
    Description: Multi-value switch statement (like Alteryx Switch function).

    Alteryx Equivalent: Switch([field], default, val1, result1, val2, result2, ...)

    Arguments:
        value: Expression to evaluate
        cases: Dictionary of value: result pairs
        default: Default value if no match

    Example Usage:
        {{ switch(
            'status_code',
            cases={'A': "'Active'", 'I': "'Inactive'", 'P': "'Pending'"},
            default="'Unknown'"
        ) }}
#}

{% macro switch(value, cases, default="null") %}
case {{ value }}
    {%- for case_val, result in cases.items() %}
    when '{{ case_val }}' then {{ result }}
    {%- endfor %}
    else {{ default }}
end
{% endmacro %}


{#
    Macro: switch_true
    Description: Multi-condition switch (CASE WHEN without value).

    Alteryx Equivalent: Nested IIF or IF-THEN-ELSEIF

    Arguments:
        conditions: List of [condition, result] pairs (evaluated in order)
        default: Default value if no condition matches

    Example Usage:
        {{ switch_true(
            conditions=[
                ["score >= 90", "'A'"],
                ["score >= 80", "'B'"],
                ["score >= 70", "'C'"],
                ["score >= 60", "'D'"]
            ],
            default="'F'"
        ) }}
#}

{% macro switch_true(conditions, default="null") %}
case
    {%- for condition, result in conditions %}
    when {{ condition }} then {{ result }}
    {%- endfor %}
    else {{ default }}
end
{% endmacro %}


{#
    Macro: null_coalesce
    Description: Returns first non-null value from a list of expressions.

    Alteryx Equivalent: Coalesce() or nested IsNull() checks

    Arguments:
        values: List of expressions to check

    Example Usage:
        {{ null_coalesce(['phone_mobile', 'phone_home', 'phone_work', "'N/A'"]) }}
#}

{% macro null_coalesce(values) %}
coalesce({{ values | join(', ') }})
{% endmacro %}


{#
    Macro: is_null
    Description: Returns boolean indicating if value is null.

    Alteryx Equivalent: IsNull([field])

    Arguments:
        column: Column to check

    Example Usage:
        where {{ is_null('email') }} = true
#}

{% macro is_null(column) %}
({{ column }} is null)
{% endmacro %}


{#
    Macro: is_empty
    Description: Returns boolean indicating if value is empty string.

    Alteryx Equivalent: IsEmpty([field])

    Arguments:
        column: Column to check
        trim_first: Trim whitespace before checking (default: true)

    Example Usage:
        where {{ is_empty('notes') }} = true
#}

{% macro is_empty(column, trim_first=true) %}
{% if trim_first %}
(trim({{ column }}) = '')
{% else %}
({{ column }} = '')
{% endif %}
{% endmacro %}


{#
    Macro: is_null_or_empty
    Description: Returns boolean indicating if value is null OR empty.

    Alteryx Equivalent: IsNull([field]) OR IsEmpty([field])

    Arguments:
        column: Column to check

    Example Usage:
        where {{ is_null_or_empty('customer_name') }} = false
#}

{% macro is_null_or_empty(column) %}
({{ column }} is null or trim({{ column }}) = '')
{% endmacro %}


{#
    Macro: default_if_null
    Description: Returns default value if expression is null.

    Alteryx Equivalent: IIF(IsNull([field]), default, [field])

    Arguments:
        column: Column to check
        default: Default value to use

    Example Usage:
        {{ default_if_null('discount', '0') }} as discount
#}

{% macro default_if_null(column, default) %}
coalesce({{ column }}, {{ default }})
{% endmacro %}


{#
    Macro: default_if_empty
    Description: Returns default value if expression is null or empty.

    Alteryx Equivalent: IIF(IsNull([field]) OR IsEmpty([field]), default, [field])

    Arguments:
        column: Column to check
        default: Default value to use

    Example Usage:
        {{ default_if_empty('notes', "'No notes'"') }} as notes
#}

{% macro default_if_empty(column, default) %}
case when {{ column }} is null or trim({{ column }}) = '' then {{ default }} else {{ column }} end
{% endmacro %}


{#
    Macro: nullif_value
    Description: Returns null if column equals specified value.

    Alteryx Equivalent: IIF([field] = value, Null(), [field])

    Arguments:
        column: Column to check
        value: Value that should become null

    Example Usage:
        {{ nullif_value('status', "'N/A'") }} as status
#}

{% macro nullif_value(column, value) %}
nullif({{ column }}, {{ value }})
{% endmacro %}


{#
    Macro: concat_fields
    Description: Concatenates multiple fields with optional separator.

    Alteryx Equivalent: [field1] + [field2] or Concat()

    Arguments:
        columns: List of column names/expressions
        separator: Separator between values (default: '')
        skip_nulls: If true, skip null values (default: true)

    Example Usage:
        {{ concat_fields(['first_name', '" "', 'last_name'], separator='') }} as full_name
#}

{% macro concat_fields(columns, separator='', skip_nulls=true) %}
{% if skip_nulls %}
array_join(
    filter(
        array[{%- for col in columns %}cast({{ col }} as varchar){% if not loop.last %}, {% endif %}{%- endfor %}],
        x -> x is not null
    ),
    '{{ separator }}'
)
{% else %}
concat({%- for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{%- endfor %})
{% endif %}
{% endmacro %}


{#
    Macro: substring_extract
    Description: Extracts substring from a string.

    Alteryx Equivalent: Substring([field], start, length)

    Arguments:
        column: Source column
        start: Starting position (1-indexed)
        length: Number of characters to extract (optional)

    Example Usage:
        {{ substring_extract('phone', 1, 3) }} as area_code
#}

{% macro substring_extract(column, start, length=none) %}
{% if length %}
substr({{ column }}, {{ start }}, {{ length }})
{% else %}
substr({{ column }}, {{ start }})
{% endif %}
{% endmacro %}


{#
    Macro: left_chars
    Description: Extracts left N characters.

    Alteryx Equivalent: Left([field], n)

    Arguments:
        column: Source column
        n: Number of characters

    Example Usage:
        {{ left_chars('postal_code', 5) }} as zip5
#}

{% macro left_chars(column, n) %}
substr({{ column }}, 1, {{ n }})
{% endmacro %}


{#
    Macro: right_chars
    Description: Extracts right N characters.

    Alteryx Equivalent: Right([field], n)

    Arguments:
        column: Source column
        n: Number of characters

    Example Usage:
        {{ right_chars('phone', 4) }} as last_four
#}

{% macro right_chars(column, n) %}
substr({{ column }}, length({{ column }}) - {{ n }} + 1)
{% endmacro %}


{#
    Macro: add_calculated_column
    Description: Adds a calculated column to a relation.

    Alteryx Equivalent: Formula tool adding new field

    Arguments:
        relation: Source relation
        column_name: Name for new column
        expression: SQL expression for the calculation

    Example Usage:
        {{ add_calculated_column(
            relation=ref('stg_orders'),
            column_name='total_with_tax',
            expression='amount * (1 + tax_rate)'
        ) }}
#}

{% macro add_calculated_column(relation, column_name, expression) %}

select
    *,
    {{ expression }} as {{ column_name }}
from {{ relation }}

{% endmacro %}


{#
    Macro: add_multiple_columns
    Description: Adds multiple calculated columns at once.

    Alteryx Equivalent: Formula tool with multiple expressions, Multi-Field Formula

    Arguments:
        relation: Source relation
        columns: Dictionary of column_name: expression pairs

    Example Usage:
        {{ add_multiple_columns(
            relation=ref('stg_orders'),
            columns={
                'total': 'quantity * unit_price',
                'with_tax': 'quantity * unit_price * 1.08',
                'is_large': "case when quantity > 100 then true else false end"
            }
        ) }}
#}

{% macro add_multiple_columns(relation, columns) %}

select
    *,
    {%- for col_name, expression in columns.items() %}
    {{ expression }} as {{ col_name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: update_column
    Description: Updates an existing column with a new expression.

    Alteryx Equivalent: Formula tool overwriting existing field

    Arguments:
        relation: Source relation
        column_name: Column to update
        expression: New expression for the column
        include_original: Keep original as _original suffix (default: false)

    Example Usage:
        {{ update_column(
            relation=ref('stg_customers'),
            column_name='phone',
            expression="regexp_replace(phone, '[^0-9]', '')"
        ) }}
#}

{% macro update_column(relation, column_name, expression, include_original=false) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for column in all_columns %}
        {%- if column.name == column_name %}
            {%- if include_original %}
    {{ column.name }} as {{ column.name }}_original,
            {%- endif %}
    {{ expression }} as {{ column.name }}
        {%- else %}
    {{ column.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: string_length
    Description: Returns length of string.

    Alteryx Equivalent: Length([field])

    Arguments:
        column: Column to measure

    Example Usage:
        {{ string_length('description') }} as desc_length
#}

{% macro string_length(column) %}
length({{ column }})
{% endmacro %}


{#
    Macro: trim_all
    Description: Trims whitespace from string.

    Alteryx Equivalent: Trim([field])

    Arguments:
        column: Column to trim
        side: 'both', 'left', 'right' (default: 'both')

    Example Usage:
        {{ trim_all('name', 'both') }} as name_trimmed
#}

{% macro trim_all(column, side='both') %}
{% if side == 'left' %}
ltrim({{ column }})
{% elif side == 'right' %}
rtrim({{ column }})
{% else %}
trim({{ column }})
{% endif %}
{% endmacro %}


{#
    Macro: change_case
    Description: Changes string case.

    Alteryx Equivalent: UpperCase(), LowerCase(), TitleCase()

    Arguments:
        column: Column to transform
        case_type: 'upper', 'lower', 'proper' (default: 'lower')

    Example Usage:
        {{ change_case('name', 'proper') }} as name_proper
#}

{% macro change_case(column, case_type='lower') %}
{% if case_type == 'upper' %}
upper({{ column }})
{% elif case_type == 'lower' %}
lower({{ column }})
{% elif case_type == 'proper' %}
{# Trino doesn't have native INITCAP, use pattern replacement #}
regexp_replace(lower({{ column }}), '(\s|^)(\w)', x -> upper(x))
{% else %}
{{ column }}
{% endif %}
{% endmacro %}
