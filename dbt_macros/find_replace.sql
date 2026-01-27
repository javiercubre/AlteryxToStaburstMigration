{#
    =============================================================================
    Find and Replace Macros
    Alteryx Equivalent: Find Replace tool
    Trino Compatible: Yes
    =============================================================================

    These macros provide find/replace functionality including pattern matching,
    lookup-based replacement, and value translation.
#}

{#
    Macro: find_replace_simple
    Description: Simple find and replace on a single column.

    Alteryx Equivalent: Find Replace tool (simple mode)

    Arguments:
        column: Column to perform replacement on
        find_value: Value to find
        replace_value: Value to replace with

    Example Usage:
        {{ find_replace_simple('status', 'Active', 'A') }}
#}

{% macro find_replace_simple(column, find_value, replace_value) %}
replace({{ column }}, '{{ find_value }}', '{{ replace_value }}')
{% endmacro %}


{#
    Macro: find_replace_multiple
    Description: Multiple find/replace operations on a column.

    Alteryx Equivalent: Find Replace tool with multiple replacements

    Arguments:
        column: Column to perform replacements on
        replacements: Dictionary of find: replace pairs

    Example Usage:
        {{ find_replace_multiple(
            'status',
            {'Active': 'A', 'Inactive': 'I', 'Pending': 'P'}
        ) }}
#}

{% macro find_replace_multiple(column, replacements) %}
{%- set result = column -%}
{%- for find_val, replace_val in replacements.items() -%}
    {%- set result = "replace(" ~ result ~ ", '" ~ find_val ~ "', '" ~ replace_val ~ "')" -%}
{%- endfor -%}
{{ result }}
{% endmacro %}


{#
    Macro: find_replace_regex
    Description: Find and replace using regular expression.

    Alteryx Equivalent: Find Replace tool with RegEx mode

    Arguments:
        column: Column to perform replacement on
        pattern: Regular expression pattern to find
        replacement: Replacement string (can use capture groups $1, $2, etc.)

    Example Usage:
        {{ find_replace_regex('phone', '[^0-9]', '') }}
#}

{% macro find_replace_regex(column, pattern, replacement) %}
regexp_replace({{ column }}, '{{ pattern }}', '{{ replacement }}')
{% endmacro %}


{#
    Macro: find_replace_first
    Description: Replace only the first occurrence of a pattern.

    Alteryx Equivalent: Find Replace with "Replace First" option

    Arguments:
        column: Column to perform replacement on
        find_value: Value to find
        replace_value: Value to replace with

    Example Usage:
        {{ find_replace_first('text', 'foo', 'bar') }}
#}

{% macro find_replace_first(column, find_value, replace_value) %}
{# Trino's regexp_replace can limit replacements #}
regexp_replace({{ column }}, '{{ find_value }}', '{{ replace_value }}', 1)
{% endmacro %}


{#
    Macro: find_replace_lookup
    Description: Replace values based on a lookup table/relation.

    Alteryx Equivalent: Find Replace tool with lookup from another input

    Arguments:
        relation: The source relation
        column: Column to perform replacement on
        lookup_relation: Lookup table/relation
        lookup_find: Column in lookup containing values to find
        lookup_replace: Column in lookup containing replacement values
        keep_original: If no match found, keep original value (default: true)

    Example Usage:
        {{ find_replace_lookup(
            relation=ref('stg_orders'),
            column='status_code',
            lookup_relation=ref('status_mapping'),
            lookup_find='code',
            lookup_replace='description',
            keep_original=true
        ) }}
#}

{% macro find_replace_lookup(relation, column, lookup_relation, lookup_find, lookup_replace, keep_original=true) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
    {% if keep_original %}coalesce(lkp.{{ lookup_replace }}, t.{{ column }}){% else %}lkp.{{ lookup_replace }}{% endif %} as {{ column }}
        {%- else %}
    t.{{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }} t
left join {{ lookup_relation }} lkp
    on t.{{ column }} = lkp.{{ lookup_find }}

{% endmacro %}


{#
    Macro: translate_chars
    Description: Character-by-character translation.

    Alteryx Equivalent: REGEX_Replace for character replacement

    Arguments:
        column: Column to translate
        from_chars: Characters to replace
        to_chars: Replacement characters (same length as from_chars)

    Example Usage:
        {{ translate_chars('sku', 'ABC', '123') }}
#}

{% macro translate_chars(column, from_chars, to_chars) %}
translate({{ column }}, '{{ from_chars }}', '{{ to_chars }}')
{% endmacro %}


{#
    Macro: remove_chars
    Description: Remove specific characters from a string.

    Alteryx Equivalent: REGEX_Replace to remove characters

    Arguments:
        column: Column to clean
        chars_to_remove: String of characters to remove

    Example Usage:
        {{ remove_chars('phone', '()-. ') }}
#}

{% macro remove_chars(column, chars_to_remove) %}
translate({{ column }}, '{{ chars_to_remove }}', '')
{% endmacro %}


{#
    Macro: keep_only_chars
    Description: Keep only specified character types.

    Alteryx Equivalent: REGEX_Replace with character class

    Arguments:
        column: Column to filter
        keep_type: 'alphanumeric', 'numeric', 'alpha', 'printable'

    Example Usage:
        {{ keep_only_chars('product_code', 'alphanumeric') }}
#}

{% macro keep_only_chars(column, keep_type='alphanumeric') %}
{% if keep_type == 'numeric' %}
regexp_replace({{ column }}, '[^0-9]', '')
{% elif keep_type == 'alpha' %}
regexp_replace({{ column }}, '[^a-zA-Z]', '')
{% elif keep_type == 'alphanumeric' %}
regexp_replace({{ column }}, '[^a-zA-Z0-9]', '')
{% elif keep_type == 'printable' %}
regexp_replace({{ column }}, '[^\\x20-\\x7E]', '')
{% else %}
{{ column }}
{% endif %}
{% endmacro %}


{#
    Macro: conditional_replace
    Description: Replace values conditionally based on a condition.

    Alteryx Equivalent: Formula with IIF for conditional replacement

    Arguments:
        column: Column to potentially replace
        condition: When this condition is true
        new_value: Value to use when condition is true
        else_value: Value when condition is false (default: original column)

    Example Usage:
        {{ conditional_replace('status', "status = 'UNKNOWN'", "'N/A'") }}
#}

{% macro conditional_replace(column, condition, new_value, else_value=none) %}
case when {{ condition }} then {{ new_value }} else {% if else_value %}{{ else_value }}{% else %}{{ column }}{% endif %} end
{% endmacro %}


{#
    Macro: case_mapping
    Description: Map values using CASE expression.

    Alteryx Equivalent: Find Replace with value mapping

    Arguments:
        column: Column to map
        mappings: Dictionary of value: replacement pairs
        default: Default value if no match (default: original value)

    Example Usage:
        {{ case_mapping(
            'grade_code',
            {'A': 'Excellent', 'B': 'Good', 'C': 'Average', 'D': 'Below Average'},
            "'Other'"
        ) }}
#}

{% macro case_mapping(column, mappings, default=none) %}
case {{ column }}
    {%- for find_val, replace_val in mappings.items() %}
    when '{{ find_val }}' then {{ replace_val }}
    {%- endfor %}
    else {% if default %}{{ default }}{% else %}{{ column }}{% endif %}
end
{% endmacro %}


{#
    Macro: apply_find_replace
    Description: Applies find/replace to a relation, updating a column.

    Alteryx Equivalent: Find Replace tool output

    Arguments:
        relation: Source relation
        column: Column to update
        find_value: Value to find
        replace_value: Replacement value
        use_regex: Whether to use regex matching (default: false)
        case_sensitive: Whether matching is case-sensitive (default: true)

    Example Usage:
        {{ apply_find_replace(
            relation=ref('stg_addresses'),
            column='state',
            find_value='Calif.',
            replace_value='California'
        ) }}
#}

{% macro apply_find_replace(relation, column, find_value, replace_value, use_regex=false, case_sensitive=true) %}

select
    {%- set all_columns = adapter.get_columns_in_relation(relation) %}
    {%- for col in all_columns %}
        {%- if col.name == column %}
            {%- if use_regex %}
    regexp_replace({{ col.name }}, '{{ find_value }}', '{{ replace_value }}') as {{ col.name }}
            {%- elif not case_sensitive %}
    case
        when lower({{ col.name }}) like lower('%{{ find_value }}%')
        then replace(lower({{ col.name }}), lower('{{ find_value }}'), '{{ replace_value }}')
        else {{ col.name }}
    end as {{ col.name }}
            {%- else %}
    replace({{ col.name }}, '{{ find_value }}', '{{ replace_value }}') as {{ col.name }}
            {%- endif %}
        {%- else %}
    {{ col.name }}
        {%- endif %}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation }}

{% endmacro %}


{#
    Macro: standardize_values
    Description: Standardize inconsistent values to canonical form.

    Alteryx Equivalent: Find Replace for data standardization

    Arguments:
        column: Column to standardize
        standard_value: The canonical/standard value
        variations: List of variations to standardize

    Example Usage:
        {{ standardize_values(
            'country',
            'United States',
            ['US', 'USA', 'U.S.', 'U.S.A.', 'United States of America']
        ) }}
#}

{% macro standardize_values(column, standard_value, variations) %}
case
    when {{ column }} in (
        {%- for var in variations %}
        '{{ var }}'{% if not loop.last %}, {% endif %}
        {%- endfor %}
    ) then '{{ standard_value }}'
    else {{ column }}
end
{% endmacro %}
