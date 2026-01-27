{#
    =============================================================================
    Conditional Logic Macros
    Alteryx Equivalent: Formula tool conditional functions
    Trino Compatible: Yes
    =============================================================================

    These macros provide conditional logic and branching functionality
    matching Alteryx Formula tool capabilities.
#}

{#
    Macro: case_when
    Description: Multi-condition CASE WHEN statement.

    Alteryx Equivalent: Nested IIF or Switch function

    Arguments:
        conditions: List of [condition, result] pairs
        else_value: Default value if no conditions match

    Example Usage:
        {{ case_when(
            conditions=[
                ["age < 18", "'Minor'"],
                ["age < 65", "'Adult'"],
                ["age >= 65", "'Senior'"]
            ],
            else_value="'Unknown'"
        ) }}
#}

{% macro case_when(conditions, else_value='null') %}
case
    {%- for condition, result in conditions %}
    when {{ condition }} then {{ result }}
    {%- endfor %}
    else {{ else_value }}
end
{% endmacro %}


{#
    Macro: case_value
    Description: CASE expression matching specific values.

    Alteryx Equivalent: Switch([field], default, val1, result1, ...)

    Arguments:
        column: Column to evaluate
        value_map: Dictionary of value: result pairs
        else_value: Default value

    Example Usage:
        {{ case_value(
            'status_code',
            value_map={'A': "'Active'", 'I': "'Inactive'", 'P': "'Pending'"},
            else_value="'Unknown'"
        ) }}
#}

{% macro case_value(column, value_map, else_value='null') %}
case {{ column }}
    {%- for val, result in value_map.items() %}
    when '{{ val }}' then {{ result }}
    {%- endfor %}
    else {{ else_value }}
end
{% endmacro %}


{#
    Macro: if_null
    Description: Returns fallback if value is null.

    Alteryx Equivalent: IIF(IsNull([field]), fallback, [field])

    Arguments:
        column: Column to check
        fallback: Value to use if null

    Example Usage:
        {{ if_null('discount', 0) }} as discount
#}

{% macro if_null(column, fallback) %}
coalesce({{ column }}, {{ fallback }})
{% endmacro %}


{#
    Macro: if_empty
    Description: Returns fallback if value is null or empty string.

    Alteryx Equivalent: IIF(IsNull([field]) OR IsEmpty([field]), fallback, [field])

    Arguments:
        column: Column to check
        fallback: Value to use if null/empty

    Example Usage:
        {{ if_empty('notes', "'No notes'"') }} as notes
#}

{% macro if_empty(column, fallback) %}
case
    when {{ column }} is null or trim({{ column }}) = ''
    then {{ fallback }}
    else {{ column }}
end
{% endmacro %}


{#
    Macro: if_zero
    Description: Returns fallback if value is zero.

    Alteryx Equivalent: IIF([field] = 0, fallback, [field])

    Arguments:
        column: Column to check
        fallback: Value to use if zero

    Example Usage:
        {{ if_zero('quantity', 1) }} as quantity
#}

{% macro if_zero(column, fallback) %}
case when {{ column }} = 0 then {{ fallback }} else {{ column }} end
{% endmacro %}


{#
    Macro: null_if
    Description: Returns null if condition is true.

    Alteryx Equivalent: IIF([condition], Null(), [field])

    Arguments:
        column: Column to potentially nullify
        condition: When true, return null

    Example Usage:
        {{ null_if('value', "value = 0") }} as value_or_null
#}

{% macro null_if(column, condition) %}
case when {{ condition }} then null else {{ column }} end
{% endmacro %}


{#
    Macro: decode
    Description: Oracle-style DECODE function.

    Alteryx Equivalent: Switch function

    Arguments:
        column: Column to evaluate
        pairs: List of [search_value, result] pairs
        default: Default value

    Example Usage:
        {{ decode('grade', [['A', 4], ['B', 3], ['C', 2], ['D', 1], ['F', 0]], 0) }}
#}

{% macro decode(column, pairs, default='null') %}
case {{ column }}
    {%- for search_val, result in pairs %}
    when {{ search_val }} then {{ result }}
    {%- endfor %}
    else {{ default }}
end
{% endmacro %}


{#
    Macro: between_check
    Description: Returns boolean for between check.

    Alteryx Equivalent: [field] >= lower AND [field] <= upper

    Arguments:
        column: Column to check
        lower: Lower bound
        upper: Upper bound
        inclusive: Include bounds (default: true)

    Example Usage:
        {{ between_check('age', 18, 65) }} as is_working_age
#}

{% macro between_check(column, lower, upper, inclusive=true) %}
{% if inclusive %}
({{ column }} >= {{ lower }} and {{ column }} <= {{ upper }})
{% else %}
({{ column }} > {{ lower }} and {{ column }} < {{ upper }})
{% endif %}
{% endmacro %}


{#
    Macro: in_list_check
    Description: Returns boolean for list membership.

    Alteryx Equivalent: [field] IN (val1, val2, ...)

    Arguments:
        column: Column to check
        values: List of values

    Example Usage:
        {{ in_list_check('status', ['active', 'pending']) }} as is_active_or_pending
#}

{% macro in_list_check(column, values) %}
{{ column }} in (
    {%- for val in values %}
    '{{ val }}'{% if not loop.last %}, {% endif %}
    {%- endfor %}
)
{% endmacro %}


{#
    Macro: greatest_value
    Description: Returns the greatest of multiple values.

    Alteryx Equivalent: Max([field1], [field2], ...)

    Arguments:
        values: List of columns/expressions

    Example Usage:
        {{ greatest_value(['score1', 'score2', 'score3']) }} as max_score
#}

{% macro greatest_value(values) %}
greatest({{ values | join(', ') }})
{% endmacro %}


{#
    Macro: least_value
    Description: Returns the least of multiple values.

    Alteryx Equivalent: Min([field1], [field2], ...)

    Arguments:
        values: List of columns/expressions

    Example Usage:
        {{ least_value(['price1', 'price2', 'price3']) }} as min_price
#}

{% macro least_value(values) %}
least({{ values | join(', ') }})
{% endmacro %}


{#
    Macro: nvl2
    Description: Oracle-style NVL2 - returns expr1 if not null, else expr2.

    Alteryx Equivalent: IIF(IsNull([field]), null_result, not_null_result)

    Arguments:
        column: Column to check
        not_null_result: Result when column is not null
        null_result: Result when column is null

    Example Usage:
        {{ nvl2('email', "'Has Email'", "'No Email'") }} as email_status
#}

{% macro nvl2(column, not_null_result, null_result) %}
case when {{ column }} is not null then {{ not_null_result }} else {{ null_result }} end
{% endmacro %}


{#
    Macro: flag_column
    Description: Creates a boolean/integer flag based on condition.

    Alteryx Equivalent: IIF([condition], 1, 0) or IIF([condition], True, False)

    Arguments:
        condition: Condition expression
        true_value: Value when true (default: 1)
        false_value: Value when false (default: 0)

    Example Usage:
        {{ flag_column("amount > 1000", 1, 0) }} as is_large_order
#}

{% macro flag_column(condition, true_value=1, false_value=0) %}
case when {{ condition }} then {{ true_value }} else {{ false_value }} end
{% endmacro %}


{#
    Macro: category_column
    Description: Categorizes values into groups.

    Alteryx Equivalent: Multi-level IIF or Formula categorization

    Arguments:
        column: Column to categorize
        thresholds: List of threshold values
        labels: List of labels (should be len(thresholds) + 1)
        ascending: Whether thresholds are in ascending order

    Example Usage:
        {{ category_column(
            'revenue',
            thresholds=[1000, 10000, 100000],
            labels=['Small', 'Medium', 'Large', 'Enterprise']
        ) }} as customer_tier
#}

{% macro category_column(column, thresholds, labels, ascending=true) %}
case
    {%- if ascending %}
        {%- for i in range(thresholds | length) %}
    when {{ column }} <= {{ thresholds[i] }} then '{{ labels[i] }}'
        {%- endfor %}
    else '{{ labels[-1] }}'
        {%- else %}
        {%- for i in range(thresholds | length) %}
    when {{ column }} >= {{ thresholds[i] }} then '{{ labels[i] }}'
        {%- endfor %}
    else '{{ labels[-1] }}'
    {%- endif %}
end
{% endmacro %}


{#
    Macro: age_from_birthdate
    Description: Calculates age from birthdate.

    Alteryx Equivalent: DateTimeDiff formula for age

    Arguments:
        birthdate_column: Column containing birthdate
        as_of_date: Date to calculate age as of (default: current_date)

    Example Usage:
        {{ age_from_birthdate('date_of_birth') }} as current_age
#}

{% macro age_from_birthdate(birthdate_column, as_of_date='current_date') %}
date_diff('year', {{ birthdate_column }}, {{ as_of_date }}) -
case
    when date_add('year', date_diff('year', {{ birthdate_column }}, {{ as_of_date }}), {{ birthdate_column }}) > {{ as_of_date }}
    then 1
    else 0
end
{% endmacro %}


{#
    Macro: days_until
    Description: Calculates days until a future date.

    Alteryx Equivalent: DateTimeDiff formula

    Arguments:
        target_date: Target date column
        from_date: Starting date (default: current_date)

    Example Usage:
        {{ days_until('expiration_date') }} as days_to_expiry
#}

{% macro days_until(target_date, from_date='current_date') %}
date_diff('day', {{ from_date }}, {{ target_date }})
{% endmacro %}


{#
    Macro: is_weekend
    Description: Returns true if date is a weekend.

    Alteryx Equivalent: Formula with day of week check

    Arguments:
        date_column: Date column to check

    Example Usage:
        {{ is_weekend('order_date') }} as is_weekend_order
#}

{% macro is_weekend(date_column) %}
day_of_week({{ date_column }}) in (6, 7)
{% endmacro %}


{#
    Macro: fiscal_year
    Description: Calculates fiscal year from date.

    Alteryx Equivalent: Formula for fiscal year

    Arguments:
        date_column: Date column
        fiscal_start_month: First month of fiscal year (default: 1 = January)

    Example Usage:
        {{ fiscal_year('transaction_date', 7) }} as fiscal_year  -- July fiscal year start
#}

{% macro fiscal_year(date_column, fiscal_start_month=1) %}
{% if fiscal_start_month == 1 %}
year({{ date_column }})
{% else %}
case
    when month({{ date_column }}) >= {{ fiscal_start_month }}
    then year({{ date_column }}) + 1
    else year({{ date_column }})
end
{% endif %}
{% endmacro %}
