{#
    =============================================================================
    Math Functions Macros
    Alteryx Equivalent: Formula tool with math functions
    Trino Compatible: Yes
    =============================================================================

    These macros provide mathematical function helpers matching Alteryx
    formula tool math capabilities.
#}

{#
    Macro: abs_value
    Description: Returns absolute value.

    Alteryx Equivalent: Abs([field])

    Arguments:
        column: Column or expression

    Example Usage:
        {{ abs_value('profit_loss') }} as absolute_value
#}

{% macro abs_value(column) %}
abs({{ column }})
{% endmacro %}


{#
    Macro: round_value
    Description: Rounds a number to specified decimal places.

    Alteryx Equivalent: Round([field], decimals)

    Arguments:
        column: Column or expression to round
        decimals: Number of decimal places (default: 0)

    Example Usage:
        {{ round_value('price', 2) }} as rounded_price
#}

{% macro round_value(column, decimals=0) %}
round({{ column }}, {{ decimals }})
{% endmacro %}


{#
    Macro: ceil_value
    Description: Rounds up to nearest integer.

    Alteryx Equivalent: Ceil([field])

    Arguments:
        column: Column or expression

    Example Usage:
        {{ ceil_value('quantity / units_per_box') }} as boxes_needed
#}

{% macro ceil_value(column) %}
ceil({{ column }})
{% endmacro %}


{#
    Macro: floor_value
    Description: Rounds down to nearest integer.

    Alteryx Equivalent: Floor([field])

    Arguments:
        column: Column or expression

    Example Usage:
        {{ floor_value('total / page_size') }} as full_pages
#}

{% macro floor_value(column) %}
floor({{ column }})
{% endmacro %}


{#
    Macro: truncate_value
    Description: Truncates a number to specified decimal places.

    Alteryx Equivalent: Truncate equivalent

    Arguments:
        column: Column or expression
        decimals: Number of decimal places (default: 0)

    Example Usage:
        {{ truncate_value('amount', 2) }} as truncated_amount
#}

{% macro truncate_value(column, decimals=0) %}
truncate({{ column }} * power(10, {{ decimals }})) / power(10, {{ decimals }})
{% endmacro %}


{#
    Macro: power_value
    Description: Raises a number to a power.

    Alteryx Equivalent: Pow([base], [exponent])

    Arguments:
        base: Base value
        exponent: Exponent value

    Example Usage:
        {{ power_value('2', 10) }} as two_to_tenth
#}

{% macro power_value(base, exponent) %}
power({{ base }}, {{ exponent }})
{% endmacro %}


{#
    Macro: sqrt_value
    Description: Calculates square root.

    Alteryx Equivalent: Sqrt([field])

    Arguments:
        column: Column or expression

    Example Usage:
        {{ sqrt_value('variance') }} as std_dev
#}

{% macro sqrt_value(column) %}
sqrt({{ column }})
{% endmacro %}


{#
    Macro: log_value
    Description: Calculates logarithm.

    Alteryx Equivalent: Log([field]) or Log10([field])

    Arguments:
        column: Column or expression
        base: Logarithm base ('natural', '10', '2', or numeric)

    Example Usage:
        {{ log_value('population', 'natural') }} as ln_population
        {{ log_value('value', 10) }} as log10_value
#}

{% macro log_value(column, base='natural') %}
{% if base == 'natural' %}
ln({{ column }})
{% elif base == 10 or base == '10' %}
log10({{ column }})
{% elif base == 2 or base == '2' %}
log2({{ column }})
{% else %}
log({{ base }}, {{ column }})
{% endif %}
{% endmacro %}


{#
    Macro: exp_value
    Description: Calculates e raised to a power.

    Alteryx Equivalent: Exp([field])

    Arguments:
        column: Column or expression (the exponent)

    Example Usage:
        {{ exp_value('growth_rate') }} as exponential_growth
#}

{% macro exp_value(column) %}
exp({{ column }})
{% endmacro %}


{#
    Macro: mod_value
    Description: Calculates modulo (remainder).

    Alteryx Equivalent: Mod([dividend], [divisor])

    Arguments:
        dividend: The number to divide
        divisor: The number to divide by

    Example Usage:
        {{ mod_value('row_num', 10) }} as batch_position
#}

{% macro mod_value(dividend, divisor) %}
mod({{ dividend }}, {{ divisor }})
{% endmacro %}


{#
    Macro: sign_value
    Description: Returns the sign of a number (-1, 0, or 1).

    Alteryx Equivalent: Sign([field])

    Arguments:
        column: Column or expression

    Example Usage:
        {{ sign_value('balance') }} as balance_direction
#}

{% macro sign_value(column) %}
sign({{ column }})
{% endmacro %}


{#
    Macro: rand_value
    Description: Generates a random number.

    Alteryx Equivalent: Rand()

    Arguments:
        min_val: Minimum value (default: 0)
        max_val: Maximum value (default: 1)

    Example Usage:
        {{ rand_value(1, 100) }} as random_1_to_100
#}

{% macro rand_value(min_val=0, max_val=1) %}
{% if min_val == 0 and max_val == 1 %}
random()
{% else %}
random() * ({{ max_val }} - {{ min_val }}) + {{ min_val }}
{% endif %}
{% endmacro %}


{#
    Macro: rand_int
    Description: Generates a random integer.

    Alteryx Equivalent: RandInt([min], [max])

    Arguments:
        min_val: Minimum value (inclusive)
        max_val: Maximum value (inclusive)

    Example Usage:
        {{ rand_int(1, 6) }} as dice_roll
#}

{% macro rand_int(min_val, max_val) %}
floor(random() * ({{ max_val }} - {{ min_val }} + 1)) + {{ min_val }}
{% endmacro %}


{#
    Macro: safe_divide
    Description: Division that returns null or default on divide by zero.

    Alteryx Equivalent: IIF([divisor] = 0, 0, [dividend] / [divisor])

    Arguments:
        dividend: The number to divide
        divisor: The number to divide by
        default: Value when dividing by zero (default: null)

    Example Usage:
        {{ safe_divide('total_revenue', 'transaction_count', 0) }} as avg_transaction
#}

{% macro safe_divide(dividend, divisor, default=none) %}
{% if default is not none %}
coalesce({{ dividend }} / nullif({{ divisor }}, 0), {{ default }})
{% else %}
{{ dividend }} / nullif({{ divisor }}, 0)
{% endif %}
{% endmacro %}


{#
    Macro: percentage
    Description: Calculates percentage.

    Alteryx Equivalent: Formula for percentage calculation

    Arguments:
        part: The part value
        whole: The whole value
        decimals: Decimal places in result (default: 2)
        multiply_100: Whether to multiply by 100 (default: true)

    Example Usage:
        {{ percentage('completed_tasks', 'total_tasks') }} as completion_pct
#}

{% macro percentage(part, whole, decimals=2, multiply_100=true) %}
round(
    {{ part }} * 1.0 / nullif({{ whole }}, 0){% if multiply_100 %} * 100{% endif %},
    {{ decimals }}
)
{% endmacro %}


{#
    Macro: growth_rate
    Description: Calculates growth rate between two values.

    Alteryx Equivalent: Formula for growth calculation

    Arguments:
        current_value: Current period value
        previous_value: Previous period value
        as_percentage: Return as percentage (default: true)
        decimals: Decimal places (default: 2)

    Example Usage:
        {{ growth_rate('current_revenue', 'previous_revenue') }} as revenue_growth
#}

{% macro growth_rate(current_value, previous_value, as_percentage=true, decimals=2) %}
round(
    ({{ current_value }} - {{ previous_value }}) / nullif(abs({{ previous_value }}), 0)
    {% if as_percentage %} * 100{% endif %},
    {{ decimals }}
)
{% endmacro %}


{#
    Macro: min_max_normalize
    Description: Normalizes values to 0-1 range using min-max scaling.

    Alteryx Equivalent: Formula for normalization

    Arguments:
        column: Column to normalize
        min_expr: Expression for minimum (or literal)
        max_expr: Expression for maximum (or literal)

    Example Usage:
        select {{ min_max_normalize('score', 'min(score) over ()', 'max(score) over ()') }} as normalized_score
#}

{% macro min_max_normalize(column, min_expr, max_expr) %}
({{ column }} - ({{ min_expr }})) / nullif(({{ max_expr }}) - ({{ min_expr }}), 0)
{% endmacro %}


{#
    Macro: z_score
    Description: Calculates z-score (standard score).

    Alteryx Equivalent: Formula for standardization

    Arguments:
        column: Column to standardize
        mean_expr: Expression for mean
        stddev_expr: Expression for standard deviation

    Example Usage:
        select {{ z_score('value', 'avg(value) over ()', 'stddev(value) over ()') }} as z_score
#}

{% macro z_score(column, mean_expr, stddev_expr) %}
({{ column }} - ({{ mean_expr }})) / nullif(({{ stddev_expr }}), 0)
{% endmacro %}


{#
    Macro: clamp_value
    Description: Clamps a value between min and max bounds.

    Alteryx Equivalent: IIF([value] < min, min, IIF([value] > max, max, [value]))

    Arguments:
        column: Column to clamp
        min_val: Minimum allowed value
        max_val: Maximum allowed value

    Example Usage:
        {{ clamp_value('score', 0, 100) }} as clamped_score
#}

{% macro clamp_value(column, min_val, max_val) %}
greatest(least({{ column }}, {{ max_val }}), {{ min_val }})
{% endmacro %}


{#
    Macro: trig_function
    Description: Trigonometric functions.

    Alteryx Equivalent: Sin(), Cos(), Tan(), etc.

    Arguments:
        column: Column or expression (in radians)
        func: 'sin', 'cos', 'tan', 'asin', 'acos', 'atan'

    Example Usage:
        {{ trig_function('angle_radians', 'sin') }} as sine_value
#}

{% macro trig_function(column, func='sin') %}
{% if func == 'sin' %}
sin({{ column }})
{% elif func == 'cos' %}
cos({{ column }})
{% elif func == 'tan' %}
tan({{ column }})
{% elif func == 'asin' %}
asin({{ column }})
{% elif func == 'acos' %}
acos({{ column }})
{% elif func == 'atan' %}
atan({{ column }})
{% else %}
{{ column }}
{% endif %}
{% endmacro %}


{#
    Macro: degrees_to_radians
    Description: Converts degrees to radians.

    Alteryx Equivalent: Formula with PI conversion

    Arguments:
        column: Column in degrees

    Example Usage:
        {{ degrees_to_radians('angle_degrees') }} as angle_radians
#}

{% macro degrees_to_radians(column) %}
({{ column }} * pi() / 180)
{% endmacro %}


{#
    Macro: radians_to_degrees
    Description: Converts radians to degrees.

    Arguments:
        column: Column in radians

    Example Usage:
        {{ radians_to_degrees('angle_radians') }} as angle_degrees
#}

{% macro radians_to_degrees(column) %}
({{ column }} * 180 / pi())
{% endmacro %}
