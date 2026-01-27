{#
    =============================================================================
    Regex Functions Macros
    Alteryx Equivalent: RegEx tool
    Trino Compatible: Yes
    =============================================================================

    These macros provide regular expression functionality matching Alteryx
    RegEx tool capabilities.
#}

{#
    Macro: regex_match
    Description: Returns true if pattern matches.

    Alteryx Equivalent: REGEX_Match([field], pattern)

    Arguments:
        column: Column to test
        pattern: Regular expression pattern

    Example Usage:
        where {{ regex_match('email', '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$') }}
#}

{% macro regex_match(column, pattern) %}
regexp_like({{ column }}, '{{ pattern }}')
{% endmacro %}


{#
    Macro: regex_extract
    Description: Extracts text matching a pattern.

    Alteryx Equivalent: REGEX_Replace with capture groups

    Arguments:
        column: Column to extract from
        pattern: Regular expression pattern
        group: Capture group to extract (default: 0 = entire match)

    Example Usage:
        {{ regex_extract('url', 'https?://([^/]+)', 1) }} as domain
#}

{% macro regex_extract(column, pattern, group=0) %}
regexp_extract({{ column }}, '{{ pattern }}', {{ group }})
{% endmacro %}


{#
    Macro: regex_extract_all
    Description: Extracts all occurrences matching a pattern.

    Alteryx Equivalent: RegEx tool with tokenize mode

    Arguments:
        column: Column to extract from
        pattern: Regular expression pattern
        group: Capture group (default: 0)

    Example Usage:
        {{ regex_extract_all('text', '\\b[A-Z]{2,}\\b') }} as all_acronyms
#}

{% macro regex_extract_all(column, pattern, group=0) %}
regexp_extract_all({{ column }}, '{{ pattern }}', {{ group }})
{% endmacro %}


{#
    Macro: regex_replace
    Description: Replaces text matching pattern.

    Alteryx Equivalent: REGEX_Replace([field], pattern, replacement)

    Arguments:
        column: Column to perform replacement on
        pattern: Regular expression pattern
        replacement: Replacement string (can use $1, $2 for groups)

    Example Usage:
        {{ regex_replace('phone', '[^0-9]', '') }} as phone_digits_only
#}

{% macro regex_replace(column, pattern, replacement='') %}
regexp_replace({{ column }}, '{{ pattern }}', '{{ replacement }}')
{% endmacro %}


{#
    Macro: regex_count
    Description: Counts matches of a pattern.

    Alteryx Equivalent: REGEX_CountMatches

    Arguments:
        column: Column to search
        pattern: Regular expression pattern

    Example Usage:
        {{ regex_count('text', '\\bword\\b') }} as word_occurrences
#}

{% macro regex_count(column, pattern) %}
cardinality(regexp_extract_all({{ column }}, '{{ pattern }}'))
{% endmacro %}


{#
    Macro: regex_split
    Description: Splits a string by regex pattern into an array.

    Alteryx Equivalent: Text to Columns with regex delimiter

    Arguments:
        column: Column to split
        pattern: Regex pattern to split on

    Example Usage:
        {{ regex_split('tags', ',\\s*') }} as tag_array
#}

{% macro regex_split(column, pattern) %}
regexp_split({{ column }}, '{{ pattern }}')
{% endmacro %}


{#
    Macro: extract_numbers
    Description: Extracts all numeric characters from a string.

    Alteryx Equivalent: REGEX_Replace([field], '[^0-9]', '')

    Arguments:
        column: Column to extract numbers from

    Example Usage:
        {{ extract_numbers('mixed_text') }} as numbers_only
#}

{% macro extract_numbers(column) %}
regexp_replace({{ column }}, '[^0-9]', '')
{% endmacro %}


{#
    Macro: extract_first_number
    Description: Extracts the first number from a string.

    Alteryx Equivalent: REGEX_Match pattern for first number

    Arguments:
        column: Column to extract from
        include_decimals: Include decimal numbers (default: true)
        include_negative: Include negative numbers (default: true)

    Example Usage:
        {{ extract_first_number('price_text') }} as price_value
#}

{% macro extract_first_number(column, include_decimals=true, include_negative=true) %}
{% if include_negative and include_decimals %}
try_cast(regexp_extract({{ column }}, '-?[0-9]+\\.?[0-9]*') as double)
{% elif include_decimals %}
try_cast(regexp_extract({{ column }}, '[0-9]+\\.?[0-9]*') as double)
{% elif include_negative %}
try_cast(regexp_extract({{ column }}, '-?[0-9]+') as bigint)
{% else %}
try_cast(regexp_extract({{ column }}, '[0-9]+') as bigint)
{% endif %}
{% endmacro %}


{#
    Macro: extract_email
    Description: Extracts email address from text.

    Alteryx Equivalent: RegEx tool with email pattern

    Arguments:
        column: Column to extract email from

    Example Usage:
        {{ extract_email('contact_info') }} as email_address
#}

{% macro extract_email(column) %}
regexp_extract({{ column }}, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}')
{% endmacro %}


{#
    Macro: extract_phone
    Description: Extracts phone number from text.

    Alteryx Equivalent: RegEx tool with phone pattern

    Arguments:
        column: Column to extract phone from
        format: 'raw' (just digits), 'us' (US format), 'international'

    Example Usage:
        {{ extract_phone('contact_info', 'us') }} as phone_number
#}

{% macro extract_phone(column, format='raw') %}
{% if format == 'raw' %}
regexp_replace(regexp_extract({{ column }}, '[0-9()\\-\\s.+]{7,}'), '[^0-9]', '')
{% elif format == 'us' %}
regexp_extract({{ column }}, '\\(?[0-9]{3}\\)?[\\s.-]?[0-9]{3}[\\s.-]?[0-9]{4}')
{% else %}
regexp_extract({{ column }}, '\\+?[0-9][0-9()\\-\\s.]{8,}[0-9]')
{% endif %}
{% endmacro %}


{#
    Macro: extract_url
    Description: Extracts URL from text.

    Alteryx Equivalent: RegEx tool with URL pattern

    Arguments:
        column: Column to extract URL from

    Example Usage:
        {{ extract_url('message') }} as url
#}

{% macro extract_url(column) %}
regexp_extract({{ column }}, 'https?://[^\\s<>"\\{\\}|\\\\^\\[\\]`]+')
{% endmacro %}


{#
    Macro: extract_domain
    Description: Extracts domain from URL or email.

    Alteryx Equivalent: RegEx tool for domain extraction

    Arguments:
        column: Column containing URL or email

    Example Usage:
        {{ extract_domain('website') }} as domain
#}

{% macro extract_domain(column) %}
coalesce(
    regexp_extract({{ column }}, 'https?://([^/]+)', 1),
    regexp_extract({{ column }}, '@([a-zA-Z0-9.-]+)', 1)
)
{% endmacro %}


{#
    Macro: is_valid_email
    Description: Returns true if value is a valid email format.

    Alteryx Equivalent: RegEx validation for email

    Arguments:
        column: Column to validate

    Example Usage:
        where {{ is_valid_email('email') }}
#}

{% macro is_valid_email(column) %}
regexp_like({{ column }}, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
{% endmacro %}


{#
    Macro: is_valid_phone
    Description: Returns true if value looks like a phone number.

    Alteryx Equivalent: RegEx validation for phone

    Arguments:
        column: Column to validate
        min_digits: Minimum number of digits (default: 10)

    Example Usage:
        where {{ is_valid_phone('phone') }}
#}

{% macro is_valid_phone(column, min_digits=10) %}
length(regexp_replace({{ column }}, '[^0-9]', '')) >= {{ min_digits }}
{% endmacro %}


{#
    Macro: mask_pattern
    Description: Masks text matching a pattern.

    Alteryx Equivalent: RegEx Replace for data masking

    Arguments:
        column: Column to mask
        pattern: Pattern to match for masking
        mask_char: Character to use for masking (default: 'X')
        keep_length: Preserve original length with mask (default: true)

    Example Usage:
        {{ mask_pattern('ssn', '[0-9]', 'X') }} as masked_ssn
#}

{% macro mask_pattern(column, pattern, mask_char='X', keep_length=true) %}
{% if keep_length %}
regexp_replace({{ column }}, '{{ pattern }}', '{{ mask_char }}')
{% else %}
'{{ mask_char * 5 }}'
{% endif %}
{% endmacro %}


{#
    Macro: tokenize
    Description: Splits text into tokens/words.

    Alteryx Equivalent: RegEx tool (Tokenize mode)

    Arguments:
        column: Column to tokenize
        delimiter_pattern: Pattern for token boundaries (default: whitespace)

    Example Usage:
        {{ tokenize('description') }} as words
#}

{% macro tokenize(column, delimiter_pattern='\\s+') %}
regexp_split({{ column }}, '{{ delimiter_pattern }}')
{% endmacro %}


{#
    Macro: clean_html
    Description: Removes HTML tags from text.

    Alteryx Equivalent: RegEx Replace to strip HTML

    Arguments:
        column: Column containing HTML

    Example Usage:
        {{ clean_html('html_content') }} as plain_text
#}

{% macro clean_html(column) %}
regexp_replace({{ column }}, '<[^>]+>', '')
{% endmacro %}


{#
    Macro: parse_key_value
    Description: Extracts value for a key from key=value format.

    Alteryx Equivalent: RegEx tool for parsing

    Arguments:
        column: Column containing key=value pairs
        key: Key to extract value for
        delimiter: Key-value delimiter (default: '=')

    Example Usage:
        {{ parse_key_value('params', 'user_id') }} as user_id
#}

{% macro parse_key_value(column, key, delimiter='=') %}
regexp_extract({{ column }}, '{{ key }}{{ delimiter }}([^&\\s,;]+)', 1)
{% endmacro %}
