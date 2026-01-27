{#
    =============================================================================
    Join and Union Macros
    Alteryx Equivalent: Join tool, Union tool, Append Fields, Join Multiple
    Trino Compatible: Yes
    =============================================================================

    These macros provide comprehensive join and union functionality matching
    Alteryx join tool capabilities.
#}

{#
    Macro: inner_join
    Description: Performs an inner join between two relations.

    Alteryx Equivalent: Join tool (Inner Join)

    Arguments:
        left_relation: Left/primary relation
        right_relation: Right relation
        left_key: Join key column(s) from left relation
        right_key: Join key column(s) from right relation (defaults to left_key)
        select_left: List of columns from left (default: all)
        select_right: List of columns from right (default: all except keys)

    Example Usage:
        {{ inner_join(
            left_relation=ref('orders'),
            right_relation=ref('customers'),
            left_key=['customer_id'],
            select_left=['order_id', 'customer_id', 'amount'],
            select_right=['customer_name', 'email']
        ) }}
#}

{% macro inner_join(left_relation, right_relation, left_key, right_key=none, select_left=none, select_right=none) %}

{%- set rkey = right_key if right_key else left_key -%}
{%- set lkey_list = left_key if left_key is iterable and left_key is not string else [left_key] -%}
{%- set rkey_list = rkey if rkey is iterable and rkey is not string else [rkey] -%}

select
    {%- if select_left %}
    {%- for col in select_left %}
    l.{{ col }},
    {%- endfor %}
    {%- else %}
    l.*,
    {%- endif %}
    {%- if select_right %}
    {%- for col in select_right %}
    r.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- else %}
    r.*
    {%- endif %}
from {{ left_relation }} l
inner join {{ right_relation }} r
    on {% for i in range(lkey_list | length) %}
    l.{{ lkey_list[i] }} = r.{{ rkey_list[i] }}{% if not loop.last %} and {% endif %}
    {%- endfor %}

{% endmacro %}


{#
    Macro: left_join
    Description: Performs a left outer join.

    Alteryx Equivalent: Join tool (Left Outer Join)

    Arguments:
        left_relation: Left/primary relation
        right_relation: Right relation
        left_key: Join key(s) from left
        right_key: Join key(s) from right
        select_left: Columns from left
        select_right: Columns from right

    Example Usage:
        {{ left_join(
            left_relation=ref('orders'),
            right_relation=ref('customers'),
            left_key='customer_id'
        ) }}
#}

{% macro left_join(left_relation, right_relation, left_key, right_key=none, select_left=none, select_right=none) %}

{%- set rkey = right_key if right_key else left_key -%}
{%- set lkey_list = left_key if left_key is iterable and left_key is not string else [left_key] -%}
{%- set rkey_list = rkey if rkey is iterable and rkey is not string else [rkey] -%}

select
    {%- if select_left %}
    {%- for col in select_left %}
    l.{{ col }},
    {%- endfor %}
    {%- else %}
    l.*,
    {%- endif %}
    {%- if select_right %}
    {%- for col in select_right %}
    r.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- else %}
    r.*
    {%- endif %}
from {{ left_relation }} l
left join {{ right_relation }} r
    on {% for i in range(lkey_list | length) %}
    l.{{ lkey_list[i] }} = r.{{ rkey_list[i] }}{% if not loop.last %} and {% endif %}
    {%- endfor %}

{% endmacro %}


{#
    Macro: right_join
    Description: Performs a right outer join.

    Alteryx Equivalent: Join tool (Right Outer Join)

    Arguments:
        Same as left_join

    Example Usage:
        {{ right_join(ref('orders'), ref('customers'), 'customer_id') }}
#}

{% macro right_join(left_relation, right_relation, left_key, right_key=none, select_left=none, select_right=none) %}

{%- set rkey = right_key if right_key else left_key -%}
{%- set lkey_list = left_key if left_key is iterable and left_key is not string else [left_key] -%}
{%- set rkey_list = rkey if rkey is iterable and rkey is not string else [rkey] -%}

select
    {%- if select_left %}
    {%- for col in select_left %}
    l.{{ col }},
    {%- endfor %}
    {%- else %}
    l.*,
    {%- endif %}
    {%- if select_right %}
    {%- for col in select_right %}
    r.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- else %}
    r.*
    {%- endif %}
from {{ left_relation }} l
right join {{ right_relation }} r
    on {% for i in range(lkey_list | length) %}
    l.{{ lkey_list[i] }} = r.{{ rkey_list[i] }}{% if not loop.last %} and {% endif %}
    {%- endfor %}

{% endmacro %}


{#
    Macro: full_outer_join
    Description: Performs a full outer join.

    Alteryx Equivalent: Join tool (Full Outer Join)

    Arguments:
        Same as other joins

    Example Usage:
        {{ full_outer_join(ref('table_a'), ref('table_b'), 'id') }}
#}

{% macro full_outer_join(left_relation, right_relation, left_key, right_key=none, select_left=none, select_right=none) %}

{%- set rkey = right_key if right_key else left_key -%}
{%- set lkey_list = left_key if left_key is iterable and left_key is not string else [left_key] -%}
{%- set rkey_list = rkey if rkey is iterable and rkey is not string else [rkey] -%}

select
    {%- if select_left %}
    {%- for col in select_left %}
    l.{{ col }},
    {%- endfor %}
    {%- else %}
    l.*,
    {%- endif %}
    {%- if select_right %}
    {%- for col in select_right %}
    r.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- else %}
    r.*
    {%- endif %}
from {{ left_relation }} l
full outer join {{ right_relation }} r
    on {% for i in range(lkey_list | length) %}
    l.{{ lkey_list[i] }} = r.{{ rkey_list[i] }}{% if not loop.last %} and {% endif %}
    {%- endfor %}

{% endmacro %}


{#
    Macro: cross_join
    Description: Performs a cross join (cartesian product).

    Alteryx Equivalent: Append Fields tool

    Arguments:
        left_relation: Left relation
        right_relation: Right relation

    Example Usage:
        {{ cross_join(ref('dates'), ref('products')) }}
#}

{% macro cross_join(left_relation, right_relation) %}

select
    l.*,
    r.*
from {{ left_relation }} l
cross join {{ right_relation }} r

{% endmacro %}


{#
    Macro: anti_join
    Description: Returns left rows that have no match in right (Left Only).

    Alteryx Equivalent: Join tool L output (non-matching from left)

    Arguments:
        left_relation: Left relation
        right_relation: Right relation
        left_key: Join key(s) from left
        right_key: Join key(s) from right

    Example Usage:
        {{ anti_join(ref('all_customers'), ref('active_orders'), 'customer_id') }}
#}

{% macro anti_join(left_relation, right_relation, left_key, right_key=none) %}

{%- set rkey = right_key if right_key else left_key -%}
{%- set lkey_list = left_key if left_key is iterable and left_key is not string else [left_key] -%}
{%- set rkey_list = rkey if rkey is iterable and rkey is not string else [rkey] -%}

select l.*
from {{ left_relation }} l
left join {{ right_relation }} r
    on {% for i in range(lkey_list | length) %}
    l.{{ lkey_list[i] }} = r.{{ rkey_list[i] }}{% if not loop.last %} and {% endif %}
    {%- endfor %}
where r.{{ rkey_list[0] }} is null

{% endmacro %}


{#
    Macro: semi_join
    Description: Returns left rows that have a match in right (exists check).

    Alteryx Equivalent: Join tool J output selecting only left columns

    Arguments:
        left_relation: Left relation
        right_relation: Right relation
        left_key: Join key(s) from left
        right_key: Join key(s) from right

    Example Usage:
        {{ semi_join(ref('products'), ref('order_items'), 'product_id') }}
#}

{% macro semi_join(left_relation, right_relation, left_key, right_key=none) %}

{%- set rkey = right_key if right_key else left_key -%}
{%- set lkey_list = left_key if left_key is iterable and left_key is not string else [left_key] -%}
{%- set rkey_list = rkey if rkey is iterable and rkey is not string else [rkey] -%}

select l.*
from {{ left_relation }} l
where exists (
    select 1
    from {{ right_relation }} r
    where {% for i in range(lkey_list | length) %}
    l.{{ lkey_list[i] }} = r.{{ rkey_list[i] }}{% if not loop.last %} and {% endif %}
    {%- endfor %}
)

{% endmacro %}


{#
    Macro: union_all
    Description: Stacks datasets vertically (keeps duplicates).

    Alteryx Equivalent: Union tool (default mode)

    Arguments:
        relations: List of relations to union

    Example Usage:
        {{ union_all([ref('sales_2022'), ref('sales_2023'), ref('sales_2024')]) }}
#}

{% macro union_all(relations) %}

{%- for relation in relations %}
select * from {{ relation }}
{% if not loop.last %}union all{% endif %}
{%- endfor %}

{% endmacro %}


{#
    Macro: union_distinct
    Description: Stacks datasets vertically with duplicate removal.

    Alteryx Equivalent: Union tool with Auto-config by name + Unique

    Arguments:
        relations: List of relations to union

    Example Usage:
        {{ union_distinct([ref('source_a'), ref('source_b')]) }}
#}

{% macro union_distinct(relations) %}

{%- for relation in relations %}
select * from {{ relation }}
{% if not loop.last %}union{% endif %}
{%- endfor %}

{% endmacro %}


{#
    Macro: union_with_source
    Description: Union with source identifier column.

    Alteryx Equivalent: Union tool + source tracking

    Arguments:
        relations: Dictionary of source_name: relation pairs

    Example Usage:
        {{ union_with_source({
            'web': ref('web_orders'),
            'mobile': ref('mobile_orders'),
            'store': ref('store_orders')
        }) }}
#}

{% macro union_with_source(relations) %}

{%- for source_name, relation in relations.items() %}
select
    '{{ source_name }}' as _source,
    *
from {{ relation }}
{% if not loop.last %}union all{% endif %}
{%- endfor %}

{% endmacro %}


{#
    Macro: join_multiple
    Description: Joins multiple tables together.

    Alteryx Equivalent: Join Multiple tool

    Arguments:
        base_relation: Starting relation
        joins: List of {relation: ..., key: ..., type: ...} dictionaries

    Example Usage:
        {{ join_multiple(
            base_relation=ref('orders'),
            joins=[
                {'relation': ref('customers'), 'key': 'customer_id', 'type': 'left'},
                {'relation': ref('products'), 'key': 'product_id', 'type': 'left'},
                {'relation': ref('regions'), 'key': 'region_id', 'type': 'inner'}
            ]
        ) }}
#}

{% macro join_multiple(base_relation, joins) %}

select
    t0.*
    {%- for i in range(joins | length) %},
    t{{ i + 1 }}.*
    {%- endfor %}
from {{ base_relation }} t0
{%- for i, join_spec in enumerate(joins) %}
{{ join_spec.get('type', 'left') }} join {{ join_spec.relation }} t{{ i + 1 }}
    on t0.{{ join_spec.key }} = t{{ i + 1 }}.{{ join_spec.key }}
{%- endfor %}

{% endmacro %}


{#
    Macro: except_rows
    Description: Returns rows in first relation not in second.

    Alteryx Equivalent: Unique tool comparing two inputs

    Arguments:
        relation_a: First relation
        relation_b: Second relation (rows to exclude)

    Example Usage:
        {{ except_rows(ref('all_products'), ref('discontinued_products')) }}
#}

{% macro except_rows(relation_a, relation_b) %}

select * from {{ relation_a }}
except
select * from {{ relation_b }}

{% endmacro %}


{#
    Macro: intersect_rows
    Description: Returns rows present in both relations.

    Alteryx Equivalent: Join to find matching records

    Arguments:
        relation_a: First relation
        relation_b: Second relation

    Example Usage:
        {{ intersect_rows(ref('customers_2023'), ref('customers_2024')) }}
#}

{% macro intersect_rows(relation_a, relation_b) %}

select * from {{ relation_a }}
intersect
select * from {{ relation_b }}

{% endmacro %}


{#
    Macro: lookup_join
    Description: Enriches data by looking up values from a reference table.

    Alteryx Equivalent: Find Replace / Join for lookup

    Arguments:
        fact_relation: Main/fact relation
        lookup_relation: Lookup/dimension relation
        fact_key: Key column in fact
        lookup_key: Key column in lookup
        lookup_columns: Columns to add from lookup

    Example Usage:
        {{ lookup_join(
            fact_relation=ref('orders'),
            lookup_relation=ref('dim_products'),
            fact_key='product_id',
            lookup_key='product_id',
            lookup_columns=['product_name', 'category', 'unit_price']
        ) }}
#}

{% macro lookup_join(fact_relation, lookup_relation, fact_key, lookup_key, lookup_columns) %}

select
    f.*,
    {%- for col in lookup_columns %}
    l.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from {{ fact_relation }} f
left join {{ lookup_relation }} l
    on f.{{ fact_key }} = l.{{ lookup_key }}

{% endmacro %}
