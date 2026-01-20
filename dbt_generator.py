"""
DBT project scaffolding generator for Starburst (Trino).
Generates starter DBT models based on Alteryx workflow analysis.

Target Platform: Starburst (Trino-based)
SQL Dialect: Trino SQL
"""
import os
import re
from pathlib import Path
from typing import List, Dict, Set, Optional
from datetime import datetime

from models import (
    AlteryxWorkflow, AlteryxNode, MedallionLayer, ToolCategory
)
from transformation_analyzer import TransformationAnalyzer
from tool_mappings import get_dbt_prefix, AGGREGATION_MAP


class DBTGenerator:
    """Generates DBT project structure from Alteryx workflows."""

    def __init__(self, output_dir: str, project_name: str = "alteryx_migration"):
        self.output_dir = Path(output_dir)
        self.project_name = project_name
        self.sources: Dict[str, Set[str]] = {}  # schema -> tables
        self.models_generated: List[str] = []

    def generate(self, workflows: List[AlteryxWorkflow]) -> None:
        """Generate complete DBT project from workflows."""
        # Create directory structure
        self._create_structure()

        # Collect all sources
        self._collect_sources(workflows)

        # Generate source definitions
        self._generate_sources_yml()

        # Generate models for each workflow
        for workflow in workflows:
            self._generate_workflow_models(workflow)

        # Generate schema.yml
        self._generate_schema_yml()

        # Generate dbt_project.yml
        self._generate_project_yml()

        print(f"DBT project generated at: {self.output_dir}")
        print(f"Models generated: {len(self.models_generated)}")

    def _create_structure(self) -> None:
        """Create DBT project directory structure."""
        dirs = [
            self.output_dir,
            self.output_dir / "models",
            self.output_dir / "models" / "staging",
            self.output_dir / "models" / "intermediate",
            self.output_dir / "models" / "marts" / "core",
            self.output_dir / "models" / "marts" / "dimensions",
            self.output_dir / "macros",
            self.output_dir / "tests",
        ]

        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

    def _collect_sources(self, workflows: List[AlteryxWorkflow]) -> None:
        """Collect all data sources from workflows."""
        for workflow in workflows:
            for node in workflow.sources:
                schema = self._get_schema_name(node)
                table = self._get_table_name(node)

                if schema not in self.sources:
                    self.sources[schema] = set()
                self.sources[schema].add(table)

    def _get_schema_name(self, node: AlteryxNode) -> str:
        """Determine schema name from source node."""
        if node.connection_string:
            # Try to extract database/schema from connection string
            conn_lower = node.connection_string.lower()
            if 'database=' in conn_lower:
                match = re.search(r'database=([^;]+)', conn_lower)
                if match:
                    return self._sanitize_name(match.group(1))

        # Default schema
        return "raw"

    def _get_table_name(self, node: AlteryxNode) -> str:
        """Determine table name from source node."""
        if node.table_name:
            return self._sanitize_name(node.table_name)

        if node.source_path:
            return self._sanitize_name(Path(node.source_path).stem)

        return f"source_{node.tool_id}"

    def _generate_sources_yml(self) -> None:
        """Generate sources.yml file."""
        content = [
            "version: 2",
            "",
            "sources:",
        ]

        for schema, tables in sorted(self.sources.items()):
            content.extend([
                f"  - name: {schema}",
                f"    description: \"Source data from {schema}\"",
                "    tables:",
            ])

            for table in sorted(tables):
                content.extend([
                    f"      - name: {table}",
                    f"        description: \"Source table {table}\"",
                ])

        self._write_file(
            self.output_dir / "models" / "staging" / "_sources.yml",
            "\n".join(content)
        )

    def _generate_workflow_models(self, workflow: AlteryxWorkflow) -> None:
        """Generate DBT models for a single workflow."""
        analyzer = TransformationAnalyzer(workflow)
        medallion = analyzer.suggest_medallion_mapping()

        workflow_prefix = self._sanitize_name(workflow.metadata.name)

        # Generate staging models (Bronze)
        bronze_nodes = medallion.get(MedallionLayer.BRONZE.value, [])
        for node in bronze_nodes:
            if node.category == ToolCategory.INPUT:
                self._generate_staging_model(node, workflow_prefix)

        # Generate intermediate models (Silver)
        silver_nodes = medallion.get(MedallionLayer.SILVER.value, [])
        for node in silver_nodes:
            self._generate_intermediate_model(node, workflow_prefix, workflow)

        # Generate mart models (Gold)
        gold_nodes = medallion.get(MedallionLayer.GOLD.value, [])
        for node in gold_nodes:
            self._generate_mart_model(node, workflow_prefix, workflow)

    def _generate_staging_model(self, node: AlteryxNode, workflow_prefix: str) -> None:
        """Generate a staging (bronze) model."""
        schema = self._get_schema_name(node)
        table = self._get_table_name(node)
        model_name = f"stg_{workflow_prefix}_{table}"

        content = [
            f"-- Staging model for {node.get_display_name()}",
            f"-- Source: {node.source_path or node.table_name or 'Unknown'}",
            f"-- Generated from Alteryx workflow tool #{node.tool_id}",
            "",
            "{{",
            "    config(",
            "        materialized='view'",
            "    )",
            "}}",
            "",
            "with source as (",
            "",
            f"    select * from {{{{ source('{schema}', '{table}') }}}}",
            "",
            "),",
            "",
            "renamed as (",
            "",
            "    select",
            "        -- Add column selections and renaming here",
            "        *",
            "    from source",
            "",
            ")",
            "",
            "select * from renamed",
        ]

        self._write_file(
            self.output_dir / "models" / "staging" / f"{model_name}.sql",
            "\n".join(content)
        )
        self.models_generated.append(model_name)

    def _generate_intermediate_model(self, node: AlteryxNode,
                                      workflow_prefix: str,
                                      workflow: AlteryxWorkflow) -> None:
        """Generate an intermediate (silver) model."""
        model_name = f"int_{workflow_prefix}_{self._sanitize_name(node.get_display_name())}"

        # Get upstream dependencies
        upstream = workflow.get_upstream_nodes(node.tool_id)

        content = [
            f"-- Intermediate model: {node.get_display_name()}",
            f"-- Tool type: {node.plugin_name}",
            f"-- Generated from Alteryx workflow tool #{node.tool_id}",
            "",
            "{{",
            "    config(",
            "        materialized='view'",
            "    )",
            "}}",
            "",
        ]

        # Generate CTEs for upstream dependencies
        if upstream:
            for i, up_node in enumerate(upstream):
                up_model = self._get_model_reference(up_node, workflow_prefix)
                cte_name = f"source_{i + 1}" if len(upstream) > 1 else "source"
                content.extend([
                    f"with {cte_name} as (",
                    "",
                    f"    select * from {{{{ ref('{up_model}') }}}}",
                    "",
                    ")," if i < len(upstream) - 1 else "),",
                    "",
                ])

        # Generate transformation logic based on tool type
        sql = self._generate_transformation_sql(node, upstream)
        content.append(sql)

        self._write_file(
            self.output_dir / "models" / "intermediate" / f"{model_name}.sql",
            "\n".join(content)
        )
        self.models_generated.append(model_name)

    def _generate_mart_model(self, node: AlteryxNode,
                             workflow_prefix: str,
                             workflow: AlteryxWorkflow) -> None:
        """Generate a mart (gold) model."""
        # Determine if it's a fact or dimension
        if node.plugin_name == "Summarize" or node.aggregations:
            prefix = "fct"
            subdir = "core"
        else:
            prefix = "dim"
            subdir = "dimensions"

        model_name = f"{prefix}_{workflow_prefix}_{self._sanitize_name(node.get_display_name())}"

        # Get upstream dependencies
        upstream = workflow.get_upstream_nodes(node.tool_id)

        content = [
            f"-- Mart model: {node.get_display_name()}",
            f"-- Tool type: {node.plugin_name}",
            f"-- Generated from Alteryx workflow tool #{node.tool_id}",
            "",
            "{{",
            "    config(",
            "        materialized='table'",
            "    )",
            "}}",
            "",
        ]

        # Generate CTEs
        if upstream:
            for i, up_node in enumerate(upstream):
                up_model = self._get_model_reference(up_node, workflow_prefix)
                cte_name = f"source_{i + 1}" if len(upstream) > 1 else "source"
                content.extend([
                    f"with {cte_name} as (",
                    "",
                    f"    select * from {{{{ ref('{up_model}') }}}}",
                    "",
                    ")," if i < len(upstream) - 1 else "),",
                    "",
                ])

        # Generate transformation logic
        sql = self._generate_transformation_sql(node, upstream)
        content.append(sql)

        self._write_file(
            self.output_dir / "models" / "marts" / subdir / f"{model_name}.sql",
            "\n".join(content)
        )
        self.models_generated.append(model_name)

    def _get_model_reference(self, node: AlteryxNode, workflow_prefix: str) -> str:
        """Get the model name to reference for a node."""
        if node.category == ToolCategory.INPUT:
            table = self._get_table_name(node)
            return f"stg_{workflow_prefix}_{table}"
        else:
            return f"int_{workflow_prefix}_{self._sanitize_name(node.get_display_name())}"

    def _generate_transformation_sql(self, node: AlteryxNode,
                                      upstream: List[AlteryxNode]) -> str:
        """Generate SQL for a transformation node."""
        source_cte = "source" if len(upstream) <= 1 else "source_1"

        if node.plugin_name == "Filter":
            condition = self._convert_expression(node.expression or "1=1")
            return f"""final as (

    select *
    from {source_cte}
    where {condition}

)

select * from final"""

        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            formulas = node.configuration.get('formulas', [])
            if formulas:
                select_parts = ["    *"]
                for f in formulas:
                    field = f.get('field', 'new_field')
                    expr = self._convert_expression(f.get('expression', 'NULL'))
                    select_parts.append(f"    , {expr} as {field}")

                return f"""final as (

    select
{chr(10).join(select_parts)}
    from {source_cte}

)

select * from final"""
            else:
                return f"select * from {source_cte}"

        elif node.plugin_name == "Join":
            join_type = node.join_type or "LEFT"
            conditions = []
            for key in node.join_keys:
                parts = key.split('=')
                if len(parts) == 2:
                    conditions.append(f"source_1.{parts[0].strip()} = source_2.{parts[1].strip()}")

            join_condition = " and ".join(conditions) if conditions else "1=1"

            return f"""final as (

    select
        source_1.*
        -- Add columns from source_2 as needed
    from source_1
    {join_type.lower()} join source_2
        on {join_condition}

)

select * from final"""

        elif node.plugin_name == "Summarize":
            group_cols = ", ".join(node.group_by_fields) if node.group_by_fields else "1"
            agg_parts = []

            for agg in node.aggregations:
                action = agg.get('action', 'COUNT')
                field = agg.get('field', '*')
                output = agg.get('output_name', field)
                sql_func = AGGREGATION_MAP.get(action, action.upper())

                if sql_func.endswith('(DISTINCT'):
                    agg_parts.append(f"{sql_func} {field}) as {output}")
                else:
                    agg_parts.append(f"{sql_func}({field}) as {output}")

            select_clause = ", ".join(node.group_by_fields) if node.group_by_fields else ""
            if select_clause and agg_parts:
                select_clause += ",\n        "

            agg_clause = ",\n        ".join(agg_parts) if agg_parts else "count(*) as record_count"

            return f"""final as (

    select
        {select_clause}{agg_clause}
    from {source_cte}
    group by {group_cols}

)

select * from final"""

        elif node.plugin_name == "Union":
            if len(upstream) > 1:
                union_parts = []
                for i in range(len(upstream)):
                    union_parts.append(f"select * from source_{i + 1}")
                return "\n\nunion all\n\n".join(union_parts)
            return f"select * from {source_cte}"

        elif node.plugin_name == "Select":
            if node.selected_fields:
                fields = ",\n        ".join(node.selected_fields[:20])
                return f"""final as (

    select
        {fields}
    from {source_cte}

)

select * from final"""

        elif node.plugin_name == "Sort":
            sort_fields = node.configuration.get('sort_fields', [])
            if sort_fields:
                order_parts = []
                for sf in sort_fields:
                    direction = "asc" if sf.get('order', 'Ascending') == 'Ascending' else "desc"
                    order_parts.append(f"{sf['field']} {direction}")
                order_clause = ", ".join(order_parts)
                return f"""final as (

    select *
    from {source_cte}
    order by {order_clause}

)

select * from final"""

        # Default
        return f"""final as (

    select
        -- TODO: Implement {node.plugin_name} transformation
        *
    from {source_cte}

)

select * from final"""

    def _convert_expression(self, expr: str) -> str:
        """Convert Alteryx expression to SQL."""
        if not expr:
            return "NULL"

        sql = expr

        # Replace field references
        sql = re.sub(r'\[([^\]]+)\]', r'\1', sql)

        # Basic function replacements
        replacements = {
            'IsNull(': 'is null -- ',
            'IsEmpty(': "= '' -- ",
            'IIF(': 'case when ',
            ', True, False)': ' then true else false end',
            'ENDIF': 'end',
            '==': '=',
            '&&': 'and',
            '||': 'or',
        }

        for old, new in replacements.items():
            sql = sql.replace(old, new)

        return sql

    def _generate_schema_yml(self) -> None:
        """Generate schema.yml with model documentation."""
        content = [
            "version: 2",
            "",
            "models:",
        ]

        for model_name in sorted(self.models_generated):
            content.extend([
                f"  - name: {model_name}",
                f"    description: \"Model migrated from Alteryx workflow\"",
                "    columns:",
                "      - name: _placeholder",
                "        description: \"Add column descriptions\"",
                "",
            ])

        self._write_file(
            self.output_dir / "models" / "_schema.yml",
            "\n".join(content)
        )

    def _generate_project_yml(self) -> None:
        """Generate dbt_project.yml for Starburst/Trino."""
        content = f"""
name: '{self.project_name}'
version: '1.0.0'
config-version: 2

# ============================================================
# TARGET PLATFORM: Starburst (Trino-based)
# ============================================================
# This dbt project is configured for Starburst/Trino.
# Ensure you have dbt-trino adapter installed:
#   pip install dbt-trino
# ============================================================

profile: '{self.project_name}'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  {self.project_name}:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
    marts:
      core:
        +materialized: table
        +schema: marts
      dimensions:
        +materialized: table
        +schema: marts

# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Migrated from Alteryx ETL workflows to Starburst/Trino ELT.
# Review and customize the models before running.
"""

        self._write_file(self.output_dir / "dbt_project.yml", content.strip())

        # Also generate a profiles.yml template
        profiles_content = f"""
# ============================================================
# Starburst/Trino dbt Profile Configuration
# ============================================================
# Copy this file to ~/.dbt/profiles.yml and configure your connection.
# Documentation: https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup
# ============================================================

{self.project_name}:
  target: dev
  outputs:
    dev:
      type: trino
      method: ldap  # or 'none', 'kerberos', 'oauth', 'jwt', 'certificate'
      host: your-starburst-host.company.com
      port: 443
      user: your_username
      password: your_password  # Or use environment variable
      catalog: your_catalog
      schema: your_schema
      http_scheme: https
      threads: 4

    prod:
      type: trino
      method: ldap
      host: your-starburst-host.company.com
      port: 443
      user: "{{{{ env_var('DBT_USER') }}}}"
      password: "{{{{ env_var('DBT_PASSWORD') }}}}"
      catalog: your_catalog
      schema: your_schema
      http_scheme: https
      threads: 8

# Notes for Starburst Galaxy users:
# - Use method: 'oauth' or 'jwt' for authentication
# - Host format: your-cluster.galaxy.starburst.io
# - See: https://docs.starburst.io/starburst-galaxy/
"""

        self._write_file(self.output_dir / "profiles.yml.template", profiles_content.strip())

    def _sanitize_name(self, name: str) -> str:
        """Sanitize a name for use in DBT."""
        if not name:
            return "unknown"

        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        sanitized = re.sub(r'_+', '_', sanitized)
        sanitized = sanitized.strip('_').lower()

        if len(sanitized) > 50:
            sanitized = sanitized[:50]

        return sanitized or "unknown"

    def _write_file(self, path: Path, content: str) -> None:
        """Write content to a file."""
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
