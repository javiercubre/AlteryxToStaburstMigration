"""
Trino SQL templates for S3 external tables.

Provides templates for creating Trino/Starburst external tables that read
from S3-compatible storage (AWS S3, MinIO, etc.).
"""
from typing import List, Dict, Any, Optional
from models import S3SourceConfig, S3SourceMapping


# =============================================================================
# External Table DDL Templates
# =============================================================================

EXTERNAL_TABLE_PARQUET = """
-- External table for Parquet files on S3
-- Source: {source_name}
CREATE TABLE IF NOT EXISTS {schema}.{table} (
{column_definitions}
)
WITH (
    external_location = '{s3_location}',
    format = 'PARQUET'
);
"""

EXTERNAL_TABLE_CSV = """
-- External table for CSV files on S3
-- Source: {source_name}
CREATE TABLE IF NOT EXISTS {schema}.{table} (
{column_definitions}
)
WITH (
    external_location = '{s3_location}',
    format = 'CSV',
    skip_header_line_count = 1
);
"""

EXTERNAL_TABLE_JSON = """
-- External table for JSON files on S3
-- Source: {source_name}
CREATE TABLE IF NOT EXISTS {schema}.{table} (
{column_definitions}
)
WITH (
    external_location = '{s3_location}',
    format = 'JSON'
);
"""

EXTERNAL_TABLE_ORC = """
-- External table for ORC files on S3
-- Source: {source_name}
CREATE TABLE IF NOT EXISTS {schema}.{table} (
{column_definitions}
)
WITH (
    external_location = '{s3_location}',
    format = 'ORC'
);
"""

# =============================================================================
# Schema Creation Templates
# =============================================================================

CREATE_SCHEMA_TEMPLATE = """
-- Create schema for S3 external tables
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
WITH (location = '{s3_location}');
"""

# =============================================================================
# DBT Sources YAML Templates
# =============================================================================

SOURCES_YML_S3_TEMPLATE = """  - name: {source_name}
    description: "S3 external tables for {description}"
    schema: {schema}
    tables:
{tables_yaml}
"""

TABLE_YML_S3_TEMPLATE = """      - name: {table_name}
        description: "{description}"
        meta:
          external_location: "{s3_location}"
          file_format: {file_format}
          source_type: s3
{columns_yaml}
"""

COLUMN_YML_TEMPLATE = """          - name: {column_name}
            description: "{description}"
"""

# =============================================================================
# Helper Functions
# =============================================================================

def get_template_for_format(file_format: str) -> str:
    """Get the appropriate DDL template for a file format.

    Args:
        file_format: File format (parquet, csv, json, orc)

    Returns:
        SQL template string
    """
    templates = {
        'parquet': EXTERNAL_TABLE_PARQUET,
        'csv': EXTERNAL_TABLE_CSV,
        'json': EXTERNAL_TABLE_JSON,
        'orc': EXTERNAL_TABLE_ORC,
    }
    return templates.get(file_format.lower(), EXTERNAL_TABLE_PARQUET)


def generate_column_definitions(
    columns: List[str],
    column_types: Optional[Dict[str, str]] = None
) -> str:
    """Generate column definitions for CREATE TABLE.

    Args:
        columns: List of column names
        column_types: Optional dict mapping column name to SQL type

    Returns:
        Formatted column definitions string
    """
    if not columns:
        return "    -- TODO: Add column definitions"

    lines = []
    for col in columns:
        col_type = "VARCHAR"  # Default type
        if column_types and col in column_types:
            col_type = column_types[col]

        # Quote column names for safety
        quoted_col = f'"{col}"' if not col.startswith('"') else col
        lines.append(f"    {quoted_col} {col_type}")

    return ",\n".join(lines)


def generate_external_table_ddl(
    mapping: S3SourceMapping,
    schema: str = "s3_raw",
    column_types: Optional[Dict[str, str]] = None
) -> str:
    """Generate complete DDL for an S3 external table.

    Args:
        mapping: S3SourceMapping with source configuration
        schema: Target schema name
        column_types: Optional column type mappings

    Returns:
        Complete CREATE TABLE SQL statement
    """
    template = get_template_for_format(mapping.s3_config.file_format)
    column_defs = generate_column_definitions(mapping.columns, column_types)

    return template.format(
        source_name=mapping.alteryx_source_name,
        schema=schema,
        table=mapping.table_name,
        column_definitions=column_defs,
        s3_location=mapping.s3_config.get_s3_uri(),
    )


def generate_schema_ddl(
    schema: str,
    s3_location: str,
    catalog: str = "hive"
) -> str:
    """Generate DDL to create a schema for S3 tables.

    Args:
        schema: Schema name
        s3_location: S3 location for schema
        catalog: Trino catalog (default: hive)

    Returns:
        CREATE SCHEMA SQL statement
    """
    return CREATE_SCHEMA_TEMPLATE.format(
        catalog=catalog,
        schema=schema,
        s3_location=s3_location,
    )


def generate_sources_yml_entry(
    mappings: List[S3SourceMapping],
    source_name: str = "s3_raw",
    schema: str = "s3_raw",
    description: str = "Alteryx source data migrated to S3"
) -> str:
    """Generate DBT sources.yml entry for S3 sources.

    Args:
        mappings: List of S3SourceMapping objects
        source_name: Name for the DBT source
        schema: Schema name
        description: Source description

    Returns:
        YAML string for sources.yml
    """
    if not mappings:
        return ""

    tables_yaml_parts = []
    for mapping in mappings:
        # Generate columns YAML if available
        columns_yaml = ""
        if mapping.columns:
            col_lines = []
            for col in mapping.columns:
                col_lines.append(COLUMN_YML_TEMPLATE.format(
                    column_name=col,
                    description=f"Column from {mapping.alteryx_source_name}"
                ))
            columns_yaml = "        columns:\n" + "".join(col_lines)

        table_yaml = TABLE_YML_S3_TEMPLATE.format(
            table_name=mapping.table_name,
            description=f"Data from {mapping.alteryx_source_name}",
            s3_location=mapping.s3_config.get_s3_uri(),
            file_format=mapping.s3_config.file_format,
            columns_yaml=columns_yaml,
        )
        tables_yaml_parts.append(table_yaml)

    return SOURCES_YML_S3_TEMPLATE.format(
        source_name=source_name,
        description=description,
        schema=schema,
        tables_yaml="".join(tables_yaml_parts),
    )


# =============================================================================
# Bronze Model Templates
# =============================================================================

BRONZE_MODEL_S3_TEMPLATE = """{{{{ config(
    materialized='table',
    tags=['bronze', 's3_source']
) }}}}

{comment}

with source as (
    select * from {{{{ source('{source_name}', '{table_name}') }}}}
),

{validation_cte}

select
{select_columns}
from {final_cte}
"""

VALIDATION_CTE_TEMPLATE = """-- Data quality checks for S3 ingestion
validated as (
    select
        *,
        "$path" as _source_file,
        "$file_modified_time" as _source_modified_at
    from source
    -- Filter out incomplete uploads or invalid records
    where 1=1
)"""

SIMPLE_VALIDATION_CTE = """final as (
    select * from source
)"""


def generate_bronze_model_sql(
    mapping: S3SourceMapping,
    source_name: str = "s3_raw",
    include_metadata: bool = True
) -> str:
    """Generate a bronze layer DBT model for an S3 source.

    Args:
        mapping: S3SourceMapping with source configuration
        source_name: DBT source name
        include_metadata: Include S3 metadata columns (_source_file, etc.)

    Returns:
        Complete DBT model SQL
    """
    # Generate comment
    comment = f"-- Bronze layer model for: {mapping.alteryx_source_name}"
    comment += f"\n-- S3 Location: {mapping.s3_config.get_s3_uri()}"
    comment += f"\n-- Format: {mapping.s3_config.file_format}"

    # Generate validation CTE
    if include_metadata:
        validation_cte = VALIDATION_CTE_TEMPLATE
        final_cte = "validated"
    else:
        validation_cte = SIMPLE_VALIDATION_CTE
        final_cte = "final"

    # Generate select columns
    if mapping.columns:
        select_parts = []
        for col in mapping.columns:
            quoted_col = f'"{col}"' if not col.startswith('"') else col
            select_parts.append(f"    {quoted_col}")

        if include_metadata:
            select_parts.append("    _source_file")
            select_parts.append("    _source_modified_at")

        select_columns = ",\n".join(select_parts)
    else:
        select_columns = "    *"
        if include_metadata:
            select_columns += ",\n    _source_file,\n    _source_modified_at"

    return BRONZE_MODEL_S3_TEMPLATE.format(
        comment=comment,
        source_name=source_name,
        table_name=mapping.table_name,
        validation_cte=validation_cte,
        select_columns=select_columns,
        final_cte=final_cte,
    )


# =============================================================================
# Setup SQL Generation
# =============================================================================

def generate_setup_sql(
    mappings: List[S3SourceMapping],
    catalog: str = "hive",
    schema: str = "s3_raw",
    base_location: Optional[str] = None
) -> str:
    """Generate complete setup SQL for S3 integration.

    Args:
        mappings: List of S3SourceMapping objects
        catalog: Trino catalog
        schema: Schema name
        base_location: Base S3 location for schema (derived from first mapping if not provided)

    Returns:
        Complete setup SQL script
    """
    if not mappings:
        return "-- No S3 mappings to set up"

    lines = [
        "-- =============================================================================",
        "-- S3 External Tables Setup Script",
        "-- Generated by Alteryx to DBT Migration Tool",
        "-- =============================================================================",
        "",
    ]

    # Determine base location
    if not base_location and mappings:
        first_config = mappings[0].s3_config
        base_location = f"s3a://{first_config.bucket}/"

    # Create schema
    lines.append("-- Create schema for S3 external tables")
    lines.append(generate_schema_ddl(schema, base_location, catalog))
    lines.append("")

    # Create tables
    lines.append("-- Create external tables")
    for mapping in mappings:
        lines.append(generate_external_table_ddl(mapping, f"{catalog}.{schema}"))
        lines.append("")

    return "\n".join(lines)
