#!/usr/bin/env python3
"""
Tests for Trino S3 templates module.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import S3SourceConfig, S3SourceMapping
import trino_s3_templates as s3_templates


def test_get_template_for_format():
    """Test template selection by format."""
    # Parquet (default)
    template = s3_templates.get_template_for_format('parquet')
    assert 'PARQUET' in template
    assert 'external_location' in template

    # CSV
    template = s3_templates.get_template_for_format('csv')
    assert 'CSV' in template
    assert 'skip_header_line_count' in template

    # JSON
    template = s3_templates.get_template_for_format('json')
    assert 'JSON' in template

    # Unknown defaults to parquet
    template = s3_templates.get_template_for_format('unknown')
    assert 'PARQUET' in template

    print("[PASS] get_template_for_format")


def test_generate_column_definitions():
    """Test column definition generation."""
    columns = ['id', 'name', 'email']

    # Without types (defaults to VARCHAR)
    defs = s3_templates.generate_column_definitions(columns)
    assert '"id" VARCHAR' in defs
    assert '"name" VARCHAR' in defs
    assert '"email" VARCHAR' in defs

    # With custom types
    types = {'id': 'BIGINT', 'name': 'VARCHAR(100)', 'email': 'VARCHAR(255)'}
    defs = s3_templates.generate_column_definitions(columns, types)
    assert '"id" BIGINT' in defs
    assert '"name" VARCHAR(100)' in defs

    # Empty columns
    defs = s3_templates.generate_column_definitions([])
    assert 'TODO' in defs

    print("[PASS] generate_column_definitions")


def test_generate_external_table_ddl():
    """Test external table DDL generation."""
    config = S3SourceConfig(
        bucket='test-bucket',
        prefix='data/customers/',
        file_format='parquet'
    )
    mapping = S3SourceMapping(
        alteryx_source_name='customers.csv',
        s3_config=config,
        table_name='customers',
        columns=['id', 'name', 'email']
    )

    ddl = s3_templates.generate_external_table_ddl(mapping)

    assert 'CREATE TABLE IF NOT EXISTS' in ddl
    assert 's3_raw.customers' in ddl
    assert '"id" VARCHAR' in ddl
    assert "s3a://test-bucket/data/customers" in ddl
    assert 'PARQUET' in ddl

    print("[PASS] generate_external_table_ddl")


def test_generate_external_table_ddl_csv():
    """Test CSV external table DDL generation."""
    config = S3SourceConfig(
        bucket='data-lake',
        prefix='bronze/orders/',
        file_format='csv'
    )
    mapping = S3SourceMapping(
        alteryx_source_name='orders.csv',
        s3_config=config,
        table_name='orders',
        columns=['order_id', 'customer_id', 'amount']
    )

    ddl = s3_templates.generate_external_table_ddl(mapping)

    assert 'CSV' in ddl
    assert 'skip_header_line_count' in ddl
    assert 's3a://data-lake/bronze/orders' in ddl

    print("[PASS] generate_external_table_ddl CSV")


def test_generate_schema_ddl():
    """Test schema DDL generation."""
    ddl = s3_templates.generate_schema_ddl(
        schema='s3_raw',
        s3_location='s3a://my-bucket/raw/',
        catalog='hive'
    )

    assert 'CREATE SCHEMA IF NOT EXISTS' in ddl
    assert 'hive.s3_raw' in ddl
    assert 's3a://my-bucket/raw/' in ddl

    print("[PASS] generate_schema_ddl")


def test_generate_sources_yml_entry():
    """Test DBT sources.yml generation."""
    config1 = S3SourceConfig(bucket='bucket', prefix='data/customers/')
    mapping1 = S3SourceMapping(
        alteryx_source_name='customers.csv',
        s3_config=config1,
        table_name='customers',
        columns=['id', 'name']
    )

    config2 = S3SourceConfig(bucket='bucket', prefix='data/orders/')
    mapping2 = S3SourceMapping(
        alteryx_source_name='orders.csv',
        s3_config=config2,
        table_name='orders',
        columns=['order_id', 'amount']
    )

    yml = s3_templates.generate_sources_yml_entry([mapping1, mapping2])

    assert 'name: s3_raw' in yml
    assert 'name: customers' in yml
    assert 'name: orders' in yml
    assert 'external_location:' in yml
    assert 'file_format: "parquet"' in yml
    assert 'columns:' in yml
    assert 'name: id' in yml

    print("[PASS] generate_sources_yml_entry")


def test_generate_bronze_model_sql():
    """Test bronze model SQL generation."""
    config = S3SourceConfig(
        bucket='my-bucket',
        prefix='bronze/sales/',
        file_format='parquet'
    )
    mapping = S3SourceMapping(
        alteryx_source_name='sales_data.parquet',
        s3_config=config,
        table_name='sales',
        columns=['sale_id', 'product_id', 'quantity', 'price']
    )

    sql = s3_templates.generate_bronze_model_sql(mapping, include_metadata=True)

    assert "config(" in sql
    assert "materialized='table'" in sql
    assert "source('s3_raw', 'sales')" in sql
    assert '_source_file' in sql
    assert '_source_modified_at' in sql
    assert 'validated' in sql

    print("[PASS] generate_bronze_model_sql")


def test_generate_bronze_model_sql_no_metadata():
    """Test bronze model SQL without metadata columns."""
    config = S3SourceConfig(bucket='bucket', prefix='data/')
    mapping = S3SourceMapping(
        alteryx_source_name='test.csv',
        s3_config=config,
        table_name='test',
        columns=['col1', 'col2']
    )

    sql = s3_templates.generate_bronze_model_sql(mapping, include_metadata=False)

    assert '_source_file' not in sql
    assert 'final' in sql  # Uses final CTE instead of validated

    print("[PASS] generate_bronze_model_sql no metadata")


def test_generate_setup_sql():
    """Test complete setup SQL generation."""
    config1 = S3SourceConfig(bucket='data-lake', prefix='bronze/customers/')
    mapping1 = S3SourceMapping(
        alteryx_source_name='customers.csv',
        s3_config=config1,
        table_name='customers',
        columns=['id', 'name']
    )

    config2 = S3SourceConfig(bucket='data-lake', prefix='bronze/orders/')
    mapping2 = S3SourceMapping(
        alteryx_source_name='orders.csv',
        s3_config=config2,
        table_name='orders',
        columns=['order_id', 'amount']
    )

    setup_sql = s3_templates.generate_setup_sql([mapping1, mapping2])

    assert 'CREATE SCHEMA IF NOT EXISTS' in setup_sql
    assert 'CREATE TABLE IF NOT EXISTS' in setup_sql
    assert 'customers' in setup_sql
    assert 'orders' in setup_sql
    assert 'S3 External Tables Setup Script' in setup_sql

    print("[PASS] generate_setup_sql")


def test_generate_setup_sql_empty():
    """Test setup SQL with no mappings."""
    setup_sql = s3_templates.generate_setup_sql([])
    assert 'No S3 mappings' in setup_sql

    print("[PASS] generate_setup_sql empty")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("Trino S3 Templates Tests")
    print("=" * 60)

    tests = [
        test_get_template_for_format,
        test_generate_column_definitions,
        test_generate_external_table_ddl,
        test_generate_external_table_ddl_csv,
        test_generate_schema_ddl,
        test_generate_sources_yml_entry,
        test_generate_bronze_model_sql,
        test_generate_bronze_model_sql_no_metadata,
        test_generate_setup_sql,
        test_generate_setup_sql_empty,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"[ERROR] {test.__name__}: {e}")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(run_all_tests())
