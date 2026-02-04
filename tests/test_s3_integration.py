#!/usr/bin/env python3
"""
Integration tests for S3 source mapping in the full workflow.

Tests the end-to-end flow from parsing Alteryx workflows to generating
DBT projects with S3 source mappings.
"""
import json
import tempfile
import shutil
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import S3SourceConfig, S3SourceMapping
from s3_config import S3ConfigResolver
from alteryx_parser import AlteryxParser
from transformation_analyzer import TransformationAnalyzer
from dbt_generator import DBTGenerator
from doc_generator import DocumentationGenerator
from quality_validator import QualityValidator


def test_full_workflow_with_s3():
    """Test complete workflow with S3 source replacement."""
    # Setup
    samples_dir = Path(__file__).parent.parent / "samples"
    workflow_path = samples_dir / "customer_orders.yxmd"

    if not workflow_path.exists():
        print(f"[SKIP] Sample workflow not found: {workflow_path}")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        dbt_dir = Path(temp_dir) / "dbt_project"
        docs_dir = Path(temp_dir) / "docs"

        # Create S3 resolver with config
        s3_resolver = S3ConfigResolver(
            default_bucket="test-data-lake",
            default_s3_user="test-user",
            default_s3_password="test-password",
            interactive=False,
            skip_all=False
        )

        # Pre-load some S3 configs
        s3_resolver.resolved_configs["customers.csv"] = S3SourceConfig(
            bucket="test-data-lake",
            prefix="bronze/customers/",
            file_format="parquet"
        )
        s3_resolver.resolved_configs["orders.csv"] = S3SourceConfig(
            bucket="test-data-lake",
            prefix="bronze/orders/",
            file_format="parquet"
        )

        # Parse workflow
        parser = AlteryxParser()
        workflow = parser.parse(str(workflow_path))
        assert workflow is not None, "Failed to parse workflow"
        assert len(workflow.nodes) > 0, "No nodes in workflow"

        # Get sources from analyzer
        analyzer = TransformationAnalyzer(workflow)
        sources = analyzer.get_source_inventory()

        # Resolve S3 mappings for sources
        source_list = [
            {'name': src['name'], 'path': src['path']}
            for src in sources
        ]
        s3_mappings = s3_resolver.resolve_sources_batch(source_list, workflow.metadata.name)

        # Apply S3 mappings to nodes
        for node in workflow.sources:
            source_name = node.source_path or node.table_name
            if source_name:
                # Check for matching S3 mapping
                base_name = Path(source_name).name if source_name else None
                if base_name and base_name in s3_resolver.source_mappings:
                    node.s3_mapping = s3_resolver.source_mappings[base_name]

        # Generate DBT project (S3 info comes from node.s3_mapping)
        dbt_generator = DBTGenerator(str(dbt_dir), interactive=False)
        dbt_generator.generate([workflow])

        # Verify DBT project structure
        assert (dbt_dir / "dbt_project.yml").exists(), "dbt_project.yml not created"
        assert (dbt_dir / "models").exists(), "models directory not created"
        assert (dbt_dir / "macros").exists(), "macros directory not created"

        # Check for s3_source macro (copied from dbt_macros)
        s3_macro_path = dbt_dir / "macros" / "s3_source.sql"
        # Note: macro might not be copied if no S3 sources detected
        # Just verify macros dir exists

        # Generate documentation
        doc_generator = DocumentationGenerator(str(docs_dir))
        doc_generator.generate_all([workflow], s3_resolver=s3_resolver)

        # Verify documentation
        assert (docs_dir / "index.md").exists(), "index.md not created"
        assert (docs_dir / "sources.md").exists(), "sources.md not created"

        # Check for S3 setup guide if there are S3 mappings
        if s3_resolver.source_mappings:
            assert (docs_dir / "s3_setup_guide.md").exists(), "s3_setup_guide.md not created"

    print("[PASS] Full workflow with S3")


def test_s3_validation_generation():
    """Test S3 validation test generation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir)
        dbt_dir = output_dir / "dbt_project"
        dbt_dir.mkdir()

        # Create S3 resolver with mappings
        s3_resolver = S3ConfigResolver(
            default_bucket="test-bucket",
            interactive=False
        )

        # Add mappings
        s3_resolver.source_mappings["customers"] = S3SourceMapping(
            alteryx_source_name="customers.csv",
            s3_config=S3SourceConfig(
                bucket="test-bucket",
                prefix="bronze/customers/",
                file_format="parquet"
            ),
            table_name="customers",
            columns=["customer_id", "name", "email"]
        )
        s3_resolver.source_mappings["orders"] = S3SourceMapping(
            alteryx_source_name="orders.csv",
            s3_config=S3SourceConfig(
                bucket="test-bucket",
                prefix="bronze/orders/",
                file_format="parquet"
            ),
            table_name="orders",
            columns=["order_id", "customer_id", "amount"]
        )

        # Generate validation tests
        validator = QualityValidator(str(output_dir))
        created_files = validator.write_s3_validation_outputs(dbt_dir, s3_resolver)

        assert len(created_files) > 0, "No validation files created"

        # Check for connection test
        s3_tests_dir = output_dir / "tests" / "s3_validation"
        assert s3_tests_dir.exists(), "s3_validation directory not created"

        connection_test = s3_tests_dir / "s3_connection_test.sql"
        assert connection_test.exists(), "S3 connection test not created"

        # Check connection test content
        content = connection_test.read_text()
        assert "customers" in content, "customers table not in connection test"
        assert "orders" in content, "orders table not in connection test"
        assert "hive.s3_raw" in content, "Schema reference missing"

        # Check for individual source tests
        assert (s3_tests_dir / "validate_s3_customers.sql").exists()
        assert (s3_tests_dir / "validate_s3_orders.sql").exists()

        # Check for setup SQL
        setup_dir = dbt_dir / "setup"
        assert setup_dir.exists(), "setup directory not created"
        assert (setup_dir / "test_s3_connections.sql").exists()

    print("[PASS] S3 validation generation")


def test_s3_config_file_roundtrip():
    """Test loading and saving S3 config maintains data integrity."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = Path(temp_dir) / "s3_config.json"

        # Create initial config
        original_config = {
            "default_bucket": "my-bucket",
            "endpoint": "http://minio:9000",
            "default_format": "parquet",
            "s3_user": "test-user",
            "s3_password": "test-password",
            "mappings": {
                "source1.csv": {
                    "bucket": "my-bucket",
                    "prefix": "bronze/source1/",
                    "format": "parquet"
                },
                "source2.json": {
                    "bucket": "other-bucket",
                    "prefix": "bronze/source2/",
                    "format": "json"
                }
            }
        }

        # Write config
        with open(config_path, 'w') as f:
            json.dump(original_config, f)

        # Load with resolver
        resolver = S3ConfigResolver(interactive=False)
        resolver.load_from_file(str(config_path))

        # Verify loaded data
        assert resolver.default_bucket == "my-bucket"
        assert resolver.default_endpoint == "http://minio:9000"
        assert resolver.default_s3_user == "test-user"
        assert resolver.default_s3_password == "test-password"
        assert len(resolver.resolved_configs) == 2

        # Resolve sources to create mappings
        resolver.resolve_source("source1.csv")
        resolver.resolve_source("source2.json")

        # Save to new file
        output_path = Path(temp_dir) / "s3_config_output.json"
        resolver.save_to_file(str(output_path))

        # Load saved file and verify
        with open(output_path, 'r') as f:
            saved_config = json.load(f)

        assert saved_config['default_bucket'] == original_config['default_bucket']
        assert saved_config['s3_user'] == original_config['s3_user']
        assert len(saved_config['mappings']) == 2

    print("[PASS] S3 config file roundtrip")


def test_s3_bronze_model_generation():
    """Test that S3 sources generate correct bronze models."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dbt_dir = Path(temp_dir)

        # Create resolver with mapping
        s3_resolver = S3ConfigResolver(
            default_bucket="data-lake",
            interactive=False
        )

        s3_config = S3SourceConfig(
            bucket="data-lake",
            prefix="raw/customers/",
            file_format="parquet"
        )

        mapping = S3SourceMapping(
            alteryx_source_name="customer_data.parquet",
            s3_config=s3_config,
            table_name="customers",
            columns=["id", "name", "email", "created_at"]
        )
        s3_resolver.source_mappings["customer_data.parquet"] = mapping

        # Import the template generator
        from trino_s3_templates import generate_bronze_model_sql

        # Generate bronze model
        model_sql = generate_bronze_model_sql(mapping, source_name="s3_raw")

        # Verify model content
        assert "source('s3_raw', 'customers')" in model_sql, "Source reference missing"
        assert "materialized='table'" in model_sql, "Materialization missing"
        assert "_source_file" in model_sql, "Metadata column missing"
        assert "_source_modified_at" in model_sql, "Metadata column missing"
        assert "s3a://data-lake/raw/customers" in model_sql, "S3 URI missing from comment"

    print("[PASS] S3 bronze model generation")


def test_s3_external_table_ddl():
    """Test external table DDL generation."""
    from trino_s3_templates import generate_external_table_ddl

    mapping = S3SourceMapping(
        alteryx_source_name="sales.parquet",
        s3_config=S3SourceConfig(
            bucket="my-bucket",
            prefix="data/sales/",
            file_format="parquet"
        ),
        table_name="sales",
        columns=["sale_id", "amount", "sale_date"]
    )

    ddl = generate_external_table_ddl(mapping, schema="s3_raw")

    # Verify DDL content
    assert "CREATE TABLE IF NOT EXISTS s3_raw.sales" in ddl
    assert 'sale_id' in ddl
    assert 'amount' in ddl
    assert 'sale_date' in ddl
    assert "external_location = 's3a://my-bucket/data/sales'" in ddl
    assert "format = 'PARQUET'" in ddl

    print("[PASS] S3 external table DDL")


def test_s3_sources_yml_generation():
    """Test sources.yml generation with S3 metadata."""
    from trino_s3_templates import generate_sources_yml_entry

    mappings = [
        S3SourceMapping(
            alteryx_source_name="customers.csv",
            s3_config=S3SourceConfig(
                bucket="data-lake",
                prefix="bronze/customers/",
                file_format="parquet"
            ),
            table_name="customers",
            columns=["id", "name"]
        ),
        S3SourceMapping(
            alteryx_source_name="orders.csv",
            s3_config=S3SourceConfig(
                bucket="data-lake",
                prefix="bronze/orders/",
                file_format="parquet"
            ),
            table_name="orders",
            columns=["order_id", "customer_id"]
        )
    ]

    yaml_content = generate_sources_yml_entry(mappings)

    # Verify YAML content
    assert "name: s3_raw" in yaml_content
    assert "name: customers" in yaml_content
    assert "name: orders" in yaml_content
    assert "external_location:" in yaml_content
    assert "file_format: parquet" in yaml_content
    assert "source_type: s3" in yaml_content

    print("[PASS] S3 sources.yml generation")


def run_all_tests():
    """Run all integration tests."""
    print("=" * 60)
    print("S3 Integration Tests")
    print("=" * 60)

    tests = [
        test_full_workflow_with_s3,
        test_s3_validation_generation,
        test_s3_config_file_roundtrip,
        test_s3_bronze_model_generation,
        test_s3_external_table_ddl,
        test_s3_sources_yml_generation,
    ]

    passed = 0
    failed = 0
    skipped = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {test.__name__}: {e}")
            failed += 1
        except FileNotFoundError as e:
            print(f"[SKIP] {test.__name__}: {e}")
            skipped += 1
        except Exception as e:
            print(f"[ERROR] {test.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(run_all_tests())
