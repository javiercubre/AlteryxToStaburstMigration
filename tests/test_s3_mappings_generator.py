#!/usr/bin/env python3
"""
Tests for S3 Mappings Generator module.
"""
import json
import os
import sys
import tempfile

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from s3_mappings_generator import S3MappingsGenerator


def test_derive_table_name():
    """Test table name derivation from source names."""
    generator = S3MappingsGenerator()

    # Basic filename
    assert generator._derive_table_name("customers.csv") == "stg_customers"

    # Filename with spaces and special chars
    assert generator._derive_table_name("Customer Data (2024).xlsx") == "stg_customer_data_2024"

    # Database table format (note: Path().stem extracts "dbo" from "dbo.CustomerMaster")
    # For actual DB tables, the source name would typically be passed differently
    assert generator._derive_table_name("dbo.CustomerMaster") == "stg_dbo"

    # Full table name without dot extension treatment
    assert generator._derive_table_name("CustomerMaster") == "stg_customermaster"

    # Already prefixed names (no double prefix since stg_ check is done)
    assert generator._derive_table_name("stg_orders.parquet") == "stg_orders"

    # Numeric start (should add tbl_ prefix)
    assert generator._derive_table_name("2024_sales.csv") == "stg_tbl_2024_sales"

    print("[PASS] test_derive_table_name")


def test_parse_s3_uri():
    """Test S3 URI parsing."""
    generator = S3MappingsGenerator()

    # Standard s3:// format
    bucket, prefix = generator._parse_s3_uri("s3://my-bucket/path/to/data")
    assert bucket == "my-bucket"
    assert prefix == "path/to/data/"

    # s3a:// format (Hadoop)
    bucket, prefix = generator._parse_s3_uri("s3a://my-bucket/bronze/customers")
    assert bucket == "my-bucket"
    assert prefix == "bronze/customers/"

    # No protocol
    bucket, prefix = generator._parse_s3_uri("my-bucket/bronze/data")
    assert bucket == "my-bucket"
    assert prefix == "bronze/data/"

    # Bucket only (no prefix)
    bucket, prefix = generator._parse_s3_uri("my-bucket")
    assert bucket == "my-bucket"
    assert prefix == ""

    # With trailing slash
    bucket, prefix = generator._parse_s3_uri("s3://bucket/path/")
    assert bucket == "bucket"
    assert prefix == "path/"

    print("[PASS] test_parse_s3_uri")


def test_add_mapping_programmatic():
    """Test adding mappings programmatically."""
    generator = S3MappingsGenerator(
        default_bucket="test-bucket",
        default_region="us-west-2"
    )

    # Add a simple mapping
    generator.add_mapping_programmatic(
        source_name="customers.csv",
        bucket="data-lake",
        prefix="bronze/customers/",
        file_format="parquet"
    )

    assert "customers.csv" in generator.mappings
    assert generator.mappings["customers.csv"]["bucket"] == "data-lake"
    assert generator.mappings["customers.csv"]["prefix"] == "bronze/customers/"
    assert generator.mappings["customers.csv"]["format"] == "parquet"
    assert generator.mappings["customers.csv"]["table_name"] == "stg_customers"

    # Add mapping with explicit table name
    generator.add_mapping_programmatic(
        source_name="orders.xlsx",
        bucket="data-lake",
        prefix="bronze/orders/",
        table_name="custom_orders_table",
        columns=["order_id", "customer_id", "total"]
    )

    assert generator.mappings["orders.xlsx"]["table_name"] == "custom_orders_table"
    assert generator.mappings["orders.xlsx"]["columns"] == ["order_id", "customer_id", "total"]

    print("[PASS] test_add_mapping_programmatic")


def test_save_and_load_file():
    """Test saving and loading configuration files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "test_config.json")

        # Create and save configuration
        generator = S3MappingsGenerator(
            default_bucket="test-bucket",
            default_region="eu-west-1",
            default_format="csv",
            default_endpoint="http://minio.local:9000"
        )

        generator.add_mapping_programmatic(
            source_name="test.csv",
            bucket="test-bucket",
            prefix="bronze/test/",
            file_format="csv"
        )

        generator.save_to_file(output_path)

        # Verify file exists and has correct content
        assert os.path.exists(output_path)

        with open(output_path, 'r') as f:
            data = json.load(f)

        assert data["default_bucket"] == "test-bucket"
        assert data["default_region"] == "eu-west-1"
        assert data["default_format"] == "csv"
        assert data["endpoint"] == "http://minio.local:9000"
        assert "test.csv" in data["mappings"]

        # Load into new generator
        generator2 = S3MappingsGenerator()
        generator2.load_from_file(output_path)

        assert generator2.default_bucket == "test-bucket"
        assert generator2.default_region == "eu-west-1"
        assert generator2.default_format == "csv"
        assert generator2.default_endpoint == "http://minio.local:9000"
        assert "test.csv" in generator2.mappings

    print("[PASS] test_save_and_load_file")


def test_default_mapping_creation():
    """Test creating mappings with defaults."""
    generator = S3MappingsGenerator(
        default_bucket="my-data-lake",
        default_region="us-east-1",
        default_format="parquet"
    )

    # Simulate creating a default mapping
    source_name = "sales_data.csv"
    generator._add_default_mapping(source_name)

    assert source_name in generator.mappings
    mapping = generator.mappings[source_name]

    assert mapping["bucket"] == "my-data-lake"
    assert mapping["prefix"] == "bronze/stg_sales_data/"
    assert mapping["format"] == "parquet"
    assert mapping["region"] == "us-east-1"
    assert mapping["table_name"] == "stg_sales_data"

    print("[PASS] test_default_mapping_creation")


def test_generator_initialization():
    """Test generator initialization with various options."""
    # Default initialization
    gen1 = S3MappingsGenerator()
    assert gen1.default_region == "us-east-1"
    assert gen1.default_format == "parquet"
    assert gen1.default_bucket is None
    assert gen1.default_endpoint is None

    # Custom initialization
    gen2 = S3MappingsGenerator(
        default_bucket="custom-bucket",
        default_region="ap-southeast-1",
        default_format="json",
        default_endpoint="http://localhost:9000"
    )
    assert gen2.default_bucket == "custom-bucket"
    assert gen2.default_region == "ap-southeast-1"
    assert gen2.default_format == "json"
    assert gen2.default_endpoint == "http://localhost:9000"

    print("[PASS] test_generator_initialization")


def run_all_tests():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Running S3 Mappings Generator Tests")
    print("=" * 60 + "\n")

    test_derive_table_name()
    test_parse_s3_uri()
    test_add_mapping_programmatic()
    test_save_and_load_file()
    test_default_mapping_creation()
    test_generator_initialization()

    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60 + "\n")


if __name__ == '__main__':
    run_all_tests()
