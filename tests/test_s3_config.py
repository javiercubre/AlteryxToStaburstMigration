#!/usr/bin/env python3
"""
Tests for S3 configuration module.
"""
import json
import tempfile
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import S3SourceConfig, S3SourceMapping
from s3_config import S3ConfigResolver


def test_s3_source_config_basic():
    """Test basic S3SourceConfig creation."""
    config = S3SourceConfig(
        bucket="my-bucket",
        prefix="data/customers/"
    )
    assert config.bucket == "my-bucket"
    assert config.prefix == "data/customers/"
    assert config.file_format == "parquet"  # default
    assert config.s3_user is None  # default
    assert config.s3_password is None  # default
    print("[PASS] S3SourceConfig basic creation")


def test_s3_source_config_uri():
    """Test S3 URI generation."""
    config = S3SourceConfig(
        bucket="my-bucket",
        prefix="data/customers/"
    )
    assert config.get_s3_uri() == "s3a://my-bucket/data/customers"
    assert config.get_s3_url() == "s3://my-bucket/data/customers"
    print("[PASS] S3SourceConfig URI generation")


def test_s3_source_config_from_uri():
    """Test creating config from S3 URI."""
    # s3:// format
    config1 = S3SourceConfig.from_s3_uri("s3://my-bucket/data/customers/")
    assert config1.bucket == "my-bucket"
    assert config1.prefix == "data/customers/"

    # s3a:// format (Trino style)
    config2 = S3SourceConfig.from_s3_uri("s3a://another-bucket/bronze/sales")
    assert config2.bucket == "another-bucket"
    assert config2.prefix == "bronze/sales"

    # Plain format (no protocol)
    config3 = S3SourceConfig.from_s3_uri("plain-bucket/some/path")
    assert config3.bucket == "plain-bucket"
    assert config3.prefix == "some/path"

    print("[PASS] S3SourceConfig from URI")


def test_s3_source_config_serialization():
    """Test JSON serialization/deserialization."""
    config = S3SourceConfig(
        bucket="my-bucket",
        prefix="data/",
        endpoint="http://minio.local:9000",
        file_format="csv",
        file_pattern="*.csv",
        s3_user="test-user",
        s3_password="test-password"
    )

    # To dict
    data = config.to_dict()
    assert data['bucket'] == "my-bucket"
    assert data['endpoint'] == "http://minio.local:9000"
    assert data['s3_user'] == "test-user"
    assert data['s3_password'] == "test-password"

    # From dict
    restored = S3SourceConfig.from_dict(data)
    assert restored.bucket == config.bucket
    assert restored.endpoint == config.endpoint
    assert restored.file_format == config.file_format
    assert restored.s3_user == config.s3_user
    assert restored.s3_password == config.s3_password

    print("[PASS] S3SourceConfig serialization")


def test_s3_source_mapping():
    """Test S3SourceMapping creation and serialization."""
    config = S3SourceConfig(bucket="data-lake", prefix="bronze/orders/")
    mapping = S3SourceMapping(
        alteryx_source_name="orders.csv",
        s3_config=config,
        table_name="orders",
        columns=["order_id", "customer_id", "amount"]
    )

    assert mapping.alteryx_source_name == "orders.csv"
    assert mapping.table_name == "orders"
    assert len(mapping.columns) == 3

    # Serialization
    data = mapping.to_dict()
    restored = S3SourceMapping.from_dict(data)
    assert restored.alteryx_source_name == mapping.alteryx_source_name
    assert restored.s3_config.bucket == config.bucket
    assert restored.columns == mapping.columns

    print("[PASS] S3SourceMapping")


def test_s3_config_resolver_defaults():
    """Test S3ConfigResolver with defaults."""
    resolver = S3ConfigResolver(
        default_bucket="my-data-lake",
        default_s3_user="test-user",
        default_s3_password="test-password",
        interactive=False,
        skip_all=True
    )

    assert resolver.default_bucket == "my-data-lake"
    assert resolver.default_s3_user == "test-user"
    assert resolver.default_s3_password == "test-password"
    assert resolver.interactive is False

    print("[PASS] S3ConfigResolver defaults")


def test_s3_config_resolver_derive_table_name():
    """Test table name derivation."""
    resolver = S3ConfigResolver(interactive=False)

    # Basic filename
    assert resolver._derive_table_name("customers.csv") == "customers"

    # With path
    assert resolver._derive_table_name("/path/to/orders.parquet") == "orders"

    # With special characters
    assert resolver._derive_table_name("My-Data File (1).xlsx") == "my_data_file_1"

    # Starting with number
    assert resolver._derive_table_name("2023_sales.csv") == "tbl_2023_sales"

    print("[PASS] S3ConfigResolver derive table name")


def test_s3_config_resolver_load_from_file():
    """Test loading config from JSON file."""
    config_data = {
        "default_bucket": "test-bucket",
        "endpoint": "http://localhost:9000",
        "s3_user": "test-user",
        "s3_password": "test-password",
        "mappings": {
            "customers.csv": {
                "bucket": "test-bucket",
                "prefix": "bronze/customers/",
                "format": "parquet"
            },
            "orders.csv": {
                "bucket": "test-bucket",
                "prefix": "bronze/orders/",
                "format": "csv",
                "s3_user": "other-user"
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(config_data, f)
        config_path = f.name

    try:
        resolver = S3ConfigResolver(interactive=False)
        resolver.load_from_file(config_path)

        assert resolver.default_bucket == "test-bucket"
        assert resolver.default_endpoint == "http://localhost:9000"
        assert resolver.default_s3_user == "test-user"
        assert resolver.default_s3_password == "test-password"
        assert len(resolver.resolved_configs) == 2
        assert "customers.csv" in resolver.resolved_configs
        assert "orders.csv" in resolver.resolved_configs

        # Check merged configs
        customers_config = resolver.resolved_configs["customers.csv"]
        assert customers_config.bucket == "test-bucket"
        assert customers_config.prefix == "bronze/customers/"
        assert customers_config.file_format == "parquet"
        assert customers_config.s3_user == "test-user"  # inherited from default

        orders_config = resolver.resolved_configs["orders.csv"]
        assert orders_config.s3_user == "other-user"  # Override

        print("[PASS] S3ConfigResolver load from file")
    finally:
        Path(config_path).unlink()


def test_s3_config_resolver_save_to_file():
    """Test saving config to JSON file."""
    resolver = S3ConfigResolver(
        default_bucket="my-bucket",
        default_s3_user="test-user",
        default_s3_password="test-password",
        interactive=False
    )

    # Add a mapping
    config = S3SourceConfig(bucket="my-bucket", prefix="bronze/users/")
    mapping = S3SourceMapping(
        alteryx_source_name="users.csv",
        s3_config=config,
        table_name="users",
        columns=["id", "name"]
    )
    resolver.source_mappings["users.csv"] = mapping

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config_path = f.name

    try:
        resolver.save_to_file(config_path)

        # Verify saved content
        with open(config_path, 'r') as f:
            saved_data = json.load(f)

        assert saved_data['default_bucket'] == "my-bucket"
        assert saved_data['s3_user'] == "test-user"
        assert saved_data['s3_password'] == "test-password"
        assert "users.csv" in saved_data['mappings']
        assert saved_data['mappings']['users.csv']['prefix'] == "bronze/users/"

        print("[PASS] S3ConfigResolver save to file")
    finally:
        Path(config_path).unlink()


def test_s3_config_resolver_non_interactive():
    """Test non-interactive source resolution."""
    resolver = S3ConfigResolver(
        default_bucket="data-lake",
        interactive=False,
        skip_all=True
    )

    # When skip_all is True, should return None without adding to todos
    result = resolver.resolve_source("unknown_source.csv")
    assert result is None, "Expected None for skipped source"
    # skip_all returns early without adding todos
    assert len(resolver.todos) == 0, f"Expected 0 todos but got {resolver.todos}"

    # With pre-loaded config, should still resolve (pre-loaded takes priority)
    resolver.resolved_configs["known_source.csv"] = S3SourceConfig(
        bucket="data-lake",
        prefix="bronze/known/"
    )
    result = resolver.resolve_source("known_source.csv")
    assert result is not None, "Expected mapping for pre-loaded source"
    assert result.s3_config.prefix == "bronze/known/"

    # Test interactive=False without skip_all (adds to todos)
    resolver2 = S3ConfigResolver(
        default_bucket="data-lake",
        interactive=False,
        skip_all=False
    )
    result2 = resolver2.resolve_source("new_source.csv")
    assert result2 is None, "Expected None for non-interactive unknown source"
    assert len(resolver2.todos) == 1, f"Expected 1 todo but got {len(resolver2.todos)}"

    print("[PASS] S3ConfigResolver non-interactive")


def test_s3_config_resolver_pattern_matching():
    """Test wildcard pattern matching."""
    resolver = S3ConfigResolver(interactive=False)

    # Test exact match
    assert resolver._matches_pattern("customers.csv", "customers.csv") is True
    assert resolver._matches_pattern("orders.csv", "customers.csv") is False

    # Test wildcard
    assert resolver._matches_pattern("anything.csv", "*") is True
    assert resolver._matches_pattern("data.parquet", "*.parquet") is True
    assert resolver._matches_pattern("data.csv", "*.parquet") is False

    # Test prefix wildcard
    assert resolver._matches_pattern("sales_2023.csv", "sales_*.csv") is True
    assert resolver._matches_pattern("orders_2023.csv", "sales_*.csv") is False

    print("[PASS] S3ConfigResolver pattern matching")


def test_s3_config_resolver_summary():
    """Test summary generation."""
    resolver = S3ConfigResolver(
        default_bucket="my-bucket",
        default_s3_user="test-user",
        interactive=False
    )

    # Add some mappings and skips
    config = S3SourceConfig(bucket="my-bucket", prefix="bronze/")
    resolver.source_mappings["source1"] = S3SourceMapping(
        alteryx_source_name="source1",
        s3_config=config,
        table_name="source1"
    )
    resolver.source_mappings["source2"] = S3SourceMapping(
        alteryx_source_name="source2",
        s3_config=config,
        table_name="source2"
    )
    resolver.skip_sources.add("skipped_source")
    resolver.todos.append({'source_name': 'pending_source'})

    summary = resolver.get_summary()
    assert summary['resolved'] == 2
    assert summary['skipped'] == 1
    assert summary['todos'] == 1
    assert summary['default_bucket'] == "my-bucket"
    assert summary['has_credentials'] is True

    print("[PASS] S3ConfigResolver summary")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("S3 Configuration Module Tests")
    print("=" * 60)

    tests = [
        test_s3_source_config_basic,
        test_s3_source_config_uri,
        test_s3_source_config_from_uri,
        test_s3_source_config_serialization,
        test_s3_source_mapping,
        test_s3_config_resolver_defaults,
        test_s3_config_resolver_derive_table_name,
        test_s3_config_resolver_load_from_file,
        test_s3_config_resolver_save_to_file,
        test_s3_config_resolver_non_interactive,
        test_s3_config_resolver_pattern_matching,
        test_s3_config_resolver_summary,
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
