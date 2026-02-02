"""
Tests for source column detection feature.

Tests that the DBTGenerator can read columns from various file formats
when column information is not available from the Alteryx workflow.

LOW-06 fix: Migrated to pytest framework.
"""
import os
import sys
import tempfile
import json

import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbt_generator import DBTGenerator


@pytest.fixture
def generator():
    """Create a DBTGenerator instance for testing."""
    return DBTGenerator(tempfile.mkdtemp(), interactive=False)


@pytest.fixture
def test_data_dir():
    """Return the path to the test data directory."""
    return os.path.join(os.path.dirname(__file__), 'test_data')


class TestCSVColumnReading:
    """Tests for CSV column reading functionality."""

    def test_csv_column_reading(self, generator, test_data_dir):
        """Test reading columns from a CSV file."""
        test_file = os.path.join(test_data_dir, 'sample_customers.csv')
        columns = generator._read_csv_columns(test_file)

        expected = ['customer_id', 'customer_name', 'email', 'region', 'created_date', 'is_active']
        assert columns == expected

    def test_csv_with_semicolon_delimiter(self, generator):
        """Test CSV reading with semicolon delimiter."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col_a;col_b;col_c\n")
            f.write("1;2;3\n")
            temp_path = f.name

        try:
            columns = generator._read_csv_columns(temp_path)
            expected = ['col_a', 'col_b', 'col_c']
            assert columns == expected
        finally:
            os.unlink(temp_path)


class TestJSONColumnReading:
    """Tests for JSON column reading functionality."""

    def test_json_column_reading(self, generator, test_data_dir):
        """Test reading columns from a JSON file."""
        test_file = os.path.join(test_data_dir, 'sample_orders.json')
        columns = generator._read_json_columns(test_file)

        expected = ['order_id', 'customer_id', 'product', 'quantity', 'price', 'order_date']
        assert columns == expected

    def test_json_nested_data_structure(self, generator):
        """Test reading columns from JSON with nested 'data' key."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({
                "meta": {"total": 2},
                "data": [
                    {"id": 1, "name": "Test", "value": 100},
                    {"id": 2, "name": "Test2", "value": 200}
                ]
            }, f)
            temp_path = f.name

        try:
            columns = generator._read_json_columns(temp_path)
            expected = ['id', 'name', 'value']
            assert columns == expected
        finally:
            os.unlink(temp_path)


class TestFileColumnDispatcher:
    """Tests for the file type dispatcher."""

    def test_dispatcher_csv(self, generator, test_data_dir):
        """Test that dispatcher handles CSV files correctly."""
        csv_file = os.path.join(test_data_dir, 'sample_customers.csv')
        csv_cols = generator._read_file_columns(csv_file)
        assert len(csv_cols) == 6

    def test_dispatcher_json(self, generator, test_data_dir):
        """Test that dispatcher handles JSON files correctly."""
        json_file = os.path.join(test_data_dir, 'sample_orders.json')
        json_cols = generator._read_file_columns(json_file)
        assert len(json_cols) == 6


class TestErrorHandling:
    """Tests for error handling in column reading."""

    def test_nonexistent_file(self, generator):
        """Test that nonexistent files return empty list."""
        columns = generator._read_file_columns('/nonexistent/path/file.csv')
        assert columns == []

    def test_empty_file(self, generator):
        """Test that empty files are handled gracefully."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name

        try:
            columns = generator._read_csv_columns(temp_path)
            assert columns == []
        finally:
            os.unlink(temp_path)


# Backwards compatibility: allow running directly
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
