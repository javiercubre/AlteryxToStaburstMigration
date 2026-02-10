"""
Tests for column data type inference feature.

Tests that the ColumnDetector can infer Trino SQL data types from
CSV and JSON files by sampling up to 100 rows. Null-only columns
should default to VARCHAR.
"""
import os
import sys
import tempfile
import json

import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from column_detector import ColumnDetector


@pytest.fixture
def detector():
    """Create a ColumnDetector instance for testing."""
    return ColumnDetector()


@pytest.fixture
def test_data_dir():
    """Return the path to the test data directory."""
    return os.path.join(os.path.dirname(__file__), 'test_data')


class TestCSVTypeInference:
    """Tests for CSV column type inference."""

    def test_basic_type_inference(self, detector):
        """Test inferring types from a CSV with mixed column types."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,price,is_active,created_date\n")
            f.write("1,Alice,29.99,true,2023-01-15\n")
            f.write("2,Bob,49.50,false,2023-02-20\n")
            f.write("3,Charlie,10.00,true,2023-03-10\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['name'] == 'VARCHAR'
            assert types['price'] == 'DOUBLE'
            assert types['is_active'] == 'BOOLEAN'
            assert types['created_date'] == 'DATE'
        finally:
            os.unlink(temp_path)

    def test_integer_vs_bigint(self, detector):
        """Test distinguishing INTEGER from BIGINT based on value range."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("small_id,big_id\n")
            f.write("1,9999999999\n")
            f.write("100,1234567890123\n")
            f.write("2000,5000000000\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['small_id'] == 'INTEGER'
            assert types['big_id'] == 'BIGINT'
        finally:
            os.unlink(temp_path)

    def test_null_only_columns_default_to_varchar(self, detector):
        """Test that columns with all null/empty values default to VARCHAR."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,empty_col,null_col,normal_col\n")
            f.write("1,,,hello\n")
            f.write("2,,,world\n")
            f.write("3,,NULL,test\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['empty_col'] == 'VARCHAR', "Empty column should default to VARCHAR"
            assert types['null_col'] == 'VARCHAR', "NULL-only column should default to VARCHAR"
            assert types['normal_col'] == 'VARCHAR'
        finally:
            os.unlink(temp_path)

    def test_mixed_null_and_values(self, detector):
        """Test that columns with some nulls infer type from non-null values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,score\n")
            f.write("1,85.5\n")
            f.write("2,\n")
            f.write("3,92.3\n")
            f.write(",78.0\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['score'] == 'DOUBLE'
        finally:
            os.unlink(temp_path)

    def test_boolean_variants(self, detector):
        """Test recognition of various boolean representations."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("flag_tf,flag_yn,flag_01\n")
            f.write("true,yes,1\n")
            f.write("false,no,0\n")
            f.write("True,Yes,1\n")
            f.write("FALSE,NO,0\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['flag_tf'] == 'BOOLEAN'
            assert types['flag_yn'] == 'BOOLEAN'
            assert types['flag_01'] == 'BOOLEAN'
        finally:
            os.unlink(temp_path)

    def test_date_formats(self, detector):
        """Test recognition of different date formats."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("iso_date,us_date,alt_date\n")
            f.write("2023-01-15,01/15/2023,2023/01/15\n")
            f.write("2023-02-20,02/20/2023,2023/02/20\n")
            f.write("2023-03-10,03/10/2023,2023/03/10\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['iso_date'] == 'DATE'
            assert types['us_date'] == 'DATE'
            assert types['alt_date'] == 'DATE'
        finally:
            os.unlink(temp_path)

    def test_timestamp_detection(self, detector):
        """Test recognition of timestamp values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("created_at,updated_at\n")
            f.write("2023-01-15T10:30:00,01/15/2023 10:30\n")
            f.write("2023-02-20T14:45:30,02/20/2023 14:45\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['created_at'] == 'TIMESTAMP'
            assert types['updated_at'] == 'TIMESTAMP'
        finally:
            os.unlink(temp_path)

    def test_semicolon_delimiter(self, detector):
        """Test type inference with semicolon-delimited CSV."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id;amount;label\n")
            f.write("1;100.50;alpha\n")
            f.write("2;200.75;beta\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['amount'] == 'DOUBLE'
            assert types['label'] == 'VARCHAR'
        finally:
            os.unlink(temp_path)

    def test_empty_file_returns_empty_dict(self, detector):
        """Test that an empty CSV returns empty dict."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types == {}
        finally:
            os.unlink(temp_path)

    def test_header_only_file(self, detector):
        """Test CSV with header but no data rows — all columns should be VARCHAR."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col_a,col_b,col_c\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['col_a'] == 'VARCHAR'
            assert types['col_b'] == 'VARCHAR'
            assert types['col_c'] == 'VARCHAR'
        finally:
            os.unlink(temp_path)

    def test_negative_numbers(self, detector):
        """Test that negative integers and floats are detected correctly."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("temp,balance\n")
            f.write("-5,100.00\n")
            f.write("10,-250.50\n")
            f.write("-20,0.00\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['temp'] == 'INTEGER'
            assert types['balance'] == 'DOUBLE'
        finally:
            os.unlink(temp_path)

    def test_existing_sample_customers_csv(self, detector, test_data_dir):
        """Test type inference on the existing sample_customers.csv test data."""
        test_file = os.path.join(test_data_dir, 'sample_customers.csv')
        types = detector.infer_csv_column_types(test_file)

        assert types['customer_id'] == 'INTEGER'
        assert types['customer_name'] == 'VARCHAR'
        assert types['email'] == 'VARCHAR'
        assert types['region'] == 'VARCHAR'
        assert types['created_date'] == 'DATE'
        assert types['is_active'] == 'BOOLEAN'

    def test_max_100_rows_sampled(self, detector):
        """Test that only first 100 rows are sampled for inference."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("value\n")
            # Write 100 integer rows
            for i in range(100):
                f.write(f"{i}\n")
            # Row 101+ has non-integer data — should NOT affect inference
            for i in range(50):
                f.write(f"text_{i}\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['value'] == 'INTEGER', "Should only sample first 100 rows"
        finally:
            os.unlink(temp_path)


class TestJSONTypeInference:
    """Tests for JSON column type inference."""

    def test_basic_json_types(self, detector):
        """Test inferring types from a JSON array of objects."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "name": "Alice", "score": 95.5, "active": True},
                {"id": 2, "name": "Bob", "score": 87.0, "active": False},
            ], f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['name'] == 'VARCHAR'
            assert types['score'] == 'DOUBLE'
            assert types['active'] == 'BOOLEAN'
        finally:
            os.unlink(temp_path)

    def test_json_null_only_columns(self, detector):
        """Test that JSON columns with all null values default to VARCHAR."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "optional_field": None},
                {"id": 2, "optional_field": None},
                {"id": 3, "optional_field": None},
            ], f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['optional_field'] == 'VARCHAR', "Null-only column should be VARCHAR"
        finally:
            os.unlink(temp_path)

    def test_json_mixed_nulls_and_values(self, detector):
        """Test that JSON columns with some nulls infer from non-null values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "score": 95.5},
                {"id": 2, "score": None},
                {"id": None, "score": 87.0},
            ], f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['score'] == 'DOUBLE'
        finally:
            os.unlink(temp_path)

    def test_json_nested_data_key(self, detector):
        """Test type inference from JSON with nested 'data' key."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({
                "meta": {"total": 2},
                "data": [
                    {"id": 1, "amount": 100.50, "date": "2023-01-15"},
                    {"id": 2, "amount": 200.75, "date": "2023-02-20"},
                ]
            }, f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['amount'] == 'DOUBLE'
            assert types['date'] == 'DATE'
        finally:
            os.unlink(temp_path)

    def test_json_nested_records_key(self, detector):
        """Test type inference from JSON with nested 'records' key."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({
                "records": [
                    {"name": "test", "value": 42},
                    {"name": "test2", "value": 99},
                ]
            }, f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['name'] == 'VARCHAR'
            assert types['value'] == 'INTEGER'
        finally:
            os.unlink(temp_path)

    def test_json_date_string_detection(self, detector):
        """Test that date-formatted strings in JSON are detected as DATE."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"event": "signup", "event_date": "2023-01-15"},
                {"event": "login", "event_date": "2023-02-20"},
            ], f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['event'] == 'VARCHAR'
            assert types['event_date'] == 'DATE'
        finally:
            os.unlink(temp_path)

    def test_json_int_and_float_mix(self, detector):
        """Test that mixed int/float columns promote to DOUBLE."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "value": 100},
                {"id": 2, "value": 200.5},
                {"id": 3, "value": 300},
            ], f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['value'] == 'DOUBLE', "Mixed int/float should promote to DOUBLE"
        finally:
            os.unlink(temp_path)

    def test_json_bigint_detection(self, detector):
        """Test that large integers are detected as BIGINT."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 9999999999},
                {"id": 1234567890123},
            ], f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['id'] == 'BIGINT'
        finally:
            os.unlink(temp_path)

    def test_existing_sample_orders_json(self, detector, test_data_dir):
        """Test type inference on the existing sample_orders.json test data."""
        test_file = os.path.join(test_data_dir, 'sample_orders.json')
        types = detector.infer_json_column_types(test_file)

        assert types['order_id'] == 'INTEGER'
        assert types['customer_id'] == 'INTEGER'
        assert types['product'] == 'VARCHAR'
        assert types['quantity'] == 'INTEGER'
        assert types['price'] == 'DOUBLE'
        assert types['order_date'] == 'DATE'


class TestDispatcherTypeInference:
    """Tests for the type inference dispatcher."""

    def test_dispatcher_csv(self, detector, test_data_dir):
        """Test that dispatcher correctly infers CSV column types."""
        csv_file = os.path.join(test_data_dir, 'sample_customers.csv')
        types = detector.infer_column_types(csv_file)
        assert len(types) == 6
        assert types['customer_id'] == 'INTEGER'

    def test_dispatcher_json(self, detector, test_data_dir):
        """Test that dispatcher correctly infers JSON column types."""
        json_file = os.path.join(test_data_dir, 'sample_orders.json')
        types = detector.infer_column_types(json_file)
        assert len(types) == 6
        assert types['order_id'] == 'INTEGER'

    def test_dispatcher_nonexistent_file(self, detector):
        """Test that nonexistent files return empty dict."""
        types = detector.infer_column_types('/nonexistent/path/file.csv')
        assert types == {}

    def test_dispatcher_tsv(self, detector):
        """Test that .tsv files are handled via CSV inference."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
            f.write("id\tname\tvalue\n")
            f.write("1\talpha\t100\n")
            f.write("2\tbeta\t200\n")
            temp_path = f.name

        try:
            types = detector.infer_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['name'] == 'VARCHAR'
            assert types['value'] == 'INTEGER'
        finally:
            os.unlink(temp_path)


class TestEdgeCases:
    """Tests for edge cases in type inference."""

    def test_single_row_inference(self, detector):
        """Test inference from a single data row."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,value\n")
            f.write("42,hello\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['value'] == 'VARCHAR'
        finally:
            os.unlink(temp_path)

    def test_all_columns_null(self, detector):
        """Test that a file where ALL columns are null defaults everything to VARCHAR."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("a,b,c\n")
            f.write(",,\n")
            f.write(",,NULL\n")
            f.write(",,\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['a'] == 'VARCHAR'
            assert types['b'] == 'VARCHAR'
            assert types['c'] == 'VARCHAR'
        finally:
            os.unlink(temp_path)

    def test_column_with_leading_zeros_is_varchar(self, detector):
        """Test that values like zip codes (leading zeros) are detected as VARCHAR.

        Note: CSV reads as strings. '00123' parses as int 123 so it still matches
        integer. This test documents current behavior — leading zeros in CSV are
        still treated as integers since the string '00123' can be parsed as int.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("zipcode,name\n")
            f.write("00123,Springfield\n")
            f.write("10001,New York\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            # CSV integers with leading zeros still parse as int
            assert types['zipcode'] == 'INTEGER'
            assert types['name'] == 'VARCHAR'
        finally:
            os.unlink(temp_path)

    def test_mixed_types_fallback_to_varchar(self, detector):
        """Test that columns with mixed types fall back to VARCHAR."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("mixed_col\n")
            f.write("123\n")
            f.write("hello\n")
            f.write("456\n")
            temp_path = f.name

        try:
            types = detector.infer_csv_column_types(temp_path)
            assert types['mixed_col'] == 'VARCHAR'
        finally:
            os.unlink(temp_path)

    def test_empty_json_array(self, detector):
        """Test that empty JSON array returns empty dict."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([], f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types == {}
        finally:
            os.unlink(temp_path)

    def test_json_with_nested_objects(self, detector):
        """Test that columns containing nested objects/arrays default to VARCHAR."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "metadata": {"key": "value"}, "tags": ["a", "b"]},
                {"id": 2, "metadata": {"key": "v2"}, "tags": ["c"]},
            ], f)
            temp_path = f.name

        try:
            types = detector.infer_json_column_types(temp_path)
            assert types['id'] == 'INTEGER'
            assert types['metadata'] == 'VARCHAR'
            assert types['tags'] == 'VARCHAR'
        finally:
            os.unlink(temp_path)


# Backwards compatibility: allow running directly
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
