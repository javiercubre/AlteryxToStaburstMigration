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


class TestDataTypeInference:
    """Tests for data_type attribute in _sources.yml generation."""

    def _make_workflow_with_source(self, source_path, table_name=None):
        """Helper to create a minimal workflow with one Input source node."""
        from models import AlteryxNode, AlteryxWorkflow, WorkflowMetadata, ToolCategory
        node = AlteryxNode(
            tool_id=1,
            tool_type="AlteryxBasePluginsGui.DbFileInput.DbFileInput",
            plugin_name="Input Data",
            category=ToolCategory.INPUT,
            source_path=source_path,
            table_name=table_name,
        )
        workflow = AlteryxWorkflow(
            metadata=WorkflowMetadata(name="test_wf", file_path="test.yxmd"),
            nodes=[node],
            connections=[],
            sources=[node],
        )
        return workflow

    def test_data_type_from_real_csv(self, test_data_dir):
        """Test that data_type attributes are added when a real CSV exists."""
        csv_path = os.path.join(test_data_dir, 'sample_customers.csv')
        workflow = self._make_workflow_with_source(csv_path)

        output_dir = tempfile.mkdtemp()
        gen = DBTGenerator(output_dir, interactive=False)
        gen._collect_sources([workflow])

        # Verify column_types were inferred for the source
        assert len(gen.sources) > 0
        for schema, tables in gen.sources.items():
            for table_name, source_info in tables.items():
                assert len(source_info.columns) > 0, "Columns should be detected from CSV"
                assert len(source_info.column_types) > 0, "Column types should be inferred from CSV"
                for col in source_info.columns:
                    assert col in source_info.column_types, (
                        f"Column '{col}' missing data_type in column_types"
                    )

    def test_data_type_written_to_sources_yml(self, test_data_dir):
        """Test that data_type attributes appear in the generated _sources.yml."""
        csv_path = os.path.join(test_data_dir, 'sample_customers.csv')
        workflow = self._make_workflow_with_source(csv_path)

        output_dir = tempfile.mkdtemp()
        gen = DBTGenerator(output_dir, interactive=False)
        gen._collect_sources([workflow])
        gen._generate_sources_yml()

        sources_yml = os.path.join(output_dir, 'models', 'bronze', '_sources.yml')
        assert os.path.exists(sources_yml), "_sources.yml should be generated"

        with open(sources_yml, 'r') as f:
            content = f.read()

        assert 'data_type:' in content, (
            "_sources.yml should contain data_type attributes when a real CSV is provided"
        )
        # Verify specific expected types from sample_customers.csv
        assert 'INTEGER' in content, "customer_id should be inferred as INTEGER"
        assert 'VARCHAR' in content, "String columns should be inferred as VARCHAR"

    def test_data_type_with_resolved_path(self, test_data_dir):
        """Test that data_type works when source uses resolved path (not original Alteryx path)."""
        csv_path = os.path.join(test_data_dir, 'sample_customers.csv')
        # Simulate an Alteryx workflow pointing to a non-existent Windows path
        workflow = self._make_workflow_with_source(
            source_path=r"C:\NonExistent\customers.csv",
            table_name="customers",
        )

        output_dir = tempfile.mkdtemp()
        gen = DBTGenerator(output_dir, interactive=False)

        # Manually resolve the source file (simulates interactive prompt resolution)
        source_node = workflow.sources[0]
        schema = gen._get_schema_name(source_node)
        table = gen._get_table_name(source_node)
        source_key = f"{schema}.{table}"
        gen._resolved_source_files[source_key] = csv_path

        # Read columns from the resolved CSV path
        columns = gen._read_file_columns(csv_path)
        assert len(columns) > 0

        # Now collect sources - type inference should use the resolved path
        gen._collect_sources([workflow])

        for s_tables in gen.sources.values():
            for source_info in s_tables.values():
                if source_info.columns:
                    assert len(source_info.column_types) > 0, (
                        "Column types should be inferred via resolved path even when "
                        "node.source_path points to a non-existent file"
                    )
                    for col in source_info.columns:
                        assert col in source_info.column_types, (
                            f"Column '{col}' missing data_type when using resolved path"
                        )

    def test_inferred_types_are_correct_for_csv(self, generator, test_data_dir):
        """Test that inferred types from the sample CSV are reasonable."""
        csv_path = os.path.join(test_data_dir, 'sample_customers.csv')
        types = generator._infer_file_column_types(csv_path)

        assert types.get('customer_id') == 'INTEGER', "customer_id should be INTEGER"
        assert types.get('customer_name') == 'VARCHAR', "customer_name should be VARCHAR"
        assert types.get('email') == 'VARCHAR', "email should be VARCHAR"
        assert types.get('region') == 'VARCHAR', "region should be VARCHAR"
        assert types.get('created_date') == 'DATE', "created_date should be DATE"
        assert types.get('is_active') == 'BOOLEAN', "is_active should be BOOLEAN"


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
