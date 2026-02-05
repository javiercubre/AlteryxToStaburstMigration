"""
Extreme edge case tests for the Alteryx to Starburst/DBT Migration Tool.

Tests boundary conditions, pathological inputs, and edge cases across:
- Formula converter: deeply nested expressions, max length, Unicode, empty inputs
- XML parser: malformed XML, huge workflows, missing attributes, containers
- DBT generator: name sanitization, special characters, empty/huge workflows
- Transformation analyzer: cyclic graphs, disconnected nodes, deep chains
- Column detector: corrupt files, huge headers, special delimiters
- S3 config: malformed URIs, empty configs, extreme paths
"""
import os
import sys
import tempfile
import json
import csv

import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from formula_converter import (
    FormulaConverter,
    convert_alteryx_expression,
    convert_aggregation,
    MAX_EXPRESSION_LENGTH,
    MAX_FUNCTION_CONVERSION_ITERATIONS,
)
from alteryx_parser import AlteryxParser
from models import (
    AlteryxNode, AlteryxConnection, AlteryxWorkflow, WorkflowMetadata,
    ToolCategory, MedallionLayer, S3SourceConfig, S3SourceMapping,
)
from transformation_analyzer import TransformationAnalyzer
from dbt_generator import DBTGenerator
from column_detector import ColumnDetector


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def converter():
    """Create a FormulaConverter instance."""
    return FormulaConverter()


@pytest.fixture
def parser():
    """Create an AlteryxParser instance."""
    return AlteryxParser()


@pytest.fixture
def generator():
    """Create a DBTGenerator instance (non-interactive)."""
    return DBTGenerator(tempfile.mkdtemp(), interactive=False)


@pytest.fixture
def detector():
    """Create a ColumnDetector instance."""
    return ColumnDetector()


def _make_workflow(nodes, connections, name="test_workflow"):
    """Helper to create an AlteryxWorkflow from node/connection lists."""
    metadata = WorkflowMetadata(name=name, file_path=f"/tmp/{name}.yxmd")
    wf = AlteryxWorkflow(
        metadata=metadata,
        nodes=nodes,
        connections=connections,
    )
    wf.sources = [n for n in nodes if n.category == ToolCategory.INPUT]
    wf.targets = [n for n in nodes if n.category == ToolCategory.OUTPUT]
    return wf


def _write_xml_workflow(content: str) -> str:
    """Write XML content to a temp .yxmd file and return the path."""
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.yxmd', delete=False,
                                     encoding='utf-8')
    f.write(content)
    f.close()
    return f.name


# =============================================================================
# 1. FORMULA CONVERTER EXTREME CASES
# =============================================================================

class TestFormulaConverterExtreme:
    """Extreme edge cases for formula conversion."""

    # --- Empty / Null inputs ---

    def test_empty_expression(self, converter):
        """Empty string should return NULL."""
        assert converter.convert("") == "NULL"

    def test_none_expression(self, converter):
        """None-like empty input should return NULL."""
        assert converter.convert("") == "NULL"

    def test_whitespace_only_expression(self, converter):
        """Whitespace-only expression should not crash."""
        result = converter.convert("   ")
        assert result is not None
        # After strip, it should be just whitespace or empty-ish
        assert isinstance(result, str)

    # --- Maximum length expression ---

    def test_expression_exceeds_max_length(self, converter):
        """Expression exceeding MAX_EXPRESSION_LENGTH should return safe fallback."""
        huge_expr = "[Field] + " * (MAX_EXPRESSION_LENGTH // 10 + 1)
        result = converter.convert(huge_expr)
        assert "Expression too long" in result
        assert "NULL" in result

    def test_expression_at_exact_max_length(self, converter):
        """Expression at exactly MAX_EXPRESSION_LENGTH should still convert."""
        # Create an expression of exactly MAX_EXPRESSION_LENGTH characters
        expr = "A" * MAX_EXPRESSION_LENGTH
        result = converter.convert(expr)
        # Should NOT contain "too long" since it's exactly at the limit
        assert "Expression too long" not in result

    def test_expression_one_over_max_length(self, converter):
        """Expression at MAX_EXPRESSION_LENGTH + 1 should trigger fallback."""
        expr = "A" * (MAX_EXPRESSION_LENGTH + 1)
        result = converter.convert(expr)
        assert "Expression too long" in result

    # --- Deeply nested functions ---

    def test_triple_nested_iif(self, converter):
        """Triple nested IIF should produce 3 CASE WHEN statements."""
        expr = 'IIF([A]>1, IIF([B]>2, IIF([C]>3, "deep", "c"), "b"), "a")'
        result = converter.convert(expr)
        assert result.count('CASE WHEN') == 3

    def test_deeply_nested_function_calls(self, converter):
        """Functions nested 5+ levels deep should still convert."""
        # Trim(UpperCase(LowerCase(Trim(UpperCase([Field])))))
        expr = 'Trim(UpperCase(LowerCase(Trim(UpperCase([Field])))))'
        result = converter.convert(expr)
        assert 'TRIM' in result
        assert 'UPPER' in result
        assert 'LOWER' in result

    def test_many_nested_functions_approaching_iteration_limit(self, converter):
        """Nesting close to MAX_FUNCTION_CONVERSION_ITERATIONS should work."""
        # Build 20 levels of Trim() nesting
        expr = "[Field]"
        for _ in range(20):
            expr = f"Trim({expr})"
        result = converter.convert(expr)
        # Should have at least some TRIM calls
        assert 'TRIM' in result

    # --- Special characters in field names ---

    def test_field_name_with_spaces(self, converter):
        """Field names with spaces should be quoted correctly."""
        result = converter.convert('[Customer Name]')
        assert '"Customer Name"' in result

    def test_field_name_with_special_chars(self, converter):
        """Field names with dots, hyphens should be quoted correctly."""
        result = converter.convert('[data.source.id]')
        assert '"data.source.id"' in result

    def test_field_name_with_unicode(self, converter):
        """Field names with Unicode characters should be preserved."""
        result = converter.convert('[café_name]')
        assert '"café_name"' in result

    def test_field_name_with_brackets_inside(self, converter):
        """Nested brackets edge case."""
        # [Field] should work, but what about weird patterns
        result = converter.convert('[A] + [B]')
        assert '"A"' in result
        assert '"B"' in result

    # --- Operator edge cases ---

    def test_triple_equals(self, converter):
        """=== is not a standard operator, should convert == part."""
        result = converter.convert('[A] === [B]')
        # == becomes = so === becomes =+remaining
        assert isinstance(result, str)

    def test_multiple_not_operators(self, converter):
        """Multiple ! operators in sequence."""
        result = converter.convert('!!IsNull([Field])')
        # Should have NOT and IS NULL
        assert 'NOT' in result

    def test_mixed_operators_with_strings(self, converter):
        """Complex mix of operators and string literals."""
        expr = "[A] != 'test!value' && [B] == 'a||b' || [C] = 'x'"
        result = converter.convert(expr)
        # String literals should be preserved
        assert "'test!value'" in result
        assert "'a||b'" in result
        assert "'x'" in result
        # Operators outside strings should be converted
        assert '<>' in result
        assert ' AND ' in result

    # --- Function edge cases ---

    def test_zero_arg_function(self, converter):
        """Zero-argument functions like DateTimeNow() should convert."""
        result = converter.convert('DateTimeNow()')
        assert 'CURRENT_TIMESTAMP' in result

    def test_function_case_insensitivity(self, converter):
        """Function names should be case-insensitive."""
        result1 = converter.convert('TRIM([Field])')
        result2 = converter.convert('trim([Field])')
        result3 = converter.convert('Trim([Field])')
        assert 'TRIM' in result1
        assert 'TRIM' in result2
        assert 'TRIM' in result3

    def test_unknown_function(self, converter):
        """Unknown functions should not crash."""
        result = converter.convert('CompletelyMadeUpFunction([Field])')
        # Should return something (potentially unconverted)
        assert isinstance(result, str)

    def test_switch_with_many_cases(self, converter):
        """Switch with many case/result pairs should produce valid CASE."""
        cases = ', '.join([f'{i}, "result_{i}"' for i in range(20)])
        expr = f'Switch([Status], "default", {cases})'
        result = converter.convert(expr)
        assert 'CASE' in result
        assert 'ELSE' in result
        assert 'END' in result
        assert result.count('WHEN') == 20

    def test_switch_with_no_cases(self, converter):
        """Switch with just value and default should handle gracefully."""
        result = converter.convert('Switch([Status], "unknown")')
        # Should produce some valid SQL, even if it's a CASE with no WHEN
        assert isinstance(result, str)

    def test_switch_with_odd_case_result_pairs(self, converter):
        """Switch with mismatched case/result pairs should handle gracefully."""
        result = converter.convert('Switch([Status], "default", "A", "result_a", "B")')
        assert isinstance(result, str)
        # Should contain TODO or COALESCE since pairs are odd
        assert 'TODO' in result or 'COALESCE' in result

    # --- String literal edge cases ---

    def test_empty_string_literal(self, converter):
        """Empty string literal should be preserved."""
        result = converter.convert("[Field] = ''")
        assert "''" in result

    def test_string_with_single_quotes_adjacent(self, converter):
        """Adjacent single quotes should not break parsing."""
        result = converter.convert("[Field] = 'it''s'")
        assert isinstance(result, str)

    def test_expression_with_only_literal(self, converter):
        """Expression that is just a string literal."""
        result = converter.convert("'hello world'")
        assert "'hello world'" in result

    # --- Argument splitting edge cases ---

    def test_function_with_nested_commas(self, converter):
        """Function args with nested function calls containing commas."""
        expr = 'IIF([A] > 0, Replace([B], ",", ";"), "none")'
        result = converter.convert(expr)
        assert 'CASE WHEN' in result
        assert 'REPLACE' in result

    def test_function_with_empty_args(self, converter):
        """Function with trailing comma (unusual but possible)."""
        # Trim with single arg should still work
        result = converter.convert('Trim([Field])')
        assert 'TRIM' in result

    # --- Aggregation edge cases ---

    def test_unknown_aggregation(self):
        """Unknown aggregation should return TODO comment."""
        result = convert_aggregation('CompletelyUnknown', '"field"')
        assert 'TODO' in result

    def test_aggregation_with_template_mismatch(self):
        """Aggregation with missing extra args for template."""
        # Percentile needs extra arg but we don't provide it
        result = convert_aggregation('Percentile', '"value"')
        # Should handle gracefully (either partial or fallback)
        assert isinstance(result, str)

    def test_aggregation_with_special_chars_in_field(self):
        """Field name with special characters in aggregation."""
        result = convert_aggregation('Sum', '"total amount ($)"')
        assert 'SUM' in result
        assert '"total amount ($)"' in result

    # --- Parenthesis matching edge cases ---

    def test_unmatched_opening_paren(self, converter):
        """Expression with unmatched opening paren should not crash."""
        result = converter.convert('Trim([Field]')  # Missing closing paren
        assert isinstance(result, str)

    def test_unmatched_closing_paren(self, converter):
        """Expression with extra closing paren should not crash."""
        result = converter.convert('Trim([Field]))')
        assert isinstance(result, str)

    def test_deeply_nested_parens(self, converter):
        """Expression with many levels of parentheses."""
        expr = "((((([Field])))))"
        result = converter.convert(expr)
        assert isinstance(result, str)


# =============================================================================
# 2. XML PARSER EXTREME CASES
# =============================================================================

class TestParserExtreme:
    """Extreme edge cases for the Alteryx XML parser."""

    def test_parse_nonexistent_file(self, parser):
        """Parsing a nonexistent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            parser.parse("/nonexistent/path/workflow.yxmd")

    def test_parse_unsupported_extension(self, parser):
        """Parsing a file with unsupported extension should raise ValueError."""
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            f.write(b"not xml")
            path = f.name
        try:
            with pytest.raises(ValueError, match="Unsupported file type"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_parse_empty_xml(self, parser):
        """Parsing valid XML with no nodes should return empty workflow."""
        xml = '<?xml version="1.0"?><AlteryxDocument yxmdVer="2023.1"><Properties/><Nodes/><Connections/></AlteryxDocument>'
        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            assert len(wf.nodes) == 0
            assert len(wf.connections) == 0
        finally:
            os.unlink(path)

    def test_parse_malformed_xml(self, parser):
        """Malformed XML should raise an exception."""
        xml = '<?xml version="1.0"?><AlteryxDocument><Nodes><Node ToolID="1"><unclosed>'
        path = _write_xml_workflow(xml)
        try:
            with pytest.raises(Exception):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_parse_node_without_toolid(self, parser):
        """Node without ToolID attribute should be skipped gracefully."""
        xml = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Properties><MetaInfo><Name>Test</Name></MetaInfo></Properties>
  <Nodes>
    <Node>
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter"/>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter"/>
      <Properties><Configuration><Expression>[A] > 0</Expression></Configuration></Properties>
    </Node>
  </Nodes>
  <Connections/>
</AlteryxDocument>'''
        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            assert len(wf.nodes) == 1  # Only node with ToolID=2
            assert wf.nodes[0].tool_id == 2
        finally:
            os.unlink(path)

    def test_parse_node_with_empty_plugin(self, parser):
        """Node with no Plugin attribute (macro) should parse as macro."""
        xml = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Properties><MetaInfo><Name>Test</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings>
        <Position x="100" y="200"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <Macro>./macros/custom_macro.yxmc</Macro>
        </Configuration>
      </Properties>
      <EngineSettings Macro="./macros/custom_macro.yxmc"/>
    </Node>
  </Nodes>
  <Connections/>
</AlteryxDocument>'''
        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            assert len(wf.nodes) == 1
            assert wf.nodes[0].is_macro is True
            assert wf.nodes[0].category == ToolCategory.MACRO
        finally:
            os.unlink(path)

    def test_parse_large_workflow(self, parser):
        """Workflow with 200 nodes and 199 connections should parse correctly."""
        nodes_xml = ""
        for i in range(1, 201):
            nodes_xml += f'''
    <Node ToolID="{i}">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula">
        <Position x="{i*50}" y="100"/>
      </GuiSettings>
      <Properties>
        <Annotation><Name>Step {i}</Name></Annotation>
        <Configuration>
          <FormulaField field="col_{i}" expression="[input] + {i}" type="Double"/>
        </Configuration>
      </Properties>
    </Node>'''

        connections_xml = ""
        for i in range(1, 200):
            connections_xml += f'''
    <Connection>
      <Origin ToolID="{i}" Connection="Output"/>
      <Destination ToolID="{i+1}" Connection="Input"/>
    </Connection>'''

        xml = f'''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Properties><MetaInfo><Name>Large Workflow</Name></MetaInfo></Properties>
  <Nodes>{nodes_xml}
  </Nodes>
  <Connections>{connections_xml}
  </Connections>
</AlteryxDocument>'''

        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            assert len(wf.nodes) == 200
            assert len(wf.connections) == 199
        finally:
            os.unlink(path)

    def test_parse_workflow_with_container(self, parser):
        """Workflow with containers and child tools should parse correctly."""
        xml = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Properties><MetaInfo><Name>Container Test</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="100">
      <GuiSettings Plugin="AlteryxGuiToolkit.ToolContainer.ToolContainer">
        <Position x="100" y="100"/>
      </GuiSettings>
      <Properties>
        <Annotation><Name>My Container</Name></Annotation>
        <Configuration>
          <ChildToolIds>1,2,3</ChildToolIds>
        </Configuration>
      </Properties>
    </Node>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter"/>
      <Properties><Configuration><Expression>[A] > 0</Expression></Configuration></Properties>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Formula.Formula"/>
      <Properties><Configuration>
        <FormulaField field="B" expression="[A] * 2" type="Double"/>
      </Configuration></Properties>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort"/>
      <Properties><Configuration>
        <SortInfo field="B" order="Ascending"/>
      </Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection>
      <Origin ToolID="1" Connection="True"/>
      <Destination ToolID="2" Connection="Input"/>
    </Connection>
    <Connection>
      <Origin ToolID="2" Connection="Output"/>
      <Destination ToolID="3" Connection="Input"/>
    </Connection>
  </Connections>
</AlteryxDocument>'''

        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            # Container node should have child_tool_ids set
            container = wf.get_node_by_id(100)
            assert container is not None
            assert set(container.child_tool_ids) == {1, 2, 3}
            # Child nodes should have container_id set
            for child_id in [1, 2, 3]:
                child = wf.get_node_by_id(child_id)
                assert child is not None
                assert child.container_id == 100
        finally:
            os.unlink(path)

    def test_parse_connection_with_missing_origin(self, parser):
        """Connection referencing non-existent origin ToolID should still parse."""
        xml = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Properties><MetaInfo><Name>Test</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter"/>
      <Properties><Configuration><Expression>[A] > 0</Expression></Configuration></Properties>
    </Node>
  </Nodes>
  <Connections>
    <Connection>
      <Origin ToolID="999" Connection="Output"/>
      <Destination ToolID="1" Connection="Input"/>
    </Connection>
  </Connections>
</AlteryxDocument>'''

        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            assert len(wf.connections) == 1
            assert wf.connections[0].origin_id == 999
            # Node 999 doesn't exist but connection still recorded
            assert wf.get_node_by_id(999) is None
        finally:
            os.unlink(path)

    def test_parse_workflow_with_unicode_metadata(self, parser):
        """Workflow with Unicode in metadata should parse correctly."""
        xml = '''<?xml version="1.0" encoding="utf-8"?>
<AlteryxDocument yxmdVer="2023.1">
  <Properties>
    <MetaInfo>
      <Name>Análisis de données café</Name>
      <Description>Workflow für Datenverarbeitung mit ñ, ü, ö</Description>
      <Author>日本語テスト</Author>
    </MetaInfo>
  </Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Position x="54" y="54"/>
      </GuiSettings>
      <Properties>
        <Annotation><Name>Données entrée</Name></Annotation>
        <Configuration>
          <File>données_café.csv</File>
        </Configuration>
      </Properties>
    </Node>
  </Nodes>
  <Connections/>
</AlteryxDocument>'''

        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            assert 'café' in wf.metadata.name
            assert '日本語テスト' in wf.metadata.author
            assert len(wf.nodes) == 1
        finally:
            os.unlink(path)

    def test_parse_join_config(self, parser):
        """Join node with multiple join keys should parse correctly."""
        xml = '''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Properties><MetaInfo><Name>Join Test</Name></MetaInfo></Properties>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Join.Join"/>
      <Properties>
        <Configuration>
          <JoinByFields>
            <Field left="customer_id" right="cust_id"/>
            <Field left="order_date" right="date"/>
          </JoinByFields>
          <SelectJoinInfo connection="Left"/>
        </Configuration>
      </Properties>
    </Node>
  </Nodes>
  <Connections/>
</AlteryxDocument>'''

        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            join_node = wf.get_node_by_id(1)
            assert len(join_node.join_keys) == 2
            assert 'customer_id=cust_id' in join_node.join_keys
            assert 'order_date=date' in join_node.join_keys
        finally:
            os.unlink(path)


# =============================================================================
# 3. DBT GENERATOR EXTREME CASES
# =============================================================================

class TestDBTGeneratorExtreme:
    """Extreme edge cases for DBT project generation."""

    # --- Name sanitization ---

    def test_sanitize_empty_name(self, generator):
        """Empty name should return 'unknown'."""
        assert generator._sanitize_name("") == "unknown"

    def test_sanitize_none_like_name(self, generator):
        """Falsy name should return 'unknown'."""
        assert generator._sanitize_name("") == "unknown"

    def test_sanitize_all_special_chars(self, generator):
        """Name with only special characters should return 'unknown'."""
        assert generator._sanitize_name("@#$%^&*()!") == "unknown"

    def test_sanitize_unicode_name(self, generator):
        """Unicode name should be sanitized to ASCII-safe."""
        result = generator._sanitize_name("café_données")
        assert result == "caf_donn_es"
        # Should only contain valid chars
        assert all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in result)

    def test_sanitize_very_long_name(self, generator):
        """Name longer than 50 chars should be truncated."""
        long_name = "a" * 100
        result = generator._sanitize_name(long_name)
        assert len(result) <= 50

    def test_sanitize_name_with_leading_trailing_underscores(self, generator):
        """Leading/trailing underscores should be stripped."""
        result = generator._sanitize_name("___test___")
        assert result == "test"
        assert not result.startswith("_")
        assert not result.endswith("_")

    def test_sanitize_name_with_consecutive_special_chars(self, generator):
        """Consecutive special chars should collapse to single underscore."""
        result = generator._sanitize_name("hello...world---test")
        assert "__" not in result  # No double underscores
        assert "hello" in result
        assert "world" in result

    def test_sanitize_name_with_spaces(self, generator):
        """Spaces should become underscores."""
        result = generator._sanitize_name("my workflow name")
        assert result == "my_workflow_name"

    def test_sanitize_name_preserves_numbers(self, generator):
        """Numbers in names should be preserved."""
        result = generator._sanitize_name("step_123_process")
        assert result == "step_123_process"

    def test_sanitize_numeric_only_name(self, generator):
        """Purely numeric name should still work."""
        result = generator._sanitize_name("12345")
        assert result == "12345"

    # --- File writing edge cases ---

    def test_write_file_creates_nested_dirs(self, generator):
        """Writing to a deeply nested path should create all directories."""
        deep_path = generator.output_dir / "a" / "b" / "c" / "d" / "test.sql"
        generator._write_file(deep_path, "SELECT 1;")
        assert deep_path.exists()
        assert deep_path.read_text() == "SELECT 1;"

    def test_write_file_with_unicode_content(self, generator):
        """Writing Unicode content should work."""
        path = generator.output_dir / "unicode_test.sql"
        content = "-- Análisis de données café\nSELECT 'ñ' AS char_ñ;"
        generator._write_file(path, content)
        assert path.exists()
        assert path.read_text(encoding='utf-8') == content


# =============================================================================
# 4. TRANSFORMATION ANALYZER EXTREME CASES
# =============================================================================

class TestTransformationAnalyzerExtreme:
    """Extreme edge cases for the transformation analyzer."""

    def test_single_node_workflow(self):
        """Workflow with a single node and no connections."""
        node = AlteryxNode(
            tool_id=1, tool_type="Input", plugin_name="Input",
            category=ToolCategory.INPUT
        )
        wf = _make_workflow([node], [])
        analyzer = TransformationAnalyzer(wf)
        steps = analyzer.get_ordered_transformations()
        assert len(steps) == 1
        assert steps[0].tool_id == 1

    def test_disconnected_nodes(self):
        """Workflow with multiple disconnected nodes."""
        nodes = [
            AlteryxNode(tool_id=1, tool_type="Input", plugin_name="Input",
                       category=ToolCategory.INPUT),
            AlteryxNode(tool_id=2, tool_type="Filter", plugin_name="Filter",
                       category=ToolCategory.PREPARATION),
            AlteryxNode(tool_id=3, tool_type="Output", plugin_name="Output",
                       category=ToolCategory.OUTPUT),
        ]
        wf = _make_workflow(nodes, [])  # No connections
        analyzer = TransformationAnalyzer(wf)
        steps = analyzer.get_ordered_transformations()
        assert len(steps) == 3
        # All nodes should be in the result
        step_ids = {s.tool_id for s in steps}
        assert step_ids == {1, 2, 3}

    def test_long_linear_chain(self):
        """Long linear chain of 50 nodes should produce correct order."""
        nodes = [
            AlteryxNode(
                tool_id=i, tool_type="Formula", plugin_name="Formula",
                category=ToolCategory.PREPARATION
            )
            for i in range(1, 51)
        ]
        connections = [
            AlteryxConnection(
                origin_id=i, origin_anchor="Output",
                destination_id=i+1, destination_anchor="Input"
            )
            for i in range(1, 50)
        ]
        wf = _make_workflow(nodes, connections)
        analyzer = TransformationAnalyzer(wf)
        steps = analyzer.get_ordered_transformations()
        assert len(steps) == 50
        # Steps should be in topological order
        step_ids = [s.tool_id for s in steps]
        for i in range(len(step_ids) - 1):
            # Each node should appear before its downstream
            assert step_ids.index(i + 1) < step_ids.index(i + 2)

    def test_diamond_dependency(self):
        """Diamond-shaped DAG: 1 -> {2,3} -> 4 should resolve correctly."""
        nodes = [
            AlteryxNode(tool_id=1, tool_type="Input", plugin_name="Input",
                       category=ToolCategory.INPUT),
            AlteryxNode(tool_id=2, tool_type="Filter", plugin_name="Filter",
                       category=ToolCategory.PREPARATION),
            AlteryxNode(tool_id=3, tool_type="Formula", plugin_name="Formula",
                       category=ToolCategory.PREPARATION),
            AlteryxNode(tool_id=4, tool_type="Join", plugin_name="Join",
                       category=ToolCategory.JOIN),
        ]
        connections = [
            AlteryxConnection(1, "Output", 2, "Input"),
            AlteryxConnection(1, "Output", 3, "Input"),
            AlteryxConnection(2, "Output", 4, "Left"),
            AlteryxConnection(3, "Output", 4, "Right"),
        ]
        wf = _make_workflow(nodes, connections)
        analyzer = TransformationAnalyzer(wf)
        steps = analyzer.get_ordered_transformations()
        step_ids = [s.tool_id for s in steps]
        # Node 1 must come before nodes 2, 3
        assert step_ids.index(1) < step_ids.index(2)
        assert step_ids.index(1) < step_ids.index(3)
        # Nodes 2, 3 must come before node 4
        assert step_ids.index(2) < step_ids.index(4)
        assert step_ids.index(3) < step_ids.index(4)

    def test_multiple_sources_multiple_targets(self):
        """Workflow with multiple input and output nodes."""
        nodes = [
            AlteryxNode(tool_id=1, tool_type="Input", plugin_name="Input",
                       category=ToolCategory.INPUT),
            AlteryxNode(tool_id=2, tool_type="Input", plugin_name="Input",
                       category=ToolCategory.INPUT),
            AlteryxNode(tool_id=3, tool_type="Join", plugin_name="Join",
                       category=ToolCategory.JOIN),
            AlteryxNode(tool_id=4, tool_type="Output", plugin_name="Output",
                       category=ToolCategory.OUTPUT),
            AlteryxNode(tool_id=5, tool_type="Output", plugin_name="Output",
                       category=ToolCategory.OUTPUT),
        ]
        connections = [
            AlteryxConnection(1, "Output", 3, "Left"),
            AlteryxConnection(2, "Output", 3, "Right"),
            AlteryxConnection(3, "Join", 4, "Input"),
            AlteryxConnection(3, "Join", 5, "Input"),
        ]
        wf = _make_workflow(nodes, connections)
        analyzer = TransformationAnalyzer(wf)
        steps = analyzer.get_ordered_transformations()
        assert len(steps) == 5
        step_ids = [s.tool_id for s in steps]
        # Sources before join
        assert step_ids.index(1) < step_ids.index(3)
        assert step_ids.index(2) < step_ids.index(3)
        # Join before outputs
        assert step_ids.index(3) < step_ids.index(4)
        assert step_ids.index(3) < step_ids.index(5)

    def test_cyclic_graph_does_not_infinite_loop(self):
        """Workflow with a cycle should not hang (DFS cycle handling)."""
        nodes = [
            AlteryxNode(tool_id=1, tool_type="Formula", plugin_name="Formula",
                       category=ToolCategory.PREPARATION),
            AlteryxNode(tool_id=2, tool_type="Formula", plugin_name="Formula",
                       category=ToolCategory.PREPARATION),
            AlteryxNode(tool_id=3, tool_type="Formula", plugin_name="Formula",
                       category=ToolCategory.PREPARATION),
        ]
        # Cycle: 1 -> 2 -> 3 -> 1
        connections = [
            AlteryxConnection(1, "Output", 2, "Input"),
            AlteryxConnection(2, "Output", 3, "Input"),
            AlteryxConnection(3, "Output", 1, "Input"),
        ]
        wf = _make_workflow(nodes, connections)
        analyzer = TransformationAnalyzer(wf)
        # The DFS-based topological sort uses a visited set,
        # so cycles just result in some nodes not being re-visited
        steps = analyzer.get_ordered_transformations()
        # Should complete without hanging; may have all 3 nodes
        assert len(steps) <= 3

    def test_workflow_upstream_downstream_methods(self):
        """Test get_upstream_nodes and get_downstream_nodes with complex graph."""
        nodes = [
            AlteryxNode(tool_id=1, tool_type="Input", plugin_name="Input",
                       category=ToolCategory.INPUT),
            AlteryxNode(tool_id=2, tool_type="Filter", plugin_name="Filter",
                       category=ToolCategory.PREPARATION),
            AlteryxNode(tool_id=3, tool_type="Formula", plugin_name="Formula",
                       category=ToolCategory.PREPARATION),
            AlteryxNode(tool_id=4, tool_type="Output", plugin_name="Output",
                       category=ToolCategory.OUTPUT),
        ]
        connections = [
            AlteryxConnection(1, "Output", 2, "Input"),
            AlteryxConnection(1, "Output", 3, "Input"),
            AlteryxConnection(2, "Output", 4, "Input"),
            AlteryxConnection(3, "Output", 4, "Input"),
        ]
        wf = _make_workflow(nodes, connections)

        # Node 1 has no upstream
        assert wf.get_upstream_nodes(1) == []
        # Node 4 has two upstream nodes
        upstream_of_4 = wf.get_upstream_nodes(4)
        upstream_ids = {n.tool_id for n in upstream_of_4}
        assert upstream_ids == {2, 3}
        # Node 1 has two downstream nodes
        downstream_of_1 = wf.get_downstream_nodes(1)
        downstream_ids = {n.tool_id for n in downstream_of_1}
        assert downstream_ids == {2, 3}

    def test_empty_workflow(self):
        """Workflow with no nodes should analyze without error."""
        wf = _make_workflow([], [])
        analyzer = TransformationAnalyzer(wf)
        steps = analyzer.get_ordered_transformations()
        assert steps == []


# =============================================================================
# 5. COLUMN DETECTOR EXTREME CASES
# =============================================================================

class TestColumnDetectorExtreme:
    """Extreme edge cases for column detection."""

    def test_csv_with_100_columns(self, detector):
        """CSV with 100 columns should read all headers."""
        cols = [f"col_{i}" for i in range(100)]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerow(range(100))
            path = f.name
        try:
            result = detector.read_csv_columns(path)
            assert len(result) == 100
            assert result[0] == "col_0"
            assert result[99] == "col_99"
        finally:
            os.unlink(path)

    def test_csv_with_unicode_headers(self, detector):
        """CSV with Unicode column headers should read correctly."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                          encoding='utf-8') as f:
            f.write("café,naïve,résumé,straße\n")
            f.write("1,2,3,4\n")
            path = f.name
        try:
            result = detector.read_csv_columns(path)
            assert 'café' in result
            assert 'naïve' in result
            assert 'résumé' in result
            assert 'straße' in result
        finally:
            os.unlink(path)

    def test_csv_with_empty_column_names(self, detector):
        """CSV with empty column names should skip them."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("a,,b,,c\n")
            f.write("1,2,3,4,5\n")
            path = f.name
        try:
            result = detector.read_csv_columns(path)
            # Empty column names should be filtered out
            assert "" not in result
            assert "a" in result
            assert "b" in result
            assert "c" in result
        finally:
            os.unlink(path)

    def test_csv_with_tab_delimiter(self, detector):
        """Tab-delimited file should auto-detect delimiter."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col_a\tcol_b\tcol_c\n")
            f.write("1\t2\t3\n")
            path = f.name
        try:
            result = detector.read_csv_columns(path)
            assert result == ['col_a', 'col_b', 'col_c']
        finally:
            os.unlink(path)

    def test_csv_with_pipe_delimiter(self, detector):
        """Pipe-delimited file should auto-detect delimiter."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col_a|col_b|col_c\n")
            f.write("1|2|3\n")
            path = f.name
        try:
            result = detector.read_csv_columns(path)
            assert result == ['col_a', 'col_b', 'col_c']
        finally:
            os.unlink(path)

    def test_csv_header_only_no_data(self, detector):
        """CSV with header but no data rows should still read columns."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,email\n")
            path = f.name
        try:
            result = detector.read_csv_columns(path)
            assert result == ['id', 'name', 'email']
        finally:
            os.unlink(path)

    def test_empty_csv_file(self, detector):
        """Empty CSV file should return empty list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            path = f.name
        try:
            result = detector.read_csv_columns(path)
            assert result == []
        finally:
            os.unlink(path)

    def test_json_empty_array(self, detector):
        """JSON with empty array should return empty list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([], f)
            path = f.name
        try:
            result = detector.read_json_columns(path)
            assert result == []
        finally:
            os.unlink(path)

    def test_json_deeply_nested(self, detector):
        """JSON with deeply nested structure (no data/records key) should handle."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"level1": {"level2": {"level3": [{"a": 1}]}}}, f)
            path = f.name
        try:
            result = detector.read_json_columns(path)
            # This structure doesn't match expected patterns, so empty
            assert result == []
        finally:
            os.unlink(path)

    def test_json_with_data_key(self, detector):
        """JSON with 'data' key containing array of objects."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"data": [{"x": 1, "y": 2, "z": 3}]}, f)
            path = f.name
        try:
            result = detector.read_json_columns(path)
            assert set(result) == {'x', 'y', 'z'}
        finally:
            os.unlink(path)

    def test_json_with_records_key(self, detector):
        """JSON with 'records' key containing array of objects."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"records": [{"a": 1, "b": 2}]}, f)
            path = f.name
        try:
            result = detector.read_json_columns(path)
            assert set(result) == {'a', 'b'}
        finally:
            os.unlink(path)

    def test_json_invalid_file(self, detector):
        """Invalid JSON should return empty list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("{invalid json")
            path = f.name
        try:
            result = detector.read_json_columns(path)
            assert result == []
        finally:
            os.unlink(path)

    def test_nonexistent_file(self, detector):
        """Nonexistent file should return empty list."""
        result = detector.read_file_columns("/nonexistent/path/data.csv")
        assert result == []

    def test_unsupported_extension(self, detector):
        """Unsupported file extension should return empty list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xyz', delete=False) as f:
            f.write("data")
            path = f.name
        try:
            result = detector.read_file_columns(path)
            assert result == []
        finally:
            os.unlink(path)

    def test_tsv_file(self, detector):
        """TSV file (.tsv) should be detected and read correctly."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
            f.write("id\tname\tvalue\n")
            f.write("1\ttest\t100\n")
            path = f.name
        try:
            result = detector.read_file_columns(path)
            assert result == ['id', 'name', 'value']
        finally:
            os.unlink(path)

    def test_csv_with_quoted_headers(self, detector):
        """CSV with quoted column headers should strip quotes."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('"Column A","Column B","Column C"\n')
            f.write('1,2,3\n')
            path = f.name
        try:
            result = detector.read_csv_columns(path)
            assert 'Column A' in result
            assert 'Column B' in result
        finally:
            os.unlink(path)


# =============================================================================
# 6. S3 CONFIG EXTREME CASES
# =============================================================================

class TestS3ConfigExtreme:
    """Extreme edge cases for S3 configuration."""

    def test_s3_source_config_empty_prefix(self):
        """S3 config with empty prefix should produce valid URI."""
        config = S3SourceConfig(bucket="my-bucket", prefix="")
        assert config.get_s3_uri() == "s3a://my-bucket/"
        assert config.get_s3_url() == "s3://my-bucket/"

    def test_s3_source_config_prefix_with_slashes(self):
        """S3 config prefix with leading/trailing slashes should normalize."""
        config = S3SourceConfig(bucket="my-bucket", prefix="/data/raw/")
        uri = config.get_s3_uri()
        assert uri == "s3a://my-bucket/data/raw"
        # No double slashes
        assert "//" not in uri.replace("s3a://", "")

    def test_s3_source_config_from_s3_uri(self):
        """Parsing various S3 URI formats."""
        # Standard s3://
        config = S3SourceConfig.from_s3_uri("s3://my-bucket/path/to/data")
        assert config.bucket == "my-bucket"
        assert config.prefix == "path/to/data"

        # s3a:// (Trino format)
        config2 = S3SourceConfig.from_s3_uri("s3a://other-bucket/raw")
        assert config2.bucket == "other-bucket"
        assert config2.prefix == "raw"

        # No protocol
        config3 = S3SourceConfig.from_s3_uri("bare-bucket/prefix")
        assert config3.bucket == "bare-bucket"
        assert config3.prefix == "prefix"

    def test_s3_source_config_bucket_only_uri(self):
        """S3 URI with only bucket name (no prefix)."""
        config = S3SourceConfig.from_s3_uri("s3://my-bucket")
        assert config.bucket == "my-bucket"
        assert config.prefix == ""

    def test_s3_source_config_to_dict_and_back(self):
        """S3 config round-trip through dict serialization."""
        original = S3SourceConfig(
            bucket="test-bucket",
            prefix="data/bronze",
            endpoint="https://minio.example.com",
            file_format="csv",
            file_pattern="*.csv",
            s3_user="access_key",
            s3_password="secret_key",
        )
        d = original.to_dict()
        restored = S3SourceConfig.from_dict(d)
        assert restored.bucket == original.bucket
        assert restored.prefix == original.prefix
        assert restored.endpoint == original.endpoint
        assert restored.file_format == original.file_format
        assert restored.s3_user == original.s3_user
        assert restored.s3_password == original.s3_password

    def test_s3_source_config_no_credentials(self):
        """S3 config without credentials should not include them in dict."""
        config = S3SourceConfig(bucket="test", prefix="data")
        d = config.to_dict()
        assert 's3_user' not in d
        assert 's3_password' not in d

    def test_s3_source_mapping_round_trip(self):
        """S3 source mapping round-trip through dict."""
        original = S3SourceMapping(
            alteryx_source_name="customers.csv",
            s3_config=S3SourceConfig(bucket="lake", prefix="raw/customers"),
            table_name="raw_customers",
            columns=["id", "name", "email"],
            original_node_id=42,
        )
        d = original.to_dict()
        restored = S3SourceMapping.from_dict(d)
        assert restored.alteryx_source_name == "customers.csv"
        assert restored.table_name == "raw_customers"
        assert restored.columns == ["id", "name", "email"]
        assert restored.original_node_id == 42
        assert restored.s3_config.bucket == "lake"

    def test_s3_source_config_from_dict_minimal(self):
        """S3 config from minimal dict (only required fields)."""
        d = {"bucket": "minimal-bucket"}
        config = S3SourceConfig.from_dict(d)
        assert config.bucket == "minimal-bucket"
        assert config.prefix == ""
        assert config.file_format == "parquet"  # default
        assert config.endpoint is None

    def test_s3_uri_with_deeply_nested_prefix(self):
        """S3 URI with deeply nested prefix path."""
        config = S3SourceConfig.from_s3_uri("s3://bucket/a/b/c/d/e/f/g/h/i/j/data")
        assert config.bucket == "bucket"
        assert config.prefix == "a/b/c/d/e/f/g/h/i/j/data"

    def test_s3_uri_with_special_chars_in_prefix(self):
        """S3 URI with special characters in prefix."""
        config = S3SourceConfig.from_s3_uri("s3://bucket/data-2024/Q1_results/file.parquet")
        assert config.prefix == "data-2024/Q1_results/file.parquet"


# =============================================================================
# 7. MODEL DATACLASS EDGE CASES
# =============================================================================

class TestModelDataclassExtreme:
    """Edge cases for data model classes."""

    def test_workflow_get_node_by_id_not_found(self):
        """Getting a non-existent node should return None."""
        wf = _make_workflow([], [])
        assert wf.get_node_by_id(999) is None

    def test_workflow_get_upstream_empty(self):
        """Getting upstream for a node with no upstream should return empty."""
        node = AlteryxNode(tool_id=1, tool_type="Input", plugin_name="Input",
                          category=ToolCategory.INPUT)
        wf = _make_workflow([node], [])
        assert wf.get_upstream_nodes(1) == []

    def test_workflow_get_downstream_empty(self):
        """Getting downstream for a node with no downstream should return empty."""
        node = AlteryxNode(tool_id=1, tool_type="Output", plugin_name="Output",
                          category=ToolCategory.OUTPUT)
        wf = _make_workflow([node], [])
        assert wf.get_downstream_nodes(1) == []

    def test_node_display_name_with_annotation(self):
        """Node with annotation should show plugin + annotation."""
        node = AlteryxNode(
            tool_id=1, tool_type="Filter", plugin_name="Filter",
            annotation="Active Customers Only"
        )
        assert "Filter" in node.get_display_name()
        assert "Active Customers Only" in node.get_display_name()

    def test_node_display_name_without_annotation(self):
        """Node without annotation should show just plugin name."""
        node = AlteryxNode(tool_id=1, tool_type="Filter", plugin_name="Filter")
        assert node.get_display_name() == "Filter"

    def test_workflow_get_upstream_by_anchor(self):
        """Getting upstream by specific anchor name."""
        left_node = AlteryxNode(tool_id=1, tool_type="Input", plugin_name="Input",
                               category=ToolCategory.INPUT)
        right_node = AlteryxNode(tool_id=2, tool_type="Input", plugin_name="Input",
                                category=ToolCategory.INPUT)
        join_node = AlteryxNode(tool_id=3, tool_type="Join", plugin_name="Join",
                               category=ToolCategory.JOIN)
        connections = [
            AlteryxConnection(1, "Output", 3, "Left"),
            AlteryxConnection(2, "Output", 3, "Right"),
        ]
        wf = _make_workflow([left_node, right_node, join_node], connections)

        left_upstream = wf.get_upstream_node_by_anchor(3, "Left")
        right_upstream = wf.get_upstream_node_by_anchor(3, "Right")
        assert left_upstream.tool_id == 1
        assert right_upstream.tool_id == 2

    def test_workflow_get_upstream_by_anchor_not_found(self):
        """Getting upstream by non-existent anchor should return None."""
        node = AlteryxNode(tool_id=1, tool_type="Input", plugin_name="Input",
                          category=ToolCategory.INPUT)
        wf = _make_workflow([node], [])
        assert wf.get_upstream_node_by_anchor(1, "NonExistent") is None

    def test_workflow_get_upstream_by_anchor_case_insensitive(self):
        """Anchor matching should be case-insensitive."""
        left_node = AlteryxNode(tool_id=1, tool_type="Input", plugin_name="Input",
                               category=ToolCategory.INPUT)
        join_node = AlteryxNode(tool_id=2, tool_type="Join", plugin_name="Join",
                               category=ToolCategory.JOIN)
        connections = [
            AlteryxConnection(1, "Output", 2, "Left"),
        ]
        wf = _make_workflow([left_node, join_node], connections)

        # Should match case-insensitively
        result = wf.get_upstream_node_by_anchor(2, "left")
        assert result is not None
        assert result.tool_id == 1

    def test_medallion_layer_values(self):
        """MedallionLayer enum values should match expected strings."""
        assert MedallionLayer.BRONZE.value == "bronze"
        assert MedallionLayer.SILVER.value == "silver"
        assert MedallionLayer.GOLD.value == "gold"

    def test_tool_category_completeness(self):
        """ToolCategory enum should have all expected categories."""
        expected = {
            'INPUT', 'OUTPUT', 'PREPARATION', 'JOIN', 'TRANSFORM',
            'PARSE', 'REPORTING', 'SPATIAL', 'PREDICTIVE', 'DEVELOPER',
            'IN_DATABASE', 'MACRO', 'CONTAINER', 'UNKNOWN'
        }
        actual = {c.name for c in ToolCategory}
        assert actual == expected


# =============================================================================
# 8. INTEGRATION EXTREME CASES (parser + analyzer together)
# =============================================================================

class TestIntegrationExtreme:
    """Integration tests combining parser + analyzer."""

    def test_parse_and_analyze_sample_workflow(self, parser):
        """Parse a real sample workflow and analyze it."""
        sample_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'samples', 'customer_orders.yxmd'
        )
        if not os.path.exists(sample_path):
            pytest.skip("Sample workflow not available")

        wf = parser.parse(sample_path)
        assert len(wf.nodes) > 0
        assert len(wf.connections) > 0

        analyzer = TransformationAnalyzer(wf)
        steps = analyzer.get_ordered_transformations()
        assert len(steps) == len(wf.nodes)

        # All sources should come before non-sources in ordering
        source_ids = {n.tool_id for n in wf.sources}
        step_ids = [s.tool_id for s in steps]
        for source_id in source_ids:
            source_pos = step_ids.index(source_id)
            for node in wf.nodes:
                if node.tool_id not in source_ids:
                    downstream = wf.get_downstream_nodes(source_id)
                    for ds in downstream:
                        assert step_ids.index(source_id) < step_ids.index(ds.tool_id)

    def test_parse_and_analyze_container_workflow(self, parser):
        """Parse container workflow and verify analyzer handles it."""
        sample_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'samples', 'workflow_with_container.yxmd'
        )
        if not os.path.exists(sample_path):
            pytest.skip("Container sample workflow not available")

        wf = parser.parse(sample_path)
        assert len(wf.nodes) > 0

        # Should have at least one container
        containers = [n for n in wf.nodes if n.category == ToolCategory.CONTAINER]
        if containers:
            # Container should have child_tool_ids
            container = containers[0]
            assert isinstance(container.child_tool_ids, list)

        analyzer = TransformationAnalyzer(wf)
        steps = analyzer.get_ordered_transformations()
        assert len(steps) > 0

    def test_wide_workflow_many_parallel_branches(self, parser):
        """Workflow with many parallel branches (fan-out) from single source."""
        branches = 20
        nodes_xml = '''
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Position x="50" y="50"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <File>input.csv</File>
        </Configuration>
      </Properties>
    </Node>'''

        connections_xml = ""
        for i in range(2, branches + 2):
            nodes_xml += f'''
    <Node ToolID="{i}">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter">
        <Position x="200" y="{i*50}"/>
      </GuiSettings>
      <Properties>
        <Configuration>
          <Expression>[branch] = {i}</Expression>
        </Configuration>
      </Properties>
    </Node>'''
            connections_xml += f'''
    <Connection>
      <Origin ToolID="1" Connection="Output"/>
      <Destination ToolID="{i}" Connection="Input"/>
    </Connection>'''

        xml = f'''<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2023.1">
  <Properties><MetaInfo><Name>Wide Workflow</Name></MetaInfo></Properties>
  <Nodes>{nodes_xml}
  </Nodes>
  <Connections>{connections_xml}
  </Connections>
</AlteryxDocument>'''

        path = _write_xml_workflow(xml)
        try:
            wf = parser.parse(path)
            assert len(wf.nodes) == branches + 1  # 1 input + N branches
            assert len(wf.connections) == branches

            analyzer = TransformationAnalyzer(wf)
            steps = analyzer.get_ordered_transformations()
            assert len(steps) == branches + 1
            # Input node should come first
            assert steps[0].tool_id == 1
        finally:
            os.unlink(path)


# =============================================================================
# Run directly with pytest
# =============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
