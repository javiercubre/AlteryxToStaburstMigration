"""
Tests for Dynamic Input tool filename-as-source detection.

Verifies that when a Dynamic Input tool has a filename in its configuration,
the parser treats it as a data source and downstream modules handle it correctly.
"""
import os
import sys
import tempfile
import textwrap

import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alteryx_parser import AlteryxParser
from models import AlteryxNode, AlteryxWorkflow, AlteryxConnection, WorkflowMetadata, ToolCategory
from dbt_generator import DBTGenerator


def _make_workflow_xml(file_path: str) -> str:
    """Build a minimal Alteryx workflow XML containing a Dynamic Input tool."""
    return textwrap.dedent(f"""\
        <?xml version="1.0"?>
        <AlteryxDocument yxmdVer="2023.1">
          <Properties>
            <MetaInfo>
              <Name>DynamicInputTest</Name>
            </MetaInfo>
          </Properties>
          <Nodes>
            <Node ToolID="1">
              <GuiSettings Plugin="AlteryxBasePluginsGui.Dynamic.Dynamic">
                <Position x="100" y="100" />
              </GuiSettings>
              <Properties>
                <Configuration>
                  <File>{file_path}</File>
                  <RecordInfo>
                    <Field name="id" type="Int32" />
                    <Field name="name" type="V_WString" />
                  </RecordInfo>
                </Configuration>
                <Annotation>
                  <DefaultAnnotationText>Dynamic CSV</DefaultAnnotationText>
                </Annotation>
              </Properties>
            </Node>
            <Node ToolID="2">
              <GuiSettings Plugin="AlteryxBasePluginsGui.BrowseV2.BrowseV2">
                <Position x="300" y="100" />
              </GuiSettings>
              <Properties>
                <Configuration />
              </Properties>
            </Node>
          </Nodes>
          <Connections>
            <Connection>
              <Origin ToolID="1" Connection="Output" />
              <Destination ToolID="2" Connection="Input" />
            </Connection>
          </Connections>
        </AlteryxDocument>
    """)


def _make_workflow_xml_no_file() -> str:
    """Build a workflow XML with a Dynamic Input tool that has no filename."""
    return textwrap.dedent("""\
        <?xml version="1.0"?>
        <AlteryxDocument yxmdVer="2023.1">
          <Properties>
            <MetaInfo>
              <Name>DynamicInputNoFile</Name>
            </MetaInfo>
          </Properties>
          <Nodes>
            <Node ToolID="1">
              <GuiSettings Plugin="AlteryxBasePluginsGui.Dynamic.Dynamic">
                <Position x="100" y="100" />
              </GuiSettings>
              <Properties>
                <Configuration />
              </Properties>
            </Node>
          </Nodes>
          <Connections />
        </AlteryxDocument>
    """)


class TestDynamicInputParser:
    """Test that the parser extracts filenames from Dynamic Input tools."""

    def test_dynamic_input_with_file_is_source(self, tmp_path):
        """Dynamic Input with a <File> element should appear in workflow.sources."""
        xml = _make_workflow_xml("C:\\\\data\\\\customers.csv")
        wf_file = tmp_path / "test.yxmd"
        wf_file.write_text(xml)

        parser = AlteryxParser()
        workflow = parser.parse(str(wf_file))

        # The Dynamic Input node should be in sources
        source_ids = [n.tool_id for n in workflow.sources]
        assert 1 in source_ids, "Dynamic Input node with filename should be a source"

    def test_dynamic_input_source_path_extracted(self, tmp_path):
        """The source_path should be extracted from the Dynamic Input config."""
        xml = _make_workflow_xml("C:/data/customers.csv")
        wf_file = tmp_path / "test.yxmd"
        wf_file.write_text(xml)

        parser = AlteryxParser()
        workflow = parser.parse(str(wf_file))

        dynamic_node = workflow.get_node_by_id(1)
        assert dynamic_node is not None
        assert dynamic_node.source_path == "C:/data/customers.csv"

    def test_dynamic_input_without_file_not_source(self, tmp_path):
        """Dynamic Input without a filename should NOT appear in workflow.sources."""
        xml = _make_workflow_xml_no_file()
        wf_file = tmp_path / "test.yxmd"
        wf_file.write_text(xml)

        parser = AlteryxParser()
        workflow = parser.parse(str(wf_file))

        source_ids = [n.tool_id for n in workflow.sources]
        assert 1 not in source_ids, "Dynamic Input without filename should not be a source"

    def test_dynamic_input_category_is_parse(self, tmp_path):
        """Dynamic Input should retain its PARSE category."""
        xml = _make_workflow_xml("C:\\\\data\\\\customers.csv")
        wf_file = tmp_path / "test.yxmd"
        wf_file.write_text(xml)

        parser = AlteryxParser()
        workflow = parser.parse(str(wf_file))

        dynamic_node = workflow.get_node_by_id(1)
        assert dynamic_node.category == ToolCategory.PARSE

    def test_dynamic_input_field_types_extracted(self, tmp_path):
        """RecordInfo/Field elements should be parsed for Dynamic Input."""
        xml = _make_workflow_xml("data.csv")
        wf_file = tmp_path / "test.yxmd"
        wf_file.write_text(xml)

        parser = AlteryxParser()
        workflow = parser.parse(str(wf_file))

        dynamic_node = workflow.get_node_by_id(1)
        field_types = dynamic_node.configuration.get('field_types', {})
        assert 'id' in field_types
        assert 'name' in field_types


class TestDynamicInputDBTGenerator:
    """Test that the DBT generator treats Dynamic Input filenames as sources."""

    def test_is_source_node_dynamic_input(self):
        """_is_source_node should return True for Dynamic Input with source_path."""
        gen = DBTGenerator(tempfile.mkdtemp(), interactive=False)

        node = AlteryxNode(
            tool_id=1,
            tool_type="AlteryxBasePluginsGui.Dynamic.Dynamic",
            plugin_name="Dynamic Input/Output",
            category=ToolCategory.PARSE,
            source_path="data/orders.csv",
        )
        assert gen._is_source_node(node) is True

    def test_is_source_node_dynamic_input_no_path(self):
        """_is_source_node should return False for Dynamic Input without source_path."""
        gen = DBTGenerator(tempfile.mkdtemp(), interactive=False)

        node = AlteryxNode(
            tool_id=1,
            tool_type="AlteryxBasePluginsGui.Dynamic.Dynamic",
            plugin_name="Dynamic Input/Output",
            category=ToolCategory.PARSE,
        )
        assert gen._is_source_node(node) is False

    def test_is_source_node_regular_input(self):
        """_is_source_node should return True for regular INPUT tools."""
        gen = DBTGenerator(tempfile.mkdtemp(), interactive=False)

        node = AlteryxNode(
            tool_id=1,
            tool_type="AlteryxBasePluginsGui.DbFileInput.DbFileInput",
            plugin_name="Input Data",
            category=ToolCategory.INPUT,
        )
        assert gen._is_source_node(node) is True

    def test_get_model_reference_dynamic_input(self):
        """Dynamic Input source should get a stg_ model reference."""
        gen = DBTGenerator(tempfile.mkdtemp(), interactive=False)

        node = AlteryxNode(
            tool_id=1,
            tool_type="AlteryxBasePluginsGui.Dynamic.Dynamic",
            plugin_name="Dynamic Input/Output",
            category=ToolCategory.PARSE,
            source_path="data/orders.csv",
        )
        ref = gen._get_model_reference(node, "test_wf")
        assert ref.startswith("stg_"), f"Expected stg_ prefix, got: {ref}"
        assert "orders" in ref, f"Expected 'orders' in reference, got: {ref}"
