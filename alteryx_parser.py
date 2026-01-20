"""
Alteryx workflow XML parser.
Parses .yxmd and .yxmc files to extract nodes, connections, and configurations.
"""
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import re

from models import (
    AlteryxNode, AlteryxConnection, AlteryxWorkflow, WorkflowMetadata,
    ToolCategory
)
from typing import TYPE_CHECKING
from tool_mappings import get_category_from_plugin, get_simple_name


class AlteryxParser:
    """Parser for Alteryx workflow files (.yxmd, .yxmc)."""

    def __init__(self):
        self.workflow_path: Optional[Path] = None
        self.tree: Optional[ET.ElementTree] = None
        self.root: Optional[ET.Element] = None

    def parse(self, file_path: str) -> AlteryxWorkflow:
        """Parse an Alteryx workflow file and return a structured representation."""
        self.workflow_path = Path(file_path)

        if not self.workflow_path.exists():
            raise FileNotFoundError(f"Workflow file not found: {file_path}")

        if not self.workflow_path.suffix.lower() in ['.yxmd', '.yxmc', '.yxwz']:
            raise ValueError(f"Unsupported file type: {self.workflow_path.suffix}")

        # Parse XML
        self.tree = ET.parse(file_path)
        self.root = self.tree.getroot()

        # Extract components
        metadata = self._parse_metadata()
        nodes = self._parse_nodes()
        connections = self._parse_connections()

        # Build workflow object
        workflow = AlteryxWorkflow(
            metadata=metadata,
            nodes=nodes,
            connections=connections,
        )

        # Identify sources and targets
        workflow.sources = [n for n in nodes if n.category == ToolCategory.INPUT]
        workflow.targets = [n for n in nodes if n.category == ToolCategory.OUTPUT]

        # Identify macros
        for node in nodes:
            if node.is_macro and node.macro_path:
                workflow.macros_used.append(node.macro_path)

        return workflow

    def _parse_metadata(self) -> WorkflowMetadata:
        """Extract workflow metadata from Properties element."""
        metadata = WorkflowMetadata(
            name=self.workflow_path.stem,
            file_path=str(self.workflow_path),
        )

        # Get Alteryx version from root attribute
        if self.root is not None:
            metadata.alteryx_version = self.root.get('yxmdVer')

        # Find Properties element
        props = self.root.find('.//Properties') if self.root is not None else None
        if props is not None:
            # MetaInfo section
            meta_info = props.find('MetaInfo')
            if meta_info is not None:
                name_elem = meta_info.find('Name')
                if name_elem is not None and name_elem.text:
                    metadata.name = name_elem.text

                desc_elem = meta_info.find('Description')
                if desc_elem is not None and desc_elem.text:
                    metadata.description = desc_elem.text

                author_elem = meta_info.find('Author')
                if author_elem is not None and author_elem.text:
                    metadata.author = author_elem.text

            # Annotation (workflow-level comment)
            annotation = props.find('Annotation')
            if annotation is not None:
                anno_text = annotation.find('DefaultAnnotationText')
                if anno_text is not None and anno_text.text and not metadata.description:
                    metadata.description = anno_text.text

        return metadata

    def _parse_nodes(self) -> List[AlteryxNode]:
        """Extract all nodes/tools from the workflow."""
        nodes = []
        container_children = {}  # container_id -> [child_ids]

        nodes_elem = self.root.find('.//Nodes') if self.root is not None else None
        if nodes_elem is None:
            return nodes

        for node_elem in nodes_elem.findall('Node'):
            node = self._parse_single_node(node_elem)
            if node:
                nodes.append(node)

                # Check if this is a tool container - extract child tool IDs
                if node.category == ToolCategory.CONTAINER:
                    child_ids = self._extract_container_children(node_elem)
                    node.child_tool_ids = child_ids
                    container_children[node.tool_id] = child_ids

        # Set container_id on child nodes
        for container_id, child_ids in container_children.items():
            for child_id in child_ids:
                for node in nodes:
                    if node.tool_id == child_id:
                        node.container_id = container_id
                        break

        return nodes

    def _extract_container_children(self, node_elem: ET.Element) -> List[int]:
        """Extract child tool IDs from a tool container."""
        child_ids = []

        # Check ChildToolIds element (common format)
        child_tools_elem = node_elem.find('.//ChildToolIds')
        if child_tools_elem is not None and child_tools_elem.text:
            # Format: "1,2,3,4" or "1 2 3 4"
            text = child_tools_elem.text.strip()
            # Split by comma or whitespace
            for part in text.replace(',', ' ').split():
                try:
                    child_ids.append(int(part.strip()))
                except ValueError:
                    pass

        # Also check Configuration/ChildToolIds
        config_children = node_elem.find('.//Configuration/ChildToolIds')
        if config_children is not None and config_children.text:
            text = config_children.text.strip()
            for part in text.replace(',', ' ').split():
                try:
                    child_id = int(part.strip())
                    if child_id not in child_ids:
                        child_ids.append(child_id)
                except ValueError:
                    pass

        # Check Properties/ChildToolIds
        props_children = node_elem.find('.//Properties/ChildToolIds')
        if props_children is not None and props_children.text:
            text = props_children.text.strip()
            for part in text.replace(',', ' ').split():
                try:
                    child_id = int(part.strip())
                    if child_id not in child_ids:
                        child_ids.append(child_id)
                except ValueError:
                    pass

        return child_ids

    def _parse_single_node(self, node_elem: ET.Element) -> Optional[AlteryxNode]:
        """Parse a single Node element."""
        tool_id_str = node_elem.get('ToolID')
        if not tool_id_str:
            return None

        tool_id = int(tool_id_str)

        # Get GuiSettings for tool type
        gui_settings = node_elem.find('GuiSettings')
        plugin = gui_settings.get('Plugin', '') if gui_settings is not None else ''

        # Position
        position = (0, 0)
        if gui_settings is not None:
            pos_elem = gui_settings.find('Position')
            if pos_elem is not None:
                x = float(pos_elem.get('x', 0))
                y = float(pos_elem.get('y', 0))
                position = (x, y)

        # Determine if this is a macro (no Plugin attribute or specific macro indicators)
        is_macro = False
        macro_path = None

        if not plugin:
            is_macro = True
            # Try to find macro path in configuration
            macro_path = self._extract_macro_path(node_elem)
        elif 'Macro' in plugin:
            # It's a macro input/output tool
            pass

        # Get simple tool name and category
        simple_name = get_simple_name(plugin) if plugin else "Macro"
        category = get_category_from_plugin(plugin) if plugin else ToolCategory.MACRO

        # If it's a macro, override category
        if is_macro:
            category = ToolCategory.MACRO
            simple_name = self._get_macro_name(node_elem) or "Macro"

        # Create node
        node = AlteryxNode(
            tool_id=tool_id,
            tool_type=plugin,
            plugin_name=simple_name,
            category=category,
            position=position,
            is_macro=is_macro,
            macro_path=macro_path,
        )

        # Parse configuration
        config = self._parse_configuration(node_elem, node)
        node.configuration = config

        # Extract annotation
        node.annotation = self._extract_annotation(node_elem)

        return node

    def _extract_macro_path(self, node_elem: ET.Element) -> Optional[str]:
        """Extract the macro file path from a macro node."""
        # Check EngineSettings for Macro attribute
        engine_settings = node_elem.find('.//EngineSettings')
        if engine_settings is not None:
            macro_attr = engine_settings.get('Macro')
            if macro_attr:
                return macro_attr

        # Check GuiSettings for macro path
        gui_settings = node_elem.find('GuiSettings')
        if gui_settings is not None:
            # Some macros store path in Plugin-like attribute
            macro_path = gui_settings.get('Macro')
            if macro_path:
                return macro_path

        # Check Configuration for macro references
        config = node_elem.find('.//Configuration')
        if config is not None:
            macro_elem = config.find('Macro')
            if macro_elem is not None and macro_elem.text:
                return macro_elem.text

        return None

    def _get_macro_name(self, node_elem: ET.Element) -> Optional[str]:
        """Extract a display name for a macro node."""
        macro_path = self._extract_macro_path(node_elem)
        if macro_path:
            return Path(macro_path).stem

        # Try annotation
        annotation = self._extract_annotation(node_elem)
        if annotation:
            return annotation

        return None

    def _extract_annotation(self, node_elem: ET.Element) -> Optional[str]:
        """Extract the annotation/comment for a node."""
        # Check Properties/Annotation
        annotation = node_elem.find('.//Properties/Annotation')
        if annotation is not None:
            name = annotation.find('Name')
            if name is not None and name.text:
                return name.text

            default_text = annotation.find('DefaultAnnotationText')
            if default_text is not None and default_text.text:
                return default_text.text

        return None

    def _parse_configuration(self, node_elem: ET.Element, node: AlteryxNode) -> Dict[str, Any]:
        """Parse tool-specific configuration."""
        config = {}

        config_elem = node_elem.find('.//Configuration')
        if config_elem is None:
            return config

        # Store raw XML for reference
        config['_raw'] = ET.tostring(config_elem, encoding='unicode')

        # Extract based on tool type
        if node.category == ToolCategory.INPUT:
            self._parse_input_config(config_elem, node, config)
        elif node.category == ToolCategory.OUTPUT:
            self._parse_output_config(config_elem, node, config)
        elif node.plugin_name == "Filter":
            self._parse_filter_config(config_elem, node, config)
        elif node.plugin_name in ["Formula", "Multi-Field Formula"]:
            self._parse_formula_config(config_elem, node, config)
        elif node.plugin_name == "Join":
            self._parse_join_config(config_elem, node, config)
        elif node.plugin_name == "Summarize":
            self._parse_summarize_config(config_elem, node, config)
        elif node.plugin_name == "Select":
            self._parse_select_config(config_elem, node, config)
        elif node.plugin_name == "Sort":
            self._parse_sort_config(config_elem, node, config)
        elif node.plugin_name == "Union":
            self._parse_union_config(config_elem, node, config)

        return config

    def _parse_input_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse input tool configuration."""
        # File path
        file_elem = config_elem.find('.//File')
        if file_elem is not None:
            node.source_path = file_elem.text or file_elem.get('OutputFileName', '')
            config['file_path'] = node.source_path

        # Connection string
        conn_elem = config_elem.find('.//Connection')
        if conn_elem is not None:
            node.connection_string = conn_elem.text
            config['connection'] = node.connection_string

        # Table name
        table_elem = config_elem.find('.//Table')
        if table_elem is not None:
            node.table_name = table_elem.text
            config['table'] = node.table_name

        # SQL Query
        query_elem = config_elem.find('.//SQLStatement')
        if query_elem is not None and query_elem.text:
            node.sql_query = query_elem.text
            config['sql'] = node.sql_query

        # Also check for queries in different locations
        query_alt = config_elem.find('.//Query')
        if query_alt is not None and query_alt.text:
            node.sql_query = query_alt.text
            config['sql'] = node.sql_query

    def _parse_output_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse output tool configuration."""
        # File path
        file_elem = config_elem.find('.//File')
        if file_elem is not None:
            node.target_path = file_elem.text or file_elem.get('OutputFileName', '')
            config['file_path'] = node.target_path

        # Connection string
        conn_elem = config_elem.find('.//Connection')
        if conn_elem is not None:
            node.connection_string = conn_elem.text
            config['connection'] = node.connection_string

        # Table name
        table_elem = config_elem.find('.//Table')
        if table_elem is not None:
            node.table_name = table_elem.text
            config['table'] = node.table_name

    def _parse_filter_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse filter tool configuration."""
        expr_elem = config_elem.find('.//Expression')
        if expr_elem is not None and expr_elem.text:
            node.expression = expr_elem.text
            config['expression'] = node.expression

        # Simple mode filter
        mode_elem = config_elem.find('.//Mode')
        if mode_elem is not None and mode_elem.text == 'Simple':
            field = config_elem.find('.//Field')
            operator = config_elem.find('.//Operator')
            operands = config_elem.findall('.//Operand')

            if field is not None and operator is not None:
                expr = f"[{field.text}] {operator.text}"
                if operands:
                    values = [op.text for op in operands if op.text]
                    expr += f" {', '.join(values)}"
                node.expression = expr
                config['expression'] = expr

    def _parse_formula_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse formula tool configuration."""
        formulas = []

        for formula_field in config_elem.findall('.//FormulaField'):
            field_name = formula_field.get('field', '')
            expression = formula_field.get('expression', '')
            field_type = formula_field.get('type', '')

            formulas.append({
                'field': field_name,
                'expression': expression,
                'type': field_type
            })

        if formulas:
            config['formulas'] = formulas
            # Store combined expression
            node.expression = '; '.join([f"{f['field']} = {f['expression']}" for f in formulas])

    def _parse_join_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse join tool configuration."""
        join_info = config_elem.find('.//JoinInfo')
        if join_info is not None:
            connection = join_info.get('connection', '')
            config['join_connection'] = connection

        # Join by fields
        join_fields = []
        for jf in config_elem.findall('.//JoinByRecordPos') + config_elem.findall('.//JoinByFields'):
            if jf.tag == 'JoinByRecordPos' and jf.text == 'True':
                config['join_by_position'] = True
            else:
                # Find field pairs
                for field in jf.findall('.//Field'):
                    left = field.get('left', '')
                    right = field.get('right', '')
                    if left and right:
                        join_fields.append({'left': left, 'right': right})

        if join_fields:
            node.join_keys = [f"{jf['left']}={jf['right']}" for jf in join_fields]
            config['join_fields'] = join_fields

        # Join type
        select_join = config_elem.find('.//SelectJoinInfo')
        if select_join is not None:
            node.join_type = select_join.get('connection', 'Inner')

    def _parse_summarize_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse summarize tool configuration."""
        group_by = []
        aggregations = []

        for field in config_elem.findall('.//SummarizeField'):
            field_name = field.get('field', '')
            action = field.get('action', '')
            rename = field.get('rename', field_name)

            if action == 'GroupBy':
                group_by.append(field_name)
            else:
                aggregations.append({
                    'field': field_name,
                    'action': action,
                    'output_name': rename
                })

        node.group_by_fields = group_by
        node.aggregations = aggregations
        config['group_by'] = group_by
        config['aggregations'] = aggregations

    def _parse_select_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse select tool configuration."""
        selected_fields = []
        deselected_fields = []
        all_fields = []

        for field in config_elem.findall('.//SelectField'):
            field_name = field.get('field', '')
            selected = field.get('selected', 'True')
            rename = field.get('rename', '')

            if not field_name:
                continue

            # Track all fields for column lineage
            all_fields.append(field_name)

            if selected == 'True':
                if rename:
                    selected_fields.append(f"{field_name} AS {rename}")
                else:
                    selected_fields.append(field_name)
            else:
                # Track deselected fields
                deselected_fields.append(field_name)

        node.selected_fields = selected_fields
        config['selected_fields'] = selected_fields
        config['deselected_fields'] = deselected_fields
        config['all_fields'] = all_fields

    def _parse_sort_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse sort tool configuration."""
        sort_fields = []

        for field in config_elem.findall('.//SortInfo'):
            field_name = field.get('field', '')
            order = field.get('order', 'Ascending')

            if field_name:
                sort_fields.append({
                    'field': field_name,
                    'order': order
                })

        config['sort_fields'] = sort_fields

    def _parse_union_config(self, config_elem: ET.Element, node: AlteryxNode, config: Dict):
        """Parse union tool configuration."""
        mode = config_elem.find('.//Mode')
        if mode is not None:
            config['union_mode'] = mode.text  # 'ByName', 'ByPosition', etc.

    def _parse_connections(self) -> List[AlteryxConnection]:
        """Extract all connections between nodes."""
        connections = []

        conn_elem = self.root.find('.//Connections') if self.root is not None else None
        if conn_elem is None:
            return connections

        for conn in conn_elem.findall('Connection'):
            origin = conn.find('Origin')
            dest = conn.find('Destination')

            if origin is not None and dest is not None:
                origin_id = int(origin.get('ToolID', 0))
                origin_anchor = origin.get('Connection', 'Output')
                dest_id = int(dest.get('ToolID', 0))
                dest_anchor = dest.get('Connection', 'Input')

                connections.append(AlteryxConnection(
                    origin_id=origin_id,
                    origin_anchor=origin_anchor,
                    destination_id=dest_id,
                    destination_anchor=dest_anchor,
                ))

        return connections


def parse_workflow(file_path: str) -> AlteryxWorkflow:
    """Convenience function to parse a workflow."""
    parser = AlteryxParser()
    return parser.parse(file_path)
