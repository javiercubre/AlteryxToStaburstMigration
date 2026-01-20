"""
Documentation generator for Alteryx to Starburst/Trino migration.
Generates Markdown documentation with Mermaid diagrams.

Target Platform: Starburst (Trino-based)
"""
import os
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from models import (
    AlteryxWorkflow, AlteryxNode, MacroInfo, MedallionLayer, ToolCategory
)
from transformation_analyzer import TransformationAnalyzer
from macro_handler import MacroInventory


class DocumentationGenerator:
    """Generates Markdown documentation for Alteryx workflows."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories
        (self.output_dir / "workflows").mkdir(exist_ok=True)

    def generate_all(self, workflows: List[AlteryxWorkflow],
                     macro_inventory: Optional[MacroInventory] = None) -> None:
        """Generate all documentation for a list of workflows."""
        # Generate index
        self._generate_index(workflows, macro_inventory)

        # Generate per-workflow docs
        for workflow in workflows:
            self._generate_workflow_doc(workflow)

        # Generate sources inventory
        self._generate_sources_doc(workflows)

        # Generate targets inventory
        self._generate_targets_doc(workflows)

        # Generate macros doc
        if macro_inventory:
            self._generate_macros_doc(macro_inventory)

        # Generate medallion mapping
        self._generate_medallion_mapping(workflows)

        print(f"Documentation generated at: {self.output_dir}")

    def _generate_index(self, workflows: List[AlteryxWorkflow],
                        macro_inventory: Optional[MacroInventory]) -> None:
        """Generate the main index.md file."""
        content = [
            "# Alteryx to Starburst Migration Documentation",
            "",
            f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            "",
            "**Target Platform**: Starburst (Trino-based) with dbt",
            "",
            "## Overview",
            "",
            f"- **Total Workflows**: {len(workflows)}",
        ]

        # Count totals
        total_sources = sum(len(w.sources) for w in workflows)
        total_targets = sum(len(w.targets) for w in workflows)
        total_nodes = sum(len(w.nodes) for w in workflows)
        total_macros = len(set(m for w in workflows for m in w.macros_used))

        content.extend([
            f"- **Total Data Sources**: {total_sources}",
            f"- **Total Outputs**: {total_targets}",
            f"- **Total Tools/Nodes**: {total_nodes}",
            f"- **Unique Macros**: {total_macros}",
            "",
            "## Workflows",
            "",
            "| Workflow | Description | Sources | Outputs | Tools | Macros |",
            "|----------|-------------|---------|---------|-------|--------|",
        ])

        for wf in workflows:
            desc = (wf.metadata.description or "")[:50]
            if len(wf.metadata.description or "") > 50:
                desc += "..."
            content.append(
                f"| [{wf.metadata.name}](workflows/{wf.metadata.name}.md) "
                f"| {desc} "
                f"| {len(wf.sources)} "
                f"| {len(wf.targets)} "
                f"| {len(wf.nodes)} "
                f"| {len(wf.macros_used)} |"
            )

        content.extend([
            "",
            "## Quick Links",
            "",
            "- [All Data Sources](sources.md)",
            "- [All Output Targets](targets.md)",
            "- [Macro Inventory](macros.md)",
            "- [Medallion Architecture Mapping](medallion_mapping.md)",
            "",
            "---",
            "",
            "*This documentation was generated to assist with migrating Alteryx ETL workflows to Starburst (Trino) / dbt ELT architecture with medallion pattern.*",
        ])

        self._write_file(self.output_dir / "index.md", "\n".join(content))

    def _generate_workflow_doc(self, workflow: AlteryxWorkflow) -> None:
        """Generate documentation for a single workflow."""
        analyzer = TransformationAnalyzer(workflow)

        content = [
            f"# {workflow.metadata.name}",
            "",
        ]

        # Metadata
        content.extend([
            "## Overview",
            "",
            f"- **File**: `{workflow.metadata.file_path}`",
        ])

        if workflow.metadata.alteryx_version:
            content.append(f"- **Alteryx Version**: {workflow.metadata.alteryx_version}")
        if workflow.metadata.description:
            content.append(f"- **Description**: {workflow.metadata.description}")
        if workflow.metadata.author:
            content.append(f"- **Author**: {workflow.metadata.author}")

        content.extend([
            f"- **Total Tools**: {len(workflow.nodes)}",
            f"- **Data Sources**: {len(workflow.sources)}",
            f"- **Outputs**: {len(workflow.targets)}",
            f"- **Macros Used**: {len(workflow.macros_used)}",
            "",
        ])

        # Data Flow Diagram
        content.extend([
            "## Data Flow Diagram",
            "",
            "```mermaid",
            self._generate_mermaid_diagram(workflow),
            "```",
            "",
        ])

        # Sources Table
        content.extend([
            "## Data Sources",
            "",
        ])

        sources = analyzer.get_source_inventory()
        if sources:
            content.extend([
                "| Source | Type | Path/Connection |",
                "|--------|------|-----------------|",
            ])
            for src in sources:
                path = src['path']
                if src['connection']:
                    path = f"{src['connection']} / {path}"
                content.append(f"| {src['name']} | {src['type']} | `{path}` |")
        else:
            content.append("*No data sources found*")

        content.append("")

        # Targets Table
        content.extend([
            "## Output Targets",
            "",
        ])

        targets = analyzer.get_target_inventory()
        if targets:
            content.extend([
                "| Target | Type | Path/Connection |",
                "|--------|------|-----------------|",
            ])
            for tgt in targets:
                path = tgt['path']
                if tgt['connection']:
                    path = f"{tgt['connection']} / {path}"
                content.append(f"| {tgt['name']} | {tgt['type']} | `{path}` |")
        else:
            content.append("*No output targets found*")

        content.append("")

        # Transformation Steps
        content.extend([
            "## Transformation Steps",
            "",
        ])

        steps = analyzer.get_ordered_transformations()
        for step in steps:
            content.append(f"{step.order}. **{step.tool_name}** (Tool #{step.tool_id})")
            content.append(f"   - {step.description}")
            if step.expression:
                expr_preview = step.expression[:100]
                if len(step.expression) > 100:
                    expr_preview += "..."
                content.append(f"   - Expression: `{expr_preview}`")
            content.append("")

        # Macros Used
        if workflow.macros_used:
            content.extend([
                "## Macros Used",
                "",
            ])
            for macro in workflow.macros_used:
                status = "Found" if macro not in workflow.missing_macros else "**MISSING**"
                content.append(f"- `{macro}` - {status}")

            content.append("")

        # Suggested DBT Structure
        content.extend([
            "## Suggested DBT Structure",
            "",
        ])

        medallion = analyzer.suggest_medallion_mapping()

        # Bronze Layer
        bronze_nodes = medallion.get(MedallionLayer.BRONZE.value, [])
        if bronze_nodes:
            content.append("### Bronze Layer (Staging)")
            content.append("")
            for node in bronze_nodes:
                model_name = self._suggest_model_name(node, MedallionLayer.BRONZE)
                content.append(f"- `{model_name}` - {node.get_display_name()}")
            content.append("")

        # Silver Layer
        silver_nodes = medallion.get(MedallionLayer.SILVER.value, [])
        if silver_nodes:
            content.append("### Silver Layer (Intermediate)")
            content.append("")
            for node in silver_nodes[:10]:  # Limit to prevent huge lists
                model_name = self._suggest_model_name(node, MedallionLayer.SILVER)
                content.append(f"- `{model_name}` - {node.get_display_name()}")
            if len(silver_nodes) > 10:
                content.append(f"- *...and {len(silver_nodes) - 10} more transformations*")
            content.append("")

        # Gold Layer
        gold_nodes = medallion.get(MedallionLayer.GOLD.value, [])
        if gold_nodes:
            content.append("### Gold Layer (Marts)")
            content.append("")
            for node in gold_nodes:
                model_name = self._suggest_model_name(node, MedallionLayer.GOLD)
                content.append(f"- `{model_name}` - {node.get_display_name()}")
            content.append("")

        # DBT SQL Hints
        content.extend([
            "## Trino SQL Translation Hints",
            "",
            "Key transformations with Trino SQL equivalents (for Starburst):",
            "",
        ])

        for step in steps:
            if step.dbt_hint and step.dbt_hint != "-- Custom logic required":
                content.append(f"### Tool #{step.tool_id}: {step.tool_name}")
                content.append("")
                content.append("```sql")
                content.append(step.dbt_hint)
                content.append("```")
                content.append("")

        content.extend([
            "---",
            "",
            f"[Back to Index](../index.md)",
        ])

        self._write_file(
            self.output_dir / "workflows" / f"{workflow.metadata.name}.md",
            "\n".join(content)
        )

    def _generate_mermaid_diagram(self, workflow: AlteryxWorkflow) -> str:
        """Generate a Mermaid flowchart for the workflow."""
        lines = ["graph LR"]

        # Create node definitions
        for node in workflow.nodes:
            node_id = f"N{node.tool_id}"
            label = node.get_display_name()

            # Escape special characters
            label = label.replace('"', "'").replace("[", "(").replace("]", ")")

            # Truncate long labels
            if len(label) > 40:
                label = label[:37] + "..."

            # Style based on category
            if node.category == ToolCategory.INPUT:
                lines.append(f'    {node_id}[("{label}")]')
            elif node.category == ToolCategory.OUTPUT:
                lines.append(f'    {node_id}[["{label}"]]')
            elif node.is_macro:
                lines.append(f'    {node_id}{{{{"{label}"}}}}')
            else:
                lines.append(f'    {node_id}["{label}"]')

        # Create connections
        for conn in workflow.connections:
            origin = f"N{conn.origin_id}"
            dest = f"N{conn.destination_id}"

            # Add anchor info if not standard
            label = ""
            if conn.origin_anchor not in ["Output", "Output1"]:
                label = conn.origin_anchor

            if label:
                lines.append(f'    {origin} -->|{label}| {dest}')
            else:
                lines.append(f'    {origin} --> {dest}')

        return "\n".join(lines)

    def _generate_sources_doc(self, workflows: List[AlteryxWorkflow]) -> None:
        """Generate sources.md with all data sources."""
        content = [
            "# Data Sources Inventory",
            "",
            "Complete inventory of all data sources across workflows.",
            "",
            "| Source | Type | Path/Connection | Used In |",
            "|--------|------|-----------------|---------|",
        ]

        # Collect all sources
        sources_map: Dict[str, List[str]] = {}

        for wf in workflows:
            analyzer = TransformationAnalyzer(wf)
            for src in analyzer.get_source_inventory():
                key = f"{src['type']}|{src['path']}"
                if key not in sources_map:
                    sources_map[key] = {
                        'name': src['name'],
                        'type': src['type'],
                        'path': src['path'],
                        'workflows': []
                    }
                sources_map[key]['workflows'].append(wf.metadata.name)

        for source in sources_map.values():
            workflows_str = ", ".join(source['workflows'][:3])
            if len(source['workflows']) > 3:
                workflows_str += f" (+{len(source['workflows']) - 3})"
            content.append(
                f"| {source['name']} | {source['type']} | `{source['path']}` | {workflows_str} |"
            )

        content.extend([
            "",
            f"**Total Unique Sources**: {len(sources_map)}",
            "",
            "---",
            "",
            "[Back to Index](index.md)",
        ])

        self._write_file(self.output_dir / "sources.md", "\n".join(content))

    def _generate_targets_doc(self, workflows: List[AlteryxWorkflow]) -> None:
        """Generate targets.md with all output targets."""
        content = [
            "# Output Targets Inventory",
            "",
            "Complete inventory of all output targets across workflows.",
            "",
            "| Target | Type | Path/Connection | Used In |",
            "|--------|------|-----------------|---------|",
        ]

        # Collect all targets
        targets_map: Dict[str, Dict] = {}

        for wf in workflows:
            analyzer = TransformationAnalyzer(wf)
            for tgt in analyzer.get_target_inventory():
                key = f"{tgt['type']}|{tgt['path']}"
                if key not in targets_map:
                    targets_map[key] = {
                        'name': tgt['name'],
                        'type': tgt['type'],
                        'path': tgt['path'],
                        'workflows': []
                    }
                targets_map[key]['workflows'].append(wf.metadata.name)

        for target in targets_map.values():
            workflows_str = ", ".join(target['workflows'][:3])
            if len(target['workflows']) > 3:
                workflows_str += f" (+{len(target['workflows']) - 3})"
            content.append(
                f"| {target['name']} | {target['type']} | `{target['path']}` | {workflows_str} |"
            )

        content.extend([
            "",
            f"**Total Unique Targets**: {len(targets_map)}",
            "",
            "---",
            "",
            "[Back to Index](index.md)",
        ])

        self._write_file(self.output_dir / "targets.md", "\n".join(content))

    def _generate_macros_doc(self, macro_inventory: MacroInventory) -> None:
        """Generate macros.md with macro inventory."""
        content = [
            "# Macro Inventory",
            "",
        ]

        summary = macro_inventory.get_summary()
        content.extend([
            "## Summary",
            "",
            f"- **Total Macros**: {summary['total_macros']}",
            f"- **Found**: {summary['found']}",
            f"- **Missing**: {summary['missing']}",
            f"- **Shared (used by multiple workflows)**: {summary['shared']}",
            "",
        ])

        # Missing macros (highlight these)
        missing = macro_inventory.get_missing_macros()
        if missing:
            content.extend([
                "## Missing Macros",
                "",
                "These macros could not be found and need to be located:",
                "",
            ])
            for macro in missing:
                workflows = macro_inventory.usage.get(macro.name, [])
                content.append(f"- **{macro.name}**")
                content.append(f"  - Original path: `{macro.file_path}`")
                content.append(f"  - Used in: {', '.join(workflows)}")
                content.append("")

        # Found macros
        found_macros = [m for m in macro_inventory.macros.values() if m.found]
        if found_macros:
            content.extend([
                "## Found Macros",
                "",
                "| Macro | Path | Inputs | Outputs | Used In |",
                "|-------|------|--------|---------|---------|",
            ])

            for macro in found_macros:
                workflows = macro_inventory.usage.get(macro.name, [])
                inputs = len(macro.inputs)
                outputs = len(macro.outputs)
                workflows_str = ", ".join(workflows[:3])
                if len(workflows) > 3:
                    workflows_str += f" (+{len(workflows) - 3})"

                content.append(
                    f"| {macro.name} | `{macro.resolved_path}` | {inputs} | {outputs} | {workflows_str} |"
                )

            content.append("")

        # Shared macros
        shared = macro_inventory.get_shared_macros()
        if shared:
            content.extend([
                "## Shared Macros",
                "",
                "Macros used by multiple workflows (candidates for DBT macros):",
                "",
            ])
            for macro in shared:
                workflows = macro_inventory.usage.get(macro.name, [])
                content.append(f"- **{macro.name}** - used by {len(workflows)} workflows")
                for wf in workflows[:5]:
                    content.append(f"  - {wf}")
                if len(workflows) > 5:
                    content.append(f"  - *...and {len(workflows) - 5} more*")
                content.append("")

        content.extend([
            "---",
            "",
            "[Back to Index](index.md)",
        ])

        self._write_file(self.output_dir / "macros.md", "\n".join(content))

    def _generate_medallion_mapping(self, workflows: List[AlteryxWorkflow]) -> None:
        """Generate medallion_mapping.md with layer suggestions."""
        content = [
            "# Medallion Architecture Mapping",
            "",
            "Suggested DBT model organization following the medallion pattern.",
            "",
            "## Architecture Overview",
            "",
            "```",
            "Bronze (Staging)     Silver (Intermediate)     Gold (Marts)",
            "----------------     --------------------     ------------",
            "stg_*                int_*                    fct_* / dim_*",
            "                                              ",
            "Raw data from        Cleaned, joined,         Business-ready",
            "sources              transformed data         aggregations",
            "```",
            "",
        ]

        # Collect all unique sources for bronze
        bronze_sources = set()
        silver_transforms = []
        gold_outputs = []

        for wf in workflows:
            analyzer = TransformationAnalyzer(wf)
            medallion = analyzer.suggest_medallion_mapping()

            for node in medallion.get(MedallionLayer.BRONZE.value, []):
                if node.source_path or node.table_name:
                    source_name = node.table_name or Path(node.source_path or "unknown").stem
                    bronze_sources.add((source_name, node.get_display_name(), wf.metadata.name))

            for node in medallion.get(MedallionLayer.SILVER.value, []):
                silver_transforms.append((node, wf.metadata.name))

            for node in medallion.get(MedallionLayer.GOLD.value, []):
                gold_outputs.append((node, wf.metadata.name))

        # Bronze Layer
        content.extend([
            "## Bronze Layer (Staging Models)",
            "",
            "Create staging models for each data source:",
            "",
            "| Suggested Model | Source | Workflow |",
            "|-----------------|--------|----------|",
        ])

        for source_name, display_name, wf_name in sorted(bronze_sources):
            model_name = f"stg_{self._sanitize_name(source_name)}"
            content.append(f"| `{model_name}` | {display_name} | {wf_name} |")

        content.append("")

        # Silver Layer
        content.extend([
            "## Silver Layer (Intermediate Models)",
            "",
            "Key transformations to implement as intermediate models:",
            "",
        ])

        # Group by transformation type
        transform_types = {}
        for node, wf_name in silver_transforms:
            key = node.plugin_name
            if key not in transform_types:
                transform_types[key] = []
            transform_types[key].append((node, wf_name))

        for transform_type, nodes in transform_types.items():
            content.append(f"### {transform_type} Operations")
            content.append("")
            for node, wf_name in nodes[:5]:
                model_name = f"int_{self._sanitize_name(node.get_display_name())}"
                content.append(f"- `{model_name}` ({wf_name})")
            if len(nodes) > 5:
                content.append(f"- *...and {len(nodes) - 5} more*")
            content.append("")

        # Gold Layer
        content.extend([
            "## Gold Layer (Marts)",
            "",
            "Final output models:",
            "",
            "| Suggested Model | Output | Workflow |",
            "|-----------------|--------|----------|",
        ])

        for node, wf_name in gold_outputs:
            prefix = "fct_" if node.plugin_name == "Summarize" else "dim_"
            model_name = f"{prefix}{self._sanitize_name(node.get_display_name())}"
            content.append(f"| `{model_name}` | {node.get_display_name()} | {wf_name} |")

        content.extend([
            "",
            "## Recommended DBT Project Structure",
            "",
            "```",
            "models/",
            "├── staging/           # Bronze layer",
            "│   ├── _staging.yml   # Source definitions",
            "│   └── stg_*.sql",
            "├── intermediate/      # Silver layer",
            "│   └── int_*.sql",
            "└── marts/             # Gold layer",
            "    ├── core/",
            "    │   └── fct_*.sql",
            "    └── dimensions/",
            "        └── dim_*.sql",
            "```",
            "",
            "---",
            "",
            "[Back to Index](index.md)",
        ])

        self._write_file(self.output_dir / "medallion_mapping.md", "\n".join(content))

    def _suggest_model_name(self, node: AlteryxNode, layer: MedallionLayer) -> str:
        """Suggest a DBT model name for a node."""
        base_name = node.table_name or node.annotation or node.plugin_name

        if node.source_path:
            base_name = Path(node.source_path).stem
        elif node.target_path:
            base_name = Path(node.target_path).stem

        sanitized = self._sanitize_name(base_name)

        if layer == MedallionLayer.BRONZE:
            return f"stg_{sanitized}"
        elif layer == MedallionLayer.SILVER:
            return f"int_{sanitized}"
        else:
            prefix = "fct_" if node.plugin_name == "Summarize" else "dim_"
            return f"{prefix}{sanitized}"

    def _sanitize_name(self, name: str) -> str:
        """Sanitize a name for use as a DBT model name."""
        import re
        # Remove special characters, replace spaces with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        # Lowercase
        sanitized = sanitized.lower()
        # Truncate if too long
        if len(sanitized) > 50:
            sanitized = sanitized[:50]

        return sanitized or "unknown"

    def _write_file(self, path: Path, content: str) -> None:
        """Write content to a file."""
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
