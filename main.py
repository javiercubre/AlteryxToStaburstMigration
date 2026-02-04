#!/usr/bin/env python3
"""
Alteryx to DBT Documentation Generator

A tool for parsing Alteryx workflows and generating documentation
to facilitate migration to Trino/DBT ELT architecture.

Usage:
    python main.py analyze <path> [options]
    python main.py generate-s3-config [options]
    python main.py --help

Examples:
    python main.py analyze .                          # Analyze current directory
    python main.py analyze ./workflows --recursive    # Analyze recursively
    python main.py analyze . --output ./docs          # Specify output directory
    python main.py analyze . --generate-dbt ./dbt     # Generate DBT scaffolding
    python main.py analyze . --macro-dir ./macros     # Specify macro directory

    # S3 mappings generation
    python main.py generate-s3-config                        # Interactive wizard
    python main.py generate-s3-config --scan ./workflows     # Discover sources first
    python main.py generate-s3-config -o my_mappings.json    # Custom output file
    python main.py generate-s3-config --bucket my-lake       # Pre-set bucket
"""
import argparse
import sys
from pathlib import Path
from typing import List, Optional

from alteryx_parser import AlteryxParser
from macro_handler import MacroResolver, MacroInventory
from doc_generator import DocumentationGenerator
from dbt_generator import DBTGenerator
from s3_config import S3ConfigResolver
from s3_mappings_generator import S3MappingsGenerator
from models import AlteryxWorkflow
from logging_config import setup_logging, get_logger

# Module logger
logger = get_logger(__name__)


def find_workflows(path: Path, recursive: bool = False) -> List[Path]:
    """Find all Alteryx workflow files in a path."""
    workflows = []

    if path.is_file():
        if path.suffix.lower() in ['.yxmd', '.yxmc', '.yxwz']:
            workflows.append(path)
    elif path.is_dir():
        pattern = '**/*.yxmd' if recursive else '*.yxmd'
        workflows.extend(path.glob(pattern))

        # Also find .yxmc (macro) files that might be standalone
        macro_pattern = '**/*.yxmc' if recursive else '*.yxmc'
        workflows.extend(path.glob(macro_pattern))

    return sorted(workflows)


def analyze(args) -> int:
    """Main analyze command."""
    # Setup logging based on CLI arguments
    setup_logging(
        verbose=args.verbose,
        quiet=args.quiet,
        log_file=args.log_file
    )

    target_path = Path(args.path).resolve()

    if not target_path.exists():
        logger.error(f"Path does not exist: {target_path}")
        return 1

    # Find workflows
    logger.info(f"Scanning for Alteryx workflows in: {target_path}")
    workflow_files = find_workflows(target_path, args.recursive)

    if not workflow_files:
        logger.error("No Alteryx workflow files (.yxmd) found.")
        return 1

    logger.info(f"Found {len(workflow_files)} workflow file(s)")

    # Setup macro resolver
    macro_resolver = MacroResolver(
        interactive=not args.non_interactive,
        skip_all=args.non_interactive,
    )

    # Add macro directories
    for macro_dir in args.macro_dir or []:
        macro_resolver.add_search_directory(macro_dir)

    # Setup S3 config resolver (V2)
    s3_resolver = S3ConfigResolver(
        interactive=not args.non_interactive,
        skip_all=args.non_interactive,
        default_bucket=args.s3_bucket,
        default_region=args.s3_region,
        default_endpoint=args.s3_endpoint,
    )

    # Load S3 config file if provided
    if args.s3_config:
        try:
            s3_resolver.load_from_file(args.s3_config)
        except FileNotFoundError:
            logger.error(f"S3 config file not found: {args.s3_config}")
            return 1
        except Exception as e:
            logger.error(f"Error loading S3 config: {e}")
            return 1

    # Parse workflows
    workflows: List[AlteryxWorkflow] = []
    macro_inventory = MacroInventory()
    parser = AlteryxParser()

    for wf_path in workflow_files:
        logger.info(f"\nParsing: {wf_path.name}")
        try:
            workflow = parser.parse(str(wf_path))
            workflows.append(workflow)

            logger.info(f"  - {len(workflow.nodes)} tools")
            logger.info(f"  - {len(workflow.sources)} sources")
            logger.info(f"  - {len(workflow.targets)} targets")
            logger.debug(f"  - {len(workflow.macros_used)} macros referenced")

            # Resolve macros
            if workflow.macros_used:
                macro_infos = macro_resolver.resolve_macros(workflow)
                for macro_path, macro_info in macro_infos.items():
                    macro_inventory.add_macro(macro_info, workflow.metadata.name)

                if workflow.missing_macros:
                    logger.warning(f"  - {len(workflow.missing_macros)} macros not found")

        except Exception as e:
            logger.error(f"  Error parsing {wf_path}: {e}")
            if args.verbose:
                import traceback
                logger.debug(traceback.format_exc())

    if not workflows:
        logger.error("\nNo workflows were successfully parsed.")
        return 1

    # Resolve S3 sources if S3 bucket or config is provided (V2)
    if args.s3_bucket or args.s3_config:
        logger.info("\nResolving S3 source mappings...")
        for workflow in workflows:
            # Collect sources from workflow
            sources = []
            for source_node in workflow.sources:
                source_name = source_node.source_path or source_node.table_name or f"source_{source_node.tool_id}"
                sources.append({
                    'name': Path(source_name).name if source_name else f"source_{source_node.tool_id}",
                    'path': source_name,
                    'node_id': source_node.tool_id,
                })

            if sources:
                s3_mappings = s3_resolver.resolve_sources_batch(
                    sources,
                    workflow_name=workflow.metadata.name
                )
                logger.debug(f"  Resolved {len(s3_mappings)} S3 mappings for {workflow.metadata.name}")

    # Determine output directory
    output_dir = Path(args.output) if args.output else target_path / "alteryx_docs"

    # Generate DBT scaffolding first if requested (to collect TODOs)
    dbt_todos = None
    if args.generate_dbt:
        dbt_dir = Path(args.generate_dbt)
        logger.info(f"\nGenerating DBT project to: {dbt_dir}")
        # Pass interactive flag (inverse of non_interactive)
        dbt_generator = DBTGenerator(
            str(dbt_dir),
            interactive=not args.non_interactive,
            default_schema=args.default_schema,
            s3_resolver=s3_resolver if (args.s3_bucket or args.s3_config) else None
        )
        # Pass macro_inventory for reusable macro generation
        dbt_generator.generate(workflows, macro_inventory)
        # Collect TODOs for documentation
        dbt_todos = dbt_generator.todos

        # Validate generated SQL if requested (HIGH-05 fix)
        if args.validate:
            logger.info("\nValidating generated SQL...")
            validation_result = dbt_generator.validate_sql()
            if validation_result['success']:
                logger.info(f"  Validation passed: {validation_result['models_validated']} models validated")
            else:
                logger.error(f"  Validation failed: {validation_result['error']}")
                if args.verbose and validation_result.get('details'):
                    logger.debug(f"  Details: {validation_result['details']}")

    # Generate documentation (including TODO guide if DBT was generated)
    logger.info(f"\nGenerating documentation to: {output_dir}")
    doc_generator = DocumentationGenerator(str(output_dir))
    doc_generator.generate_all(workflows, macro_inventory, dbt_todos)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Workflows analyzed: {len(workflows)}")
    logger.info(f"Total tools: {sum(len(w.nodes) for w in workflows)}")
    logger.info(f"Total sources: {sum(len(w.sources) for w in workflows)}")
    logger.info(f"Total outputs: {sum(len(w.targets) for w in workflows)}")

    macro_summary = macro_inventory.get_summary()
    logger.info(f"Macros found: {macro_summary['found']}")
    logger.info(f"Macros missing: {macro_summary['missing']}")

    # S3 summary (V2)
    if args.s3_bucket or args.s3_config:
        s3_summary = s3_resolver.get_summary()
        logger.info(f"S3 sources mapped: {s3_summary['resolved']}")
        if s3_summary['skipped'] > 0:
            logger.info(f"S3 sources skipped: {s3_summary['skipped']}")
        if s3_summary['todos'] > 0:
            logger.warning(f"S3 sources pending: {s3_summary['todos']} (see TODOs)")

    logger.info(f"\nDocumentation: {output_dir / 'index.md'}")
    if args.generate_dbt:
        logger.info(f"DBT Project: {Path(args.generate_dbt) / 'dbt_project.yml'}")

    return 0


def generate_s3_config(args) -> int:
    """Generate S3 mappings configuration interactively."""
    # Setup logging
    setup_logging(
        verbose=args.verbose,
        quiet=args.quiet,
        log_file=getattr(args, 'log_file', None)
    )

    # Check for non-interactive mode
    if args.non_interactive:
        logger.error("The generate-s3-config command requires interactive mode.")
        logger.error("Remove the --non-interactive flag to use this command.")
        return 1

    # Collect workflow paths if provided
    workflow_paths = []
    if args.scan:
        for path_str in args.scan:
            path = Path(path_str).resolve()
            if path.exists():
                workflow_paths.append(str(path))
            else:
                logger.warning(f"Path not found: {path_str}")

    # Initialize generator with any provided defaults
    generator = S3MappingsGenerator(
        default_bucket=args.bucket,
        default_region=args.region,
        default_format=args.format,
        default_endpoint=args.endpoint,
    )

    # Load existing config if provided (for extending)
    if args.extend:
        try:
            generator.load_from_file(args.extend)
            logger.info(f"Loaded existing configuration from: {args.extend}")
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {args.extend}")
            return 1
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return 1

    # Run the interactive wizard
    output_path = args.output or "s3_mappings.json"
    success = generator.run_interactive(output_path, workflow_paths)

    return 0 if success else 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Alteryx to DBT Documentation Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s analyze .                           Analyze workflows in current directory
  %(prog)s analyze ./workflows -r              Analyze recursively
  %(prog)s analyze . -o ./docs                 Output to specific directory
  %(prog)s analyze . --generate-dbt ./dbt      Also generate DBT scaffolding
  %(prog)s analyze . --macro-dir ./macros      Specify macro search directory
  %(prog)s analyze . --non-interactive         Skip prompts for missing macros

  %(prog)s generate-s3-config                  Interactive S3 config wizard
  %(prog)s generate-s3-config --scan ./wf      Scan workflows for sources first
  %(prog)s generate-s3-config --bucket my-lake Pre-set default bucket
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Analyze command
    analyze_parser = subparsers.add_parser(
        'analyze',
        help='Analyze Alteryx workflows and generate documentation'
    )

    analyze_parser.add_argument(
        'path',
        help='Path to workflow file or directory containing workflows'
    )

    analyze_parser.add_argument(
        '-r', '--recursive',
        action='store_true',
        help='Recursively search for workflows in subdirectories'
    )

    analyze_parser.add_argument(
        '-o', '--output',
        help='Output directory for documentation (default: <path>/alteryx_docs)'
    )

    analyze_parser.add_argument(
        '--generate-dbt',
        metavar='DIR',
        help='Also generate DBT project scaffolding to specified directory'
    )

    analyze_parser.add_argument(
        '--macro-dir',
        action='append',
        metavar='DIR',
        help='Directory to search for macros (can be specified multiple times)'
    )

    analyze_parser.add_argument(
        '--non-interactive',
        action='store_true',
        help='Skip interactive prompts for missing macros'
    )

    analyze_parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose/debug output'
    )

    analyze_parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Suppress info messages, only show warnings and errors'
    )

    analyze_parser.add_argument(
        '--log-file',
        metavar='FILE',
        help='Write logs to specified file (in addition to console)'
    )

    analyze_parser.add_argument(
        '--default-schema',
        default='raw',
        metavar='NAME',
        help='Default schema name for sources without explicit schema (default: raw)'
    )

    analyze_parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate generated SQL by running dbt compile (requires dbt to be installed)'
    )

    # S3 source integration arguments (V2)
    analyze_parser.add_argument(
        '--s3-config',
        metavar='FILE',
        help='JSON file with S3 source mappings'
    )

    analyze_parser.add_argument(
        '--s3-bucket',
        metavar='NAME',
        help='Default S3 bucket for all sources'
    )

    analyze_parser.add_argument(
        '--s3-region',
        default='us-east-1',
        metavar='REGION',
        help='Default AWS region for S3 sources (default: us-east-1)'
    )

    analyze_parser.add_argument(
        '--s3-endpoint',
        metavar='URL',
        help='S3-compatible endpoint URL (for MinIO, etc.)'
    )

    # Generate S3 config command
    s3_config_parser = subparsers.add_parser(
        'generate-s3-config',
        help='Interactively generate an S3 mappings configuration file'
    )

    s3_config_parser.add_argument(
        '-o', '--output',
        metavar='FILE',
        default='s3_mappings.json',
        help='Output file path (default: s3_mappings.json)'
    )

    s3_config_parser.add_argument(
        '--scan',
        action='append',
        metavar='PATH',
        help='Scan workflow files/directories for sources (can be specified multiple times)'
    )

    s3_config_parser.add_argument(
        '--extend',
        metavar='FILE',
        help='Extend an existing S3 config file with additional mappings'
    )

    s3_config_parser.add_argument(
        '--bucket',
        metavar='NAME',
        help='Pre-set default S3 bucket name'
    )

    s3_config_parser.add_argument(
        '--region',
        default='us-east-1',
        metavar='REGION',
        help='Pre-set default AWS region (default: us-east-1)'
    )

    s3_config_parser.add_argument(
        '--format',
        default='parquet',
        choices=['parquet', 'csv', 'json', 'orc', 'avro'],
        help='Pre-set default file format (default: parquet)'
    )

    s3_config_parser.add_argument(
        '--endpoint',
        metavar='URL',
        help='Pre-set S3-compatible endpoint URL (for MinIO, etc.)'
    )

    s3_config_parser.add_argument(
        '--non-interactive',
        action='store_true',
        help='Not supported for this command (will show error)'
    )

    s3_config_parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose/debug output'
    )

    s3_config_parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Suppress info messages, only show warnings and errors'
    )

    # Parse arguments
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == 'analyze':
        return analyze(args)

    if args.command == 'generate-s3-config':
        return generate_s3_config(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())
