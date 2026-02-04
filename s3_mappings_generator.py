"""
Interactive S3 Mappings Generator.

Provides a CLI wizard for creating s3_mappings.json configuration files
by scanning Alteryx workflows and prompting users for S3 locations.
"""
import json
import sys
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Tuple

from models import S3SourceConfig
from logging_config import get_logger

# Module logger
logger = get_logger(__name__)


@dataclass
class S3MappingsGenerator:
    """Interactive generator for S3 mappings configuration files.

    Scans Alteryx workflows to discover data sources, then guides the user
    through mapping each source to an S3 location.
    Uses user/password authentication for S3 connections.
    """

    # Default settings
    default_bucket: Optional[str] = None
    default_format: str = "parquet"
    default_endpoint: Optional[str] = None
    default_s3_user: Optional[str] = None  # S3 access key / username
    default_s3_password: Optional[str] = None  # S3 secret key / password

    # Generated mappings
    mappings: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Discovered sources (source_name -> source_info)
    discovered_sources: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def run_interactive(self, output_path: str = "s3_mappings.json",
                        workflow_paths: Optional[List[str]] = None) -> bool:
        """Run the interactive generator wizard.

        Args:
            output_path: Path to save the generated JSON file
            workflow_paths: Optional list of workflow paths to scan for sources

        Returns:
            True if configuration was saved successfully
        """
        if not sys.stdin.isatty():
            logger.error("Interactive mode requires a terminal. Use --non-interactive mode instead.")
            return False

        self._print_header()

        # Step 1: Configure defaults
        if not self._configure_defaults():
            return False

        # Step 2: Discover sources from workflows (if provided)
        if workflow_paths:
            self._discover_sources(workflow_paths)

        # Step 3: Add mappings
        if not self._add_mappings_interactive():
            return False

        # Step 4: Add wildcard patterns
        if not self._add_wildcard_patterns():
            return False

        # Step 5: Review and save
        return self._review_and_save(output_path)

    def _print_header(self) -> None:
        """Print welcome header."""
        print("\n" + "=" * 70)
        print("  S3 MAPPINGS GENERATOR")
        print("  Interactive wizard for creating s3_mappings.json")
        print("=" * 70)
        print("\nThis wizard will help you create an S3 mappings configuration file")
        print("for migrating Alteryx workflows to Starburst/DBT.\n")

    def _configure_defaults(self) -> bool:
        """Configure default settings interactively."""
        print("-" * 70)
        print("STEP 1: Configure Default Settings")
        print("-" * 70)
        print("\nThese defaults will be applied to all mappings unless overridden.\n")

        try:
            # Default bucket
            bucket = input(f"Default S3 bucket name [{self.default_bucket or 'my-data-lake'}]: ").strip()
            self.default_bucket = bucket or self.default_bucket or "my-data-lake"

            # Default format
            print("\nSupported formats: parquet, csv, json, orc, avro")
            fmt = input(f"Default file format [{self.default_format}]: ").strip().lower()
            if fmt in ['parquet', 'csv', 'json', 'orc', 'avro']:
                self.default_format = fmt

            # S3-compatible endpoint
            print("\nFor S3-compatible services (MinIO, etc.), enter the endpoint URL.")
            print("Leave blank for AWS S3.")
            endpoint = input("S3 endpoint URL []: ").strip()
            if endpoint:
                self.default_endpoint = endpoint

            # S3 credentials
            print("\nS3 Authentication (Access Key / Secret Key)")
            s3_user = input(f"S3 User/Access Key [{self.default_s3_user or ''}]: ").strip()
            if s3_user:
                self.default_s3_user = s3_user

            s3_password = input(f"S3 Password/Secret Key [{self.default_s3_password and '****' or ''}]: ").strip()
            if s3_password:
                self.default_s3_password = s3_password

            print(f"\n[OK] Defaults configured:")
            print(f"     Bucket: {self.default_bucket}")
            print(f"     Format: {self.default_format}")
            if self.default_endpoint:
                print(f"     Endpoint: {self.default_endpoint}")
            if self.default_s3_user:
                print(f"     S3 User: {self.default_s3_user}")
                print(f"     S3 Password: {'****' if self.default_s3_password else '(not set)'}")
            print()

            return True

        except (KeyboardInterrupt, EOFError):
            print("\n\nCancelled.")
            return False

    def _discover_sources(self, workflow_paths: List[str]) -> None:
        """Discover data sources from workflow files."""
        print("-" * 70)
        print("Discovering sources from workflows...")
        print("-" * 70 + "\n")

        # Import parser here to avoid circular imports
        from alteryx_parser import AlteryxParser

        parser = AlteryxParser()

        for wf_path_str in workflow_paths:
            wf_path = Path(wf_path_str)

            if wf_path.is_dir():
                # Find all workflow files in directory
                files = list(wf_path.glob("**/*.yxmd")) + list(wf_path.glob("**/*.yxmc"))
            else:
                files = [wf_path] if wf_path.exists() else []

            for file_path in files:
                try:
                    workflow = parser.parse(str(file_path))

                    for source in workflow.sources:
                        source_name = self._get_source_name(source)
                        if source_name and source_name not in self.discovered_sources:
                            self.discovered_sources[source_name] = {
                                'path': source.source_path,
                                'table': source.table_name,
                                'connection': source.connection_string,
                                'workflow': workflow.metadata.name,
                                'tool_type': source.plugin_name,
                            }

                except Exception as e:
                    logger.warning(f"Could not parse {file_path}: {e}")

        if self.discovered_sources:
            print(f"Found {len(self.discovered_sources)} unique source(s):\n")
            for i, (name, info) in enumerate(self.discovered_sources.items(), 1):
                print(f"  {i}. {name}")
                if info.get('path') and info['path'] != name:
                    print(f"     Path: {info['path']}")
                if info.get('table'):
                    print(f"     Table: {info['table']}")
            print()
        else:
            print("No sources found in workflows.\n")

    def _get_source_name(self, source_node) -> Optional[str]:
        """Extract a meaningful name from a source node."""
        if source_node.source_path:
            return Path(source_node.source_path).name
        if source_node.table_name:
            return source_node.table_name
        return None

    def _add_mappings_interactive(self) -> bool:
        """Add individual source mappings interactively."""
        print("-" * 70)
        print("STEP 2: Add Source Mappings")
        print("-" * 70)

        # If we discovered sources, offer to map them
        if self.discovered_sources:
            print("\nWould you like to map the discovered sources?")
            choice = input("Map discovered sources? (y/n) [y]: ").strip().lower()

            if choice != 'n':
                for source_name, source_info in self.discovered_sources.items():
                    if not self._add_single_mapping(source_name, source_info):
                        # User chose to skip or cancel
                        continue

        # Allow adding additional mappings
        print("\nYou can now add additional source mappings manually.")
        print("Press Enter with no input to finish adding mappings.\n")

        try:
            while True:
                source = input("Source name (or Enter to finish): ").strip()
                if not source:
                    break

                self._add_single_mapping(source, {})

        except (KeyboardInterrupt, EOFError):
            print("\n")

        print(f"\n[OK] Added {len(self.mappings)} mapping(s).\n")
        return True

    def _add_single_mapping(self, source_name: str, source_info: Dict[str, Any]) -> bool:
        """Add a single source mapping interactively."""
        print(f"\n--- Mapping: {source_name} ---")

        if source_info.get('workflow'):
            print(f"    Found in: {source_info['workflow']}")

        try:
            print("\nOptions:")
            print("  [1] Use defaults (auto-derive prefix from source name)")
            print("  [2] Enter custom S3 location")
            print("  [3] Skip this source")
            print("  [4] Skip all remaining sources")

            choice = input("\nYour choice [1]: ").strip()

            if choice == '4':
                return False

            if choice == '3':
                return True

            if choice == '2':
                return self._add_custom_mapping(source_name)

            # Default: auto-derive
            return self._add_default_mapping(source_name)

        except (KeyboardInterrupt, EOFError):
            return False

    def _add_default_mapping(self, source_name: str) -> bool:
        """Add a mapping using default settings."""
        table_name = self._derive_table_name(source_name)
        prefix = f"bronze/{table_name}/"

        self.mappings[source_name] = {
            'bucket': self.default_bucket,
            'prefix': prefix,
            'format': self.default_format,
            'table_name': table_name,
        }

        if self.default_endpoint:
            self.mappings[source_name]['endpoint'] = self.default_endpoint
        if self.default_s3_user:
            self.mappings[source_name]['s3_user'] = self.default_s3_user
        if self.default_s3_password:
            self.mappings[source_name]['s3_password'] = self.default_s3_password

        print(f"  -> s3://{self.default_bucket}/{prefix}")
        return True

    def _add_custom_mapping(self, source_name: str) -> bool:
        """Add a mapping with custom settings."""
        try:
            # S3 location
            print("\nEnter the S3 location (s3://bucket/prefix or just bucket/prefix)")
            s3_uri = input("S3 location: ").strip()

            if not s3_uri:
                print("  Skipped (no location entered)")
                return True

            # Parse the URI
            bucket, prefix = self._parse_s3_uri(s3_uri)

            # Table name
            default_table = self._derive_table_name(source_name)
            table_name = input(f"Table name [{default_table}]: ").strip() or default_table

            # Format
            fmt = input(f"File format [{self.default_format}]: ").strip().lower()
            if fmt not in ['parquet', 'csv', 'json', 'orc', 'avro', '']:
                print(f"  Invalid format, using {self.default_format}")
                fmt = ''

            # S3 credentials (optional override)
            s3_user = input(f"S3 User/Access Key [{self.default_s3_user or ''}]: ").strip() or self.default_s3_user
            s3_password = input(f"S3 Password/Secret Key [{self.default_s3_password and '****' or ''}]: ").strip() or self.default_s3_password

            # Columns (optional)
            print("Enter column names separated by commas (optional, press Enter to skip)")
            columns_str = input("Columns: ").strip()
            columns = [c.strip() for c in columns_str.split(',')] if columns_str else []

            # Build mapping
            self.mappings[source_name] = {
                'bucket': bucket,
                'prefix': prefix,
                'format': fmt or self.default_format,
                'table_name': table_name,
            }

            if columns:
                self.mappings[source_name]['columns'] = columns

            if self.default_endpoint:
                self.mappings[source_name]['endpoint'] = self.default_endpoint
            if s3_user:
                self.mappings[source_name]['s3_user'] = s3_user
            if s3_password:
                self.mappings[source_name]['s3_password'] = s3_password

            print(f"  -> s3://{bucket}/{prefix}")
            return True

        except (KeyboardInterrupt, EOFError):
            return True

    def _add_wildcard_patterns(self) -> bool:
        """Add wildcard pattern mappings."""
        print("-" * 70)
        print("STEP 3: Add Wildcard Patterns (Optional)")
        print("-" * 70)
        print("\nWildcard patterns match multiple sources (e.g., '*.csv' matches all CSV files).")
        print("These are applied when no exact match is found.\n")

        # Suggest common patterns
        common_patterns = [
            ("*.csv", "bronze/csv_imports/", "csv"),
            ("*.xlsx", "bronze/excel_imports/", "parquet"),
            ("*.json", "bronze/json_imports/", "json"),
            ("*.parquet", "bronze/parquet_imports/", "parquet"),
        ]

        print("Common patterns:")
        for i, (pattern, prefix, fmt) in enumerate(common_patterns, 1):
            print(f"  {i}. {pattern} -> {prefix} ({fmt})")
        print("  5. Add custom pattern")
        print("  6. Skip wildcard patterns")

        try:
            while True:
                choice = input("\nAdd pattern (1-5) or 6 to finish: ").strip()

                if choice == '6' or not choice:
                    break

                if choice in ['1', '2', '3', '4']:
                    idx = int(choice) - 1
                    pattern, prefix, fmt = common_patterns[idx]
                    self.mappings[pattern] = {
                        'bucket': self.default_bucket,
                        'prefix': prefix,
                        'format': fmt,
                    }
                    print(f"  Added: {pattern} -> s3://{self.default_bucket}/{prefix}")

                elif choice == '5':
                    pattern = input("Enter pattern (e.g., 'raw_*.csv'): ").strip()
                    if pattern:
                        default_prefix = "bronze/" + pattern.replace("*", "misc") + "/"
                        prefix = input(f"S3 prefix [{default_prefix}]: ").strip()
                        prefix = prefix or default_prefix
                        fmt = input(f"Format [{self.default_format}]: ").strip() or self.default_format

                        self.mappings[pattern] = {
                            'bucket': self.default_bucket,
                            'prefix': prefix,
                            'format': fmt,
                        }
                        print(f"  Added: {pattern} -> s3://{self.default_bucket}/{prefix}")
                else:
                    print("  Invalid choice")

        except (KeyboardInterrupt, EOFError):
            print("\n")

        return True

    def _review_and_save(self, output_path: str) -> bool:
        """Review configuration and save to file."""
        print("\n" + "-" * 70)
        print("STEP 4: Review and Save")
        print("-" * 70)

        # Build the configuration
        config = {
            '_comment': 'S3 Source Mappings Configuration for Alteryx to Starburst/DBT Migration',
            '_version': '1.0',
            '_generated_by': 'S3 Mappings Generator',
            'default_bucket': self.default_bucket,
            'default_format': self.default_format,
            'endpoint': self.default_endpoint,
            'mappings': self.mappings,
        }

        # Include credentials in config if set
        if self.default_s3_user:
            config['s3_user'] = self.default_s3_user
        if self.default_s3_password:
            config['s3_password'] = self.default_s3_password

        # Show summary
        print(f"\nConfiguration Summary:")
        print(f"  Default Bucket: {self.default_bucket}")
        print(f"  Default Format: {self.default_format}")
        if self.default_endpoint:
            print(f"  Endpoint: {self.default_endpoint}")
        if self.default_s3_user:
            print(f"  S3 User: {self.default_s3_user}")
            print(f"  S3 Password: {'****' if self.default_s3_password else '(not set)'}")
        print(f"  Total Mappings: {len(self.mappings)}")

        if self.mappings:
            print("\nMappings:")
            for source, mapping in self.mappings.items():
                bucket = mapping.get('bucket', self.default_bucket)
                prefix = mapping.get('prefix', '')
                fmt = mapping.get('format', self.default_format)
                print(f"  {source}")
                print(f"    -> s3://{bucket}/{prefix} ({fmt})")

        print(f"\nOutput file: {output_path}")

        try:
            confirm = input("\nSave configuration? (y/n) [y]: ").strip().lower()

            if confirm == 'n':
                print("Configuration not saved.")
                return False

            # Save to file
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2)

            print(f"\n[OK] Configuration saved to: {output_path}")
            print("\nUsage:")
            print(f"  python main.py analyze <path> --s3-config {output_path} --generate-dbt ./dbt")

            return True

        except (KeyboardInterrupt, EOFError):
            print("\n\nCancelled.")
            return False
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            return False

    def _parse_s3_uri(self, uri: str) -> Tuple[str, str]:
        """Parse an S3 URI into bucket and prefix."""
        # Remove protocol prefix
        if uri.startswith('s3a://'):
            uri = uri[6:]
        elif uri.startswith('s3://'):
            uri = uri[5:]

        # Split bucket and prefix
        parts = uri.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        # Ensure prefix ends with /
        if prefix and not prefix.endswith('/'):
            prefix += '/'

        return bucket, prefix

    def _derive_table_name(self, source_name: str) -> str:
        """Derive a Trino-compatible table name from source name."""
        # Remove file extension
        name = Path(source_name).stem

        # Convert to snake_case
        name = re.sub(r'[^a-zA-Z0-9]', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_').lower()

        # Ensure it doesn't start with a number
        if name and name[0].isdigit():
            name = 'tbl_' + name

        # Add stg_ prefix for staging tables
        if not name.startswith(('stg_', 'int_', 'fct_', 'dim_')):
            name = 'stg_' + name

        return name or 'stg_unnamed_source'

    def add_mapping_programmatic(self, source_name: str, bucket: str, prefix: str,
                                  file_format: str = "parquet",
                                  table_name: Optional[str] = None,
                                  columns: Optional[List[str]] = None,
                                  endpoint: Optional[str] = None,
                                  s3_user: Optional[str] = None,
                                  s3_password: Optional[str] = None) -> None:
        """Add a mapping programmatically (non-interactive).

        Args:
            source_name: Alteryx source name (filename or table)
            bucket: S3 bucket name
            prefix: S3 prefix/folder path
            file_format: File format (parquet, csv, json, etc.)
            table_name: Target table name (auto-derived if not provided)
            columns: Optional list of column names
            endpoint: S3-compatible endpoint URL
            s3_user: S3 access key / username
            s3_password: S3 secret key / password
        """
        mapping = {
            'bucket': bucket,
            'prefix': prefix,
            'format': file_format,
            'table_name': table_name or self._derive_table_name(source_name),
        }

        if columns:
            mapping['columns'] = columns
        if endpoint:
            mapping['endpoint'] = endpoint
        if s3_user:
            mapping['s3_user'] = s3_user
        if s3_password:
            mapping['s3_password'] = s3_password

        self.mappings[source_name] = mapping

    def save_to_file(self, output_path: str) -> None:
        """Save the current configuration to a JSON file.

        Args:
            output_path: Path to save the JSON file
        """
        config = {
            '_comment': 'S3 Source Mappings Configuration for Alteryx to Starburst/DBT Migration',
            '_version': '1.0',
            'default_bucket': self.default_bucket,
            'default_format': self.default_format,
            'endpoint': self.default_endpoint,
            'mappings': self.mappings,
        }

        # Include credentials if set
        if self.default_s3_user:
            config['s3_user'] = self.default_s3_user
        if self.default_s3_password:
            config['s3_password'] = self.default_s3_password

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)

        logger.info(f"Saved S3 mappings to: {output_path}")

    def load_from_file(self, config_path: str) -> None:
        """Load existing configuration from a JSON file.

        Args:
            config_path: Path to the JSON configuration file
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        self.default_bucket = data.get('default_bucket', self.default_bucket)
        self.default_format = data.get('default_format', self.default_format)
        self.default_endpoint = data.get('endpoint')
        self.default_s3_user = data.get('s3_user')
        self.default_s3_password = data.get('s3_password')
        self.mappings = data.get('mappings', {})

        logger.info(f"Loaded {len(self.mappings)} mappings from: {config_path}")


def run_generator(output_path: str = "s3_mappings.json",
                  workflow_paths: Optional[List[str]] = None,
                  default_bucket: Optional[str] = None,
                  default_s3_user: Optional[str] = None,
                  default_s3_password: Optional[str] = None) -> bool:
    """Run the interactive S3 mappings generator.

    Args:
        output_path: Path to save the generated JSON file
        workflow_paths: Optional list of workflow paths to scan
        default_bucket: Default S3 bucket name
        default_s3_user: Default S3 access key / username
        default_s3_password: Default S3 secret key / password

    Returns:
        True if configuration was saved successfully
    """
    generator = S3MappingsGenerator(
        default_bucket=default_bucket,
        default_s3_user=default_s3_user,
        default_s3_password=default_s3_password,
    )

    return generator.run_interactive(output_path, workflow_paths)


if __name__ == '__main__':
    # Allow running directly for testing
    import argparse

    parser = argparse.ArgumentParser(description='Generate S3 mappings configuration')
    parser.add_argument('-o', '--output', default='s3_mappings.json',
                        help='Output file path')
    parser.add_argument('--workflows', nargs='*',
                        help='Workflow paths to scan for sources')
    parser.add_argument('--bucket', help='Default S3 bucket')
    parser.add_argument('--s3-user', help='Default S3 access key / username')
    parser.add_argument('--s3-password', help='Default S3 secret key / password')

    args = parser.parse_args()

    success = run_generator(
        output_path=args.output,
        workflow_paths=args.workflows,
        default_bucket=args.bucket,
        default_s3_user=args.s3_user,
        default_s3_password=args.s3_password,
    )

    sys.exit(0 if success else 1)
