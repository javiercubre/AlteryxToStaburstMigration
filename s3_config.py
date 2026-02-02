"""
S3 configuration handler with interactive prompts for source mapping.

Provides functionality to map Alteryx sources to S3-compatible bucket locations
for migration to the bronze DBT/Trino layer.
"""
import json
import sys
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any

from models import S3SourceConfig, S3SourceMapping
from logging_config import get_logger

# Module logger
logger = get_logger(__name__)


@dataclass
class S3ConfigResolver:
    """Handles S3 source resolution with caching and interactive prompts.

    Follows the same pattern as MacroResolver for consistency.
    """

    # Default settings
    default_bucket: Optional[str] = None
    default_region: str = "us-east-1"
    default_endpoint: Optional[str] = None  # For S3-compatible services
    default_format: str = "parquet"

    # Cache of resolved S3 configs (source_name -> S3SourceConfig)
    resolved_configs: Dict[str, S3SourceConfig] = field(default_factory=dict)

    # Source mappings (source_name -> S3SourceMapping)
    source_mappings: Dict[str, S3SourceMapping] = field(default_factory=dict)

    # Sources to skip
    skip_sources: set = field(default_factory=set)

    # Skip all sources (non-interactive mode)
    skip_all: bool = False

    # Interactive mode flag
    interactive: bool = True

    # TODOs for non-interactive mode
    todos: List[Dict[str, str]] = field(default_factory=list)

    def load_from_file(self, config_path: str) -> None:
        """Load S3 mappings from a JSON configuration file.

        Expected format:
        {
            "default_region": "us-east-1",
            "default_bucket": "my-data-lake",
            "endpoint": null,
            "mappings": {
                "customers.csv": {
                    "bucket": "my-data-lake",
                    "prefix": "bronze/customers/",
                    "format": "parquet"
                }
            }
        }
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"S3 config file not found: {config_path}")

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Load defaults
        if 'default_region' in data:
            self.default_region = data['default_region']
        if 'default_bucket' in data:
            self.default_bucket = data['default_bucket']
        if 'endpoint' in data:
            self.default_endpoint = data['endpoint']
        if 'default_format' in data:
            self.default_format = data['default_format']

        # Load mappings
        for source_name, config_data in data.get('mappings', {}).items():
            # Merge defaults into config
            if 'region' not in config_data and self.default_region:
                config_data['region'] = self.default_region
            if 'bucket' not in config_data and self.default_bucket:
                config_data['bucket'] = self.default_bucket
            if 'endpoint' not in config_data and self.default_endpoint:
                config_data['endpoint'] = self.default_endpoint
            if 'file_format' not in config_data and 'format' in config_data:
                config_data['file_format'] = config_data.pop('format')

            s3_config = S3SourceConfig.from_dict(config_data)
            self.resolved_configs[source_name] = s3_config

        logger.info(f"Loaded {len(self.resolved_configs)} S3 mappings from {config_path}")

    def save_to_file(self, config_path: str) -> None:
        """Save current S3 mappings to a JSON configuration file."""
        data = {
            'default_region': self.default_region,
            'default_bucket': self.default_bucket,
            'endpoint': self.default_endpoint,
            'default_format': self.default_format,
            'mappings': {}
        }

        for source_name, mapping in self.source_mappings.items():
            data['mappings'][source_name] = {
                'bucket': mapping.s3_config.bucket,
                'prefix': mapping.s3_config.prefix,
                'format': mapping.s3_config.file_format,
                'region': mapping.s3_config.region,
                'table_name': mapping.table_name,
                'columns': mapping.columns,
            }
            if mapping.s3_config.endpoint:
                data['mappings'][source_name]['endpoint'] = mapping.s3_config.endpoint

        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved {len(self.source_mappings)} S3 mappings to {config_path}")

    def resolve_source(self, source_name: str, source_path: Optional[str] = None,
                       workflow_name: str = "") -> Optional[S3SourceMapping]:
        """Resolve an Alteryx source to an S3 location.

        Args:
            source_name: Name of the source (e.g., filename, table name)
            source_path: Original path/connection string from Alteryx
            workflow_name: Name of the workflow (for context in prompts)

        Returns:
            S3SourceMapping if resolved, None if skipped
        """
        # Already resolved?
        if source_name in self.source_mappings:
            return self.source_mappings[source_name]

        # Check pre-loaded configs FIRST (these always take priority)
        if source_name in self.resolved_configs:
            s3_config = self.resolved_configs[source_name]
            mapping = S3SourceMapping(
                alteryx_source_name=source_name,
                s3_config=s3_config,
                table_name=self._derive_table_name(source_name),
            )
            self.source_mappings[source_name] = mapping
            return mapping

        # Try wildcard matching in pre-loaded configs
        for pattern, s3_config in self.resolved_configs.items():
            if self._matches_pattern(source_name, pattern):
                mapping = S3SourceMapping(
                    alteryx_source_name=source_name,
                    s3_config=s3_config,
                    table_name=self._derive_table_name(source_name),
                )
                self.source_mappings[source_name] = mapping
                return mapping

        # Should we skip? (only for sources not in config)
        if source_name in self.skip_sources or self.skip_all:
            return None

        # Interactive prompt
        if self.interactive:
            return self._prompt_for_s3_location(source_name, source_path, workflow_name)

        # Non-interactive: add to TODOs
        self._add_todo(source_name, source_path)
        return None

    def resolve_sources_batch(self, sources: List[Dict[str, Any]],
                               workflow_name: str = "") -> Dict[str, S3SourceMapping]:
        """Resolve multiple sources at once.

        Args:
            sources: List of dicts with 'name', 'path', and optionally 'node_id'
            workflow_name: Name of the workflow

        Returns:
            Dict of source_name -> S3SourceMapping for resolved sources
        """
        results = {}

        # Show summary first if interactive
        if self.interactive and sources and sys.stdin.isatty():
            logger.info("\n" + "=" * 60)
            logger.info(f"S3 Source Mapping for: {workflow_name or 'workflow'}")
            logger.info("=" * 60)
            logger.info(f"\nFound {len(sources)} source(s) to map:")
            for i, src in enumerate(sources, 1):
                logger.info(f"  {i}. {src.get('name', 'unknown')}")
                if src.get('path'):
                    logger.info(f"     Original: {src['path']}")
            logger.info("")

            # Offer bulk options
            if len(sources) > 1 and self.default_bucket:
                choice = self._prompt_bulk_option()
                if choice == 'all_default':
                    for src in sources:
                        name = src.get('name', 'unknown')
                        mapping = self._create_default_mapping(name)
                        self.source_mappings[name] = mapping
                        results[name] = mapping
                    return results
                elif choice == 'skip_all':
                    self.skip_all = True
                    return results

        # Resolve each source
        for src in sources:
            name = src.get('name', 'unknown')
            path = src.get('path')
            node_id = src.get('node_id')

            mapping = self.resolve_source(name, path, workflow_name)
            if mapping:
                if node_id:
                    mapping.original_node_id = node_id
                results[name] = mapping

        return results

    def _prompt_for_s3_location(self, source_name: str, source_path: Optional[str],
                                 workflow_name: str) -> Optional[S3SourceMapping]:
        """Interactively prompt user for S3 location."""
        # Check if we can actually interact with the user
        if not sys.stdin.isatty():
            logger.info(f"Non-interactive environment. Cannot prompt for source: {source_name}")
            logger.info("  Use --s3-config FILE to specify S3 mappings")
            self._add_todo(source_name, source_path)
            return None

        logger.info("\n" + "-" * 60)
        logger.info(f"Source: \"{source_name}\"")
        if source_path and source_path != source_name:
            logger.info(f"Original path: {source_path}")
        if workflow_name:
            logger.info(f"Workflow: {workflow_name}")
        logger.info("-" * 60)
        logger.info("\nOptions:")
        logger.info("[1] Enter S3 location (s3://bucket/prefix)")
        logger.info("[2] Use default bucket" + (f" ({self.default_bucket})" if self.default_bucket else " (not set)"))
        logger.info("[3] Skip this source")
        logger.info("[4] Skip all remaining sources")
        logger.info("")

        while True:
            try:
                choice = input("Your choice (1-4): ").strip()

                if choice == "1":
                    return self._prompt_s3_details(source_name)

                elif choice == "2":
                    if not self.default_bucket:
                        bucket = input("Enter default bucket name: ").strip()
                        if bucket:
                            self.default_bucket = bucket
                        else:
                            logger.warning("No bucket specified")
                            continue

                    mapping = self._create_default_mapping(source_name)
                    self.source_mappings[source_name] = mapping
                    logger.info(f"  Mapped to: {mapping.s3_config.get_s3_uri()}")
                    return mapping

                elif choice == "3":
                    self.skip_sources.add(source_name)
                    logger.info(f"Skipping source: {source_name}")
                    self._add_todo(source_name, source_path)
                    return None

                elif choice == "4":
                    self.skip_all = True
                    self.skip_sources.add(source_name)
                    logger.info("Skipping all remaining sources")
                    self._add_todo(source_name, source_path)
                    return None

                else:
                    logger.warning("Invalid choice. Please enter 1, 2, 3, or 4.")

            except KeyboardInterrupt:
                logger.info("\nSkipping S3 source resolution")
                return None
            except EOFError:
                # Non-interactive environment
                self.skip_sources.add(source_name)
                self._add_todo(source_name, source_path)
                return None

    def _prompt_s3_details(self, source_name: str) -> Optional[S3SourceMapping]:
        """Prompt for detailed S3 configuration."""
        try:
            # Get S3 URI
            uri = input("Enter S3 location (s3://bucket/prefix): ").strip()
            if not uri:
                logger.warning("No S3 location provided")
                return None

            # Parse URI
            s3_config = S3SourceConfig.from_s3_uri(uri)

            # Set region (use default or prompt)
            region_prompt = f"Region [{self.default_region}]: "
            region = input(region_prompt).strip() or self.default_region
            s3_config.region = region

            # Set endpoint if needed
            if self.default_endpoint:
                s3_config.endpoint = self.default_endpoint
            else:
                endpoint = input("S3-compatible endpoint (leave blank for AWS): ").strip()
                if endpoint:
                    s3_config.endpoint = endpoint

            # Set file format
            format_prompt = f"File format [{self.default_format}]: "
            file_format = input(format_prompt).strip() or self.default_format
            s3_config.file_format = file_format

            # Derive table name
            table_name = self._derive_table_name(source_name)
            custom_table = input(f"Table name [{table_name}]: ").strip()
            if custom_table:
                table_name = custom_table

            # Create mapping
            mapping = S3SourceMapping(
                alteryx_source_name=source_name,
                s3_config=s3_config,
                table_name=table_name,
            )

            self.source_mappings[source_name] = mapping
            self.resolved_configs[source_name] = s3_config

            logger.info(f"  Mapped '{source_name}' to: {s3_config.get_s3_uri()}")
            return mapping

        except Exception as e:
            logger.warning(f"Error configuring S3 source: {e}")
            return None

    def _prompt_bulk_option(self) -> str:
        """Prompt for bulk mapping option."""
        try:
            logger.info("Bulk options:")
            logger.info(f"  [a] Map ALL sources to default bucket ({self.default_bucket})")
            logger.info("  [s] Skip all sources (document as TODOs)")
            logger.info("  [i] Configure individually")
            logger.info("")

            choice = input("Your choice (a/s/i): ").strip().lower()

            if choice == 'a':
                return 'all_default'
            elif choice == 's':
                return 'skip_all'
            else:
                return 'individual'

        except (KeyboardInterrupt, EOFError):
            return 'individual'

    def _create_default_mapping(self, source_name: str) -> S3SourceMapping:
        """Create a mapping using default settings."""
        # Derive prefix from source name
        prefix = self._derive_prefix(source_name)

        s3_config = S3SourceConfig(
            bucket=self.default_bucket or "data-lake",
            prefix=prefix,
            region=self.default_region,
            endpoint=self.default_endpoint,
            file_format=self.default_format,
        )

        return S3SourceMapping(
            alteryx_source_name=source_name,
            s3_config=s3_config,
            table_name=self._derive_table_name(source_name),
        )

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

        return name or 'unnamed_source'

    def _derive_prefix(self, source_name: str) -> str:
        """Derive an S3 prefix from source name."""
        table_name = self._derive_table_name(source_name)
        return f"bronze/{table_name}/"

    def _matches_pattern(self, source_name: str, pattern: str) -> bool:
        """Check if source name matches a pattern (supports * wildcard)."""
        if pattern == '*':
            return True

        # Convert glob pattern to regex
        regex_pattern = pattern.replace('.', r'\.').replace('*', '.*')
        return bool(re.match(f'^{regex_pattern}$', source_name, re.IGNORECASE))

    def _add_todo(self, source_name: str, source_path: Optional[str]) -> None:
        """Add a TODO item for manual S3 configuration."""
        self.todos.append({
            'type': 's3_mapping',
            'source_name': source_name,
            'source_path': source_path or source_name,
            'message': f"Configure S3 location for source: {source_name}",
        })

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of S3 source resolution.

        Returns:
            Dict with keys: total_sources, resolved, skipped, todos
        """
        return {
            'total_sources': len(self.source_mappings) + len(self.skip_sources) + len(self.todos),
            'resolved': len(self.source_mappings),
            'skipped': len(self.skip_sources),
            'todos': len(self.todos),
            'default_bucket': self.default_bucket,
            'default_region': self.default_region,
        }
