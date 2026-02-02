"""
Data models for Alteryx workflow parsing.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from enum import Enum
from pathlib import Path


class ToolCategory(Enum):
    """Categories of Alteryx tools."""
    INPUT = "input"
    OUTPUT = "output"
    PREPARATION = "preparation"
    JOIN = "join"
    TRANSFORM = "transform"
    PARSE = "parse"
    REPORTING = "reporting"
    SPATIAL = "spatial"
    PREDICTIVE = "predictive"
    DEVELOPER = "developer"
    IN_DATABASE = "in_database"
    MACRO = "macro"
    CONTAINER = "container"  # Tool containers for organization
    UNKNOWN = "unknown"


class MedallionLayer(Enum):
    """Medallion architecture layers."""
    BRONZE = "bronze"  # Raw/staging
    SILVER = "silver"  # Intermediate/cleaned
    GOLD = "gold"      # Business-ready/marts


@dataclass
class AlteryxNode:
    """Represents a single tool/node in an Alteryx workflow."""
    tool_id: int
    tool_type: str                          # Full plugin name from GuiSettings
    plugin_name: str                        # Simplified tool name
    category: ToolCategory = ToolCategory.UNKNOWN
    configuration: Dict[str, Any] = field(default_factory=dict)
    position: tuple = (0, 0)                # x, y coordinates
    is_macro: bool = False
    macro_path: Optional[str] = None
    annotation: Optional[str] = None        # Tool annotation/comment

    # Container relationships
    container_id: Optional[int] = None      # ID of parent container (if inside one)
    child_tool_ids: List[int] = field(default_factory=list)  # For containers: IDs of child tools

    # Extracted details based on tool type
    source_path: Optional[str] = None       # For input tools
    target_path: Optional[str] = None       # For output tools
    connection_string: Optional[str] = None # Database connection
    table_name: Optional[str] = None        # Database table
    sql_query: Optional[str] = None         # SQL query if present

    # Parsed connection string components (MED-10 fix)
    db_server: Optional[str] = None         # Server/host name
    db_database: Optional[str] = None       # Database name
    db_schema: Optional[str] = None         # Schema name
    db_auth_type: Optional[str] = None      # Authentication type (e.g., 'SQL', 'Windows', 'OAuth')
    expression: Optional[str] = None        # Formula/filter expression
    join_keys: List[str] = field(default_factory=list)
    join_type: Optional[str] = None
    group_by_fields: List[str] = field(default_factory=list)
    aggregations: List[Dict[str, str]] = field(default_factory=list)
    selected_fields: List[str] = field(default_factory=list)

    # S3 source mapping (V2) - set when source is replaced with S3 location
    s3_mapping: Optional['S3SourceMapping'] = None

    def get_display_name(self) -> str:
        """Get a human-readable name for the tool."""
        if self.annotation:
            return f"{self.plugin_name}: {self.annotation}"
        return self.plugin_name


@dataclass
class AlteryxConnection:
    """Represents a connection between two tools."""
    origin_id: int
    origin_anchor: str      # Output anchor name (e.g., "Output", "True", "False", "Left", "Right")
    destination_id: int
    destination_anchor: str # Input anchor name (e.g., "Input", "Left", "Right")

    def __repr__(self) -> str:
        return f"Connection({self.origin_id}:{self.origin_anchor} -> {self.destination_id}:{self.destination_anchor})"


@dataclass
class WorkflowMetadata:
    """Metadata about the workflow."""
    name: str
    file_path: str
    alteryx_version: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    created_date: Optional[str] = None
    modified_date: Optional[str] = None


@dataclass
class AlteryxWorkflow:
    """Complete parsed Alteryx workflow."""
    metadata: WorkflowMetadata
    nodes: List[AlteryxNode] = field(default_factory=list)
    connections: List[AlteryxConnection] = field(default_factory=list)

    # Derived data
    sources: List[AlteryxNode] = field(default_factory=list)
    targets: List[AlteryxNode] = field(default_factory=list)
    macros_used: List[str] = field(default_factory=list)
    missing_macros: List[str] = field(default_factory=list)

    def get_node_by_id(self, tool_id: int) -> Optional[AlteryxNode]:
        """Get a node by its tool ID."""
        for node in self.nodes:
            if node.tool_id == tool_id:
                return node
        return None

    def get_downstream_nodes(self, tool_id: int) -> List[AlteryxNode]:
        """Get all nodes that receive input from the given tool."""
        downstream_ids = [c.destination_id for c in self.connections if c.origin_id == tool_id]
        return [n for n in self.nodes if n.tool_id in downstream_ids]

    def get_upstream_nodes(self, tool_id: int) -> List[AlteryxNode]:
        """Get all nodes that provide input to the given tool."""
        upstream_ids = [c.origin_id for c in self.connections if c.destination_id == tool_id]
        return [n for n in self.nodes if n.tool_id in upstream_ids]

    def get_upstream_connections(self, tool_id: int) -> List[AlteryxConnection]:
        """Get all connections that feed into the given tool.

        Returns connections with full anchor information for tools like Join
        that need to distinguish Left vs Right inputs (HIGH-02 fix).
        """
        return [c for c in self.connections if c.destination_id == tool_id]

    def get_upstream_node_by_anchor(self, tool_id: int, anchor: str) -> Optional[AlteryxNode]:
        """Get the upstream node connected to a specific input anchor.

        Args:
            tool_id: The tool ID to find upstream for
            anchor: The destination anchor name (e.g., 'Left', 'Right', 'Input')

        Returns:
            The upstream node connected to that anchor, or None
        """
        for conn in self.connections:
            if conn.destination_id == tool_id and conn.destination_anchor.lower() == anchor.lower():
                return self.get_node_by_id(conn.origin_id)
        return None


@dataclass
class MacroInfo:
    """Information about a macro."""
    name: str
    file_path: str
    found: bool = False
    resolved_path: Optional[str] = None
    workflow: Optional[AlteryxWorkflow] = None  # Parsed macro workflow
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class TransformationStep:
    """A single transformation step in the workflow."""
    order: int
    tool_id: int
    tool_name: str
    category: ToolCategory
    description: str
    expression: Optional[str] = None
    medallion_layer: MedallionLayer = MedallionLayer.SILVER
    dbt_hint: Optional[str] = None  # SQL/DBT translation hint


@dataclass
class DataLineage:
    """Data lineage from source to target."""
    source: AlteryxNode
    target: AlteryxNode
    path: List[AlteryxNode]  # Ordered list of nodes from source to target
    transformations: List[TransformationStep] = field(default_factory=list)


@dataclass
class SourceInventory:
    """Inventory of all data sources across workflows."""
    name: str
    source_type: str  # file, database, api, etc.
    path_or_connection: str
    workflows_using: List[str] = field(default_factory=list)
    suggested_dbt_source: Optional[str] = None


@dataclass
class TargetInventory:
    """Inventory of all output targets across workflows."""
    name: str
    target_type: str
    path_or_connection: str
    workflows_using: List[str] = field(default_factory=list)
    suggested_dbt_model: Optional[str] = None


@dataclass
class DBTModel:
    """Represents a suggested DBT model."""
    name: str
    layer: MedallionLayer
    sql_template: str
    source_models: List[str] = field(default_factory=list)
    description: Optional[str] = None
    original_tools: List[int] = field(default_factory=list)  # Tool IDs this model represents


# =============================================================================
# S3 Source Integration (V2)
# =============================================================================

@dataclass
class S3SourceConfig:
    """Configuration for an S3-compatible bucket source.

    Supports AWS S3, MinIO, and other S3-compatible storage services.
    """
    bucket: str
    prefix: str  # folder path in bucket (without leading slash)
    region: str = "us-east-1"
    endpoint: Optional[str] = None  # For S3-compatible services (MinIO, etc.)
    file_format: str = "parquet"  # parquet, csv, json
    file_pattern: Optional[str] = None  # e.g., "*.parquet"
    credentials_profile: Optional[str] = None  # AWS credentials profile name

    def get_s3_uri(self) -> str:
        """Get the full S3 URI (s3a:// for Trino compatibility)."""
        prefix = self.prefix.strip('/')
        return f"s3a://{self.bucket}/{prefix}"

    def get_s3_url(self) -> str:
        """Get the S3 URL (s3:// format)."""
        prefix = self.prefix.strip('/')
        return f"s3://{self.bucket}/{prefix}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'bucket': self.bucket,
            'prefix': self.prefix,
            'region': self.region,
            'endpoint': self.endpoint,
            'file_format': self.file_format,
            'file_pattern': self.file_pattern,
            'credentials_profile': self.credentials_profile,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'S3SourceConfig':
        """Create from dictionary (JSON deserialization)."""
        return cls(
            bucket=data['bucket'],
            prefix=data.get('prefix', ''),
            region=data.get('region', 'us-east-1'),
            endpoint=data.get('endpoint'),
            file_format=data.get('file_format', 'parquet'),
            file_pattern=data.get('file_pattern'),
            credentials_profile=data.get('credentials_profile'),
        )

    @classmethod
    def from_s3_uri(cls, uri: str, **kwargs) -> 'S3SourceConfig':
        """Parse an S3 URI into a config.

        Supports formats:
        - s3://bucket/prefix
        - s3a://bucket/prefix
        - bucket/prefix (assumes s3://)
        """
        # Remove protocol prefix
        if uri.startswith('s3a://'):
            uri = uri[6:]
        elif uri.startswith('s3://'):
            uri = uri[5:]

        # Split bucket and prefix
        parts = uri.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        return cls(bucket=bucket, prefix=prefix, **kwargs)


@dataclass
class S3SourceMapping:
    """Maps an Alteryx source to an S3 location.

    Used to replace local/database sources with S3 bucket data during migration.
    """
    alteryx_source_name: str  # Original source from Alteryx (filename or connection)
    s3_config: S3SourceConfig
    table_name: str  # Target table name in Trino
    columns: List[str] = field(default_factory=list)  # Column names if known
    original_node_id: Optional[int] = None  # Reference to original Alteryx node

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'alteryx_source_name': self.alteryx_source_name,
            's3_config': self.s3_config.to_dict(),
            'table_name': self.table_name,
            'columns': self.columns,
            'original_node_id': self.original_node_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'S3SourceMapping':
        """Create from dictionary (JSON deserialization)."""
        return cls(
            alteryx_source_name=data['alteryx_source_name'],
            s3_config=S3SourceConfig.from_dict(data['s3_config']),
            table_name=data['table_name'],
            columns=data.get('columns', []),
            original_node_id=data.get('original_node_id'),
        )


@dataclass
class YXDBReference:
    """Reference to a YXDB file for gold layer validation.

    The user provides a YXDB (Alteryx output) file to validate that the
    gold layer produces equivalent output after migration.
    """
    file_path: Path
    table_name: str  # Expected gold layer table name
    columns: List[str] = field(default_factory=list)
    row_count: Optional[int] = None
    checksum: Optional[str] = None  # For exact match validation

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'file_path': str(self.file_path),
            'table_name': self.table_name,
            'columns': self.columns,
            'row_count': self.row_count,
            'checksum': self.checksum,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'YXDBReference':
        """Create from dictionary (JSON deserialization)."""
        return cls(
            file_path=Path(data['file_path']),
            table_name=data['table_name'],
            columns=data.get('columns', []),
            row_count=data.get('row_count'),
            checksum=data.get('checksum'),
        )
