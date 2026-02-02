# V2: S3-Compatible Bucket Source Integration Plan

## Overview

Extend the Alteryx to DBT migration tool to support S3-compatible buckets as source replacements. Users can interactively map Alteryx sources to S3 bucket locations, which are then loaded into the bronze DBT/Trino layer.

## Current State Analysis

**Existing Patterns:**
- Sources parsed via `alteryx_parser.py` → `AlteryxNode` dataclass
- Bronze layer: `stg_*` models referencing `_sources.yml`
- Interactive: `MacroResolver` pattern with TTY detection, session caching, graceful degradation

**S3 Already Recognized:**
- `AlteryxConnectGui.S3Input.S3Input` mapped to `ToolCategory.INPUT`
- No dedicated S3 parameter extraction or Trino external table generation

---

## Implementation Tasks (7 Sessions)

### Session 1: Data Models & Configuration
**Goal:** Define S3 configuration structures and dataclasses

**Files to modify:**
- `models.py` - Add S3-specific dataclasses
- New file: `s3_config.py` - S3 configuration handler

**Tasks:**
1. Add `S3SourceConfig` dataclass to `models.py`:
   ```python
   @dataclass
   class S3SourceConfig:
       bucket: str
       prefix: str  # folder path in bucket
       region: str = "us-east-1"
       endpoint: Optional[str] = None  # For S3-compatible (MinIO, etc.)
       file_format: str = "parquet"  # parquet, csv, json
       file_pattern: Optional[str] = None  # e.g., "*.parquet"
       credentials_profile: Optional[str] = None
   ```

2. Add `S3SourceMapping` dataclass to `models.py`:
   ```python
   @dataclass
   class S3SourceMapping:
       alteryx_source_name: str  # Original source from Alteryx
       s3_config: S3SourceConfig
       table_name: str  # Target table name in Trino
       columns: List[str] = field(default_factory=list)
   ```

3. Create `s3_config.py` with `S3ConfigResolver` class:
   - Load/save JSON config files
   - Session caching for resolved S3 paths
   - Validation methods for S3 URIs

**Acceptance Criteria:**
- Dataclasses properly typed with `@dataclass` decorator
- JSON serialization/deserialization working
- Unit tests for config validation

---

### Session 2: Interactive S3 Metadata Collection
**Goal:** Implement interactive prompts for S3 bucket metadata

**Files to modify:**
- `s3_config.py` - Add interactive resolution
- `main.py` - Add new CLI arguments

**Tasks:**
1. Add interactive prompt methods to `S3ConfigResolver`:
   ```python
   def _prompt_for_s3_location(self, source_name: str) -> Optional[S3SourceConfig]:
       """Prompt user for S3 bucket and path."""
       # Menu: Enter s3://bucket/path, Select region, Skip, Skip all
   ```

2. Add TTY detection following existing pattern:
   ```python
   if not sys.stdin.isatty():
       logger.info("Non-interactive environment. Use --s3-config FILE")
       self._add_todo(...)
       return None
   ```

3. Add session caching:
   - `resolved_s3_configs: Dict[str, S3SourceConfig]`
   - `default_bucket: Optional[str]` (reuse for subsequent sources)

4. Add CLI arguments to `main.py`:
   ```python
   --s3-config FILE     # JSON file with S3 mappings
   --s3-bucket NAME     # Default bucket for all sources
   --s3-region REGION   # Default region
   --s3-endpoint URL    # For S3-compatible services
   ```

**Acceptance Criteria:**
- Interactive prompts work in TTY
- Non-interactive mode reads from config file
- Session caching prevents duplicate prompts for same bucket

---

### Session 3: Source Replacement Logic
**Goal:** Map Alteryx sources to S3 locations during analysis

**Files to modify:**
- `transformation_analyzer.py` - Add S3 source replacement
- `alteryx_parser.py` - Enhance source detection

**Tasks:**
1. Add `replace_sources_with_s3()` method to `TransformationAnalyzer`:
   ```python
   def replace_sources_with_s3(
       self,
       workflow: AlteryxWorkflow,
       s3_resolver: S3ConfigResolver
   ) -> Dict[int, S3SourceMapping]:
       """For each INPUT node, prompt/resolve S3 location."""
   ```

2. Add source inventory for S3 replacement:
   - List all Alteryx sources from workflow
   - Display to user for mapping decision
   - Store mappings in workflow metadata

3. Update `AlteryxNode` with S3 mapping:
   ```python
   s3_mapping: Optional[S3SourceMapping] = None
   ```

4. Add source matching logic:
   - Match by source name
   - Match by file pattern
   - Allow wildcard mappings (`*` → default bucket)

**Acceptance Criteria:**
- All INPUT nodes can be mapped to S3 locations
- Mappings persisted in workflow analysis
- Support for bulk mapping (all sources to same bucket/prefix)

---

### Session 4: Trino External Table Generation
**Goal:** Generate Trino-compatible external table definitions for S3

**Files to modify:**
- `dbt_generator.py` - Add S3 source generation
- New file: `trino_s3_templates.py` - SQL templates

**Tasks:**
1. Create `trino_s3_templates.py` with Starburst/Trino templates:
   ```python
   EXTERNAL_TABLE_PARQUET = """
   CREATE TABLE IF NOT EXISTS {schema}.{table} (
       {column_definitions}
   )
   WITH (
       external_location = 's3a://{bucket}/{prefix}',
       format = 'PARQUET'
   )
   """

   EXTERNAL_TABLE_CSV = """
   -- With CSV format and options
   WITH (
       external_location = 's3a://{bucket}/{prefix}',
       format = 'CSV',
       skip_header_line_count = 1
   )
   """
   ```

2. Add `_generate_s3_source_definition()` to `DBTGenerator`:
   - Generate Trino external table DDL
   - Support Parquet, CSV, JSON formats
   - Handle partitioned data (Hive-style partitions)

3. Update `_generate_sources_yml()` for S3:
   ```yaml
   sources:
     - name: s3_raw
       tables:
         - name: customers
           meta:
             external_location: "s3a://my-bucket/data/customers/"
             file_format: parquet
   ```

4. Add format-specific column inference:
   - Parquet: schema from file metadata
   - CSV: header row detection
   - JSON: sample record analysis

**Acceptance Criteria:**
- Valid Trino external table DDL generated
- DBT sources.yml includes S3 metadata
- Support for major file formats (Parquet, CSV, JSON)

---

### Session 5: Bronze Model Generation for S3
**Goal:** Generate bronze layer DBT models that read from S3

**Files to modify:**
- `dbt_generator.py` - Update bronze model generation
- `dbt_macros/` - Add S3-specific macros

**Tasks:**
1. Update `_generate_bronze_model()` for S3 sources:
   ```sql
   -- stg_customers.sql
   {{ config(materialized='table') }}

   with source as (
       select * from {{ source('s3_raw', 'customers') }}
   ),

   -- Data quality checks for S3 ingestion
   validated as (
       select
           *,
           _file as source_file,
           _modified as source_modified_at
       from source
       where _file is not null  -- Filter incomplete uploads
   )

   select * from validated
   ```

2. Create new DBT macro `dbt_macros/s3_source.sql`:
   ```sql
   {% macro s3_source(bucket, prefix, format='parquet', columns=none) %}
       -- Trino S3 source macro with format handling
   {% endmacro %}
   ```

3. Add S3-specific metadata columns:
   - `_file` - Source file path
   - `_modified` - Last modified timestamp
   - `_size` - File size (optional)

4. Generate setup SQL for S3 schema:
   ```sql
   -- setup/create_s3_schema.sql
   CREATE SCHEMA IF NOT EXISTS hive.s3_raw
   WITH (location = 's3a://my-bucket/raw/');
   ```

**Acceptance Criteria:**
- Bronze models properly reference S3 sources
- S3 metadata columns included for lineage
- Setup scripts for Trino schema creation

---

### Session 6: Documentation & Validation
**Goal:** Document S3 sources and add validation tests

**Files to modify:**
- `doc_generator.py` - Add S3 documentation
- `quality_validator.py` - Add S3 validation tests

**Tasks:**
1. Update `_generate_sources_doc()` for S3:
   ```markdown
   ## S3 Data Sources

   | Source | Bucket | Prefix | Format | Schema |
   |--------|--------|--------|--------|--------|
   | customers | my-bucket | data/customers/ | parquet | s3_raw |
   ```

2. Add S3 setup guide to `index.md`:
   - Trino S3 connector configuration
   - AWS credentials setup
   - S3-compatible endpoint configuration (MinIO, etc.)

3. Add S3 validation tests to `quality_validator.py`:
   ```python
   def generate_s3_validation_tests(self, s3_mappings):
       """Generate tests for S3 source accessibility and schema."""
       # Test: Can read from S3 location
       # Test: Schema matches expected columns
       # Test: File format is valid
   ```

4. Generate connection test SQL:
   ```sql
   -- tests/s3_connection_test.sql
   SELECT count(*) FROM hive.s3_raw.customers LIMIT 1;
   ```

**Acceptance Criteria:**
- S3 sources documented with setup instructions
- Validation tests for S3 connectivity
- Clear error messages for S3 configuration issues

---

### Session 7: Integration & Testing
**Goal:** End-to-end integration and comprehensive testing

**Files to modify:**
- `main.py` - Final integration
- `tests/` - Add S3 integration tests

**Tasks:**
1. Wire everything in `main.py`:
   ```python
   # Initialize S3 resolver
   s3_resolver = S3ConfigResolver(
       interactive=not args.non_interactive,
       default_bucket=args.s3_bucket,
       default_region=args.s3_region,
       endpoint=args.s3_endpoint,
   )

   if args.s3_config:
       s3_resolver.load_from_file(args.s3_config)

   # Pass to analyzer and generator
   analyzer = TransformationAnalyzer(workflow, s3_resolver=s3_resolver)
   ```

2. Add sample S3 config file:
   ```json
   {
     "default_region": "us-east-1",
     "endpoint": null,
     "mappings": {
       "customers.csv": {
         "bucket": "my-data-lake",
         "prefix": "bronze/customers/",
         "format": "parquet"
       }
     }
   }
   ```

3. Create test fixtures:
   - Mock S3 responses for unit tests
   - Sample workflows with S3 sources
   - Expected output validation

4. Add integration test:
   ```python
   def test_full_s3_workflow():
       """Test complete workflow with S3 source replacement."""
       # Parse workflow
       # Replace sources with S3
       # Generate DBT project
       # Validate output structure
   ```

5. Update README/CLAUDE.md with S3 usage examples

**Acceptance Criteria:**
- Full end-to-end workflow working
- All unit tests passing
- Documentation updated with S3 usage

---

## File Change Summary

| File | Changes |
|------|---------|
| `models.py` | Add `S3SourceConfig`, `S3SourceMapping` dataclasses |
| `s3_config.py` | **NEW** - S3 configuration resolver with interactive prompts |
| `main.py` | Add S3 CLI arguments, wire S3 resolver |
| `transformation_analyzer.py` | Add `replace_sources_with_s3()` method |
| `alteryx_parser.py` | Minor updates for S3 source detection |
| `dbt_generator.py` | S3 source generation, bronze model updates |
| `trino_s3_templates.py` | **NEW** - Trino external table SQL templates |
| `doc_generator.py` | S3 documentation generation |
| `quality_validator.py` | S3 validation tests |
| `dbt_macros/s3_source.sql` | **NEW** - S3 source DBT macro |

---

## CLI Usage (Final)

```bash
# Interactive mode - prompts for each source
python main.py analyze ./workflows --generate-dbt ./dbt

# With default S3 bucket
python main.py analyze ./workflows --generate-dbt ./dbt \
    --s3-bucket my-data-lake \
    --s3-region us-east-1

# With S3-compatible endpoint (MinIO)
python main.py analyze ./workflows --generate-dbt ./dbt \
    --s3-endpoint http://minio.local:9000

# Non-interactive with config file
python main.py analyze ./workflows --generate-dbt ./dbt \
    --s3-config s3_mappings.json \
    --non-interactive
```

---

## Verification Plan

After implementation, verify by:

1. **Unit tests:** `python tests/test_s3_config.py`
2. **Integration test:** Run against sample workflow with S3 sources
3. **Manual test:**
   - Run in interactive mode, provide S3 metadata
   - Verify generated DBT project structure
   - Validate Trino DDL syntax
   - Check documentation includes S3 setup guide

---

## Dependencies

- No new external Python dependencies (standard library only)
- Trino/Starburst with S3 connector (target environment)
- AWS credentials or S3-compatible endpoint (runtime)
