# Alteryx to Starburst/DBT Migration Tool

A Python CLI tool that parses Alteryx workflows (.yxmd files) and generates:
1. **Comprehensive documentation** with Mermaid diagrams
2. **DBT project scaffolding** organized by medallion layers (Bronze → Silver → Gold)
3. **S3-compatible data source mappings** for Trino/Starburst

Facilitates migration from Alteryx ETL to **Starburst (Trino-based)** ELT architecture.

## Target Platform

- **Data Platform**: [Starburst](https://www.starburst.io/) (enterprise Trino distribution)
- **Transformation Layer**: [dbt](https://www.getdbt.com/) (data build tool)
- **SQL Dialect**: Trino SQL syntax

## Features

### Core Capabilities
- **Workflow Parsing**: Extracts nodes, connections, and configurations from Alteryx XML files
- **Data Lineage**: Builds flow diagrams showing data transformations with topological ordering
- **Macro Handling**: Recursively parses macros with interactive prompts for missing ones
- **Markdown Documentation**: Generates comprehensive docs with Mermaid diagrams
- **DBT Scaffolding**: Creates starter DBT models organized by medallion layers
- **Column Detection**: Automatic column header extraction from CSV, Excel, JSON, and Parquet files
- **Structured Logging**: Configurable logging with verbose, quiet, and file output modes

### Macro-First Architecture
- **23 Reusable DBT Macros**: Pre-built macros covering 85%+ of Alteryx tools
- **Macro Call Generation**: Generated models use `{{ macro_name(params) }}` instead of raw SQL
- **Maintainable Output**: Changes to SQL logic happen in macros, not generated code
- **Automatic Copying**: Macros are copied to generated DBT projects

### S3 Source Integration
- **S3 Source Mapping**: Replace Alteryx file/database sources with S3 bucket locations
- **External Table DDL**: Generates Trino CREATE TABLE statements for S3 data
- **Interactive Resolution**: Prompts for S3 locations or uses config files
- **S3 Setup Guide**: Generates Trino/Starburst S3 configuration documentation
- **S3 Config Generator**: Interactive wizard to create `s3_mappings.json` configuration files

### Formula Conversion
- **60+ Function Mappings**: Converts Alteryx formulas to Trino SQL
- **String, Math, Date Functions**: Comprehensive coverage of common functions
- **Conditional Logic**: `IIF()` → `CASE WHEN`, `IsNull()`, `Coalesce()`, etc.

### Quality Validation
- **Parallel Validation Tests**: Record count comparison between Alteryx and DBT outputs
- **Null Completeness Checks**: Per-column validation
- **S3 Connectivity Tests**: Generated test scripts for S3 sources

## Installation

No external dependencies required for core functionality - uses only Python standard library (Python 3.7+).

Optional dependencies for enhanced column detection:
- `openpyxl` - Excel .xlsx file support
- `xlrd` - Excel .xls file support
- `pyarrow` - Parquet file support

```bash
# Clone the repository
git clone <repository-url>
cd AlteryxToStarburstMigration

# Run directly
python main.py --help

# Optional: install extras for column detection
pip install openpyxl pyarrow
```

## Usage

### Basic Usage

```bash
# Analyze workflows in current directory
python main.py analyze .

# Analyze a specific workflow
python main.py analyze workflow.yxmd

# Analyze folder recursively
python main.py analyze ./workflows --recursive
```

### Generate Documentation and DBT Project

```bash
# Output documentation to specific directory
python main.py analyze ./workflows --output ./docs

# Generate both documentation and DBT project
python main.py analyze ./workflows --output ./docs --generate-dbt ./dbt_project
```

### S3 Source Integration

```bash
# Use default S3 bucket for all sources
python main.py analyze . --s3-bucket my-data-lake --generate-dbt ./dbt

# Use S3 configuration file with mappings
python main.py analyze . --s3-config s3_mappings.json --generate-dbt ./dbt

# Specify S3 credentials and endpoint (for MinIO, etc.)
python main.py analyze . --s3-bucket my-bucket --s3-user access-key --s3-password secret-key --s3-endpoint http://minio:9000
```

### Generate S3 Mappings Configuration

Use the interactive wizard to create `s3_mappings.json` configuration files:

```bash
# Run the interactive wizard
python main.py generate-s3-config

# Scan workflows first to discover sources
python main.py generate-s3-config --scan ./workflows

# Specify output file and default bucket
python main.py generate-s3-config -o my_mappings.json --bucket my-data-lake

# Pre-configure credentials
python main.py generate-s3-config --s3-user access-key --s3-password secret-key
```

The wizard guides you through:
1. **Configure defaults** - Set default bucket, format, endpoint, and credentials
2. **Discover sources** - Scan Alteryx workflows to find data sources
3. **Map sources to S3** - Configure S3 locations for each source
4. **Add wildcard patterns** - Define patterns like `*.csv` for bulk mappings

### Macro Handling

```bash
# Pre-specify macro directories (avoids prompts)
python main.py analyze . --macro-dir ./shared_macros --macro-dir ./team_macros

# Non-interactive mode (skip prompts, use config files)
python main.py analyze . --non-interactive
```

### Logging Options

```bash
# Verbose output (debug-level logging)
python main.py analyze . --verbose

# Quiet mode (warnings and errors only)
python main.py analyze . --quiet

# Log to file
python main.py analyze . --log-file ./migration.log

# Combine: quiet console + verbose file logging
python main.py analyze . --quiet --log-file ./migration.log
```

### Full Example

```bash
python main.py analyze ./workflows \
    --recursive \
    --output ./migration_docs \
    --generate-dbt ./dbt_project \
    --macro-dir ./macros \
    --s3-config ./s3_mappings.json \
    --default-schema raw \
    --non-interactive \
    --verbose \
    --log-file ./migration.log
```

## CLI Arguments

### `analyze` Command

| Argument | Description |
|----------|-------------|
| `path` | Path to workflow file or directory |
| `--recursive`, `-r` | Recursively scan directories |
| `--output`, `-o` | Output directory for documentation |
| `--generate-dbt` | Generate DBT project scaffolding |
| `--macro-dir` | Additional directories to search for macros (repeatable) |
| `--non-interactive` | Skip interactive prompts |
| `--verbose`, `-v` | Enable verbose/debug output |
| `--quiet`, `-q` | Suppress info messages, only show warnings and errors |
| `--log-file` | Write logs to specified file (in addition to console) |
| `--default-schema` | Default schema name for sources (default: `raw`) |
| `--validate` | Validate generated SQL by running `dbt compile` |
| `--s3-bucket` | Default S3 bucket for all sources |
| `--s3-config` | JSON file with S3 source mappings |
| `--s3-endpoint` | S3 endpoint URL (for S3-compatible services) |
| `--s3-user` | S3 access key / username for authentication |
| `--s3-password` | S3 secret key / password for authentication |

### `generate-s3-config` Command

| Argument | Description |
|----------|-------------|
| `-o`, `--output` | Output file path (default: `s3_mappings.json`) |
| `--scan` | Workflow paths to scan for source discovery (repeatable) |
| `--bucket` | Pre-set default S3 bucket name |
| `--extend` | Extend existing config file instead of creating new |
| `--format` | Pre-set default file format: `parquet`, `csv`, `json`, `orc`, `avro` (default: `parquet`) |
| `--endpoint` | Pre-set S3-compatible endpoint URL |
| `--s3-user` | Pre-set S3 access key / username |
| `--s3-password` | Pre-set S3 secret key / password |
| `--verbose`, `-v` | Enable verbose/debug output |
| `--quiet`, `-q` | Suppress info messages, only show warnings and errors |

## S3 Configuration File

Create a JSON file to map Alteryx sources to S3 locations:

```json
{
  "default_bucket": "my-data-lake",
  "endpoint": null,
  "s3_user": "your-access-key",
  "s3_password": "your-secret-key",
  "mappings": {
    "customers.csv": {
      "bucket": "my-data-lake",
      "prefix": "bronze/customers/",
      "format": "parquet"
    },
    "*.csv": {
      "bucket": "my-data-lake",
      "prefix": "bronze/misc/",
      "format": "csv"
    }
  }
}
```

See `samples/s3_config.json` for a complete example.

## Output Structure

### Documentation (`--output`)

```
docs/
├── index.md                    # Overview with workflow summary and TODO guide
├── workflows/
│   └── workflow_name.md        # Per-workflow documentation with Mermaid diagrams
├── sources.md                  # All data sources inventory
├── targets.md                  # All output targets
├── macros.md                   # Macro inventory
├── medallion_mapping.md        # Suggested DBT layer assignments
└── s3_setup_guide.md           # S3/Trino configuration guide (if S3 enabled)
```

### DBT Project (`--generate-dbt`)

```
dbt_project/
├── dbt_project.yml
├── models/
│   ├── staging/                # Bronze layer (stg_*)
│   │   ├── _sources.yml        # Source definitions with S3 metadata
│   │   └── stg_*.sql
│   ├── intermediate/           # Silver layer (int_*)
│   │   └── int_*.sql
│   └── marts/                  # Gold layer
│       ├── core/               # Fact tables (fct_*)
│       └── dimensions/         # Dimension tables (dim_*)
├── macros/                     # 23 reusable DBT macros
│   ├── aggregation.sql
│   ├── filter_helpers.sql
│   ├── join_union.sql
│   ├── formula_helpers.sql
│   ├── s3_source.sql
│   └── ...
├── setup/                      # S3 setup scripts (if S3 enabled)
│   ├── create_s3_tables.sql    # Trino external table DDL
│   └── test_s3_connections.sql # S3 connectivity tests
└── tests/
    └── s3_validation/          # S3 validation tests
```

## Supported Alteryx Tools

### Tools with Macro Mappings (31 tools, 85%+ coverage)

| Category | Tools |
|----------|-------|
| **Input/Output** | Input Data, Text Input, Output Data |
| **Preparation** | Filter, Formula, Select, Sort, Sample, Unique, Data Cleansing, Auto Field |
| **Join** | Join, Union, Append Fields, Join Multiple |
| **Transform** | Summarize, Transpose, Cross Tab, Tile, Arrange |
| **Parse** | RegEx, Text To Columns, DateTime Parse, JSON Parse |
| **Row Operations** | Multi-Field Formula, Multi-Row Formula, Running Total, Generate Rows |
| **Data Quality** | Find Replace, Imputation, Select Records, Weighted Average |
| **Macros** | Standard macros (.yxmc) |

### DBT Macros Included

23 macro files covering common transformations:

- `aggregation.sql` - Summarize tool macros
- `filter_helpers.sql` - Filter operations
- `join_union.sql` - Join and Union operations
- `formula_helpers.sql` - Formula calculations
- `select_transform.sql` - Select, Sort, RecordID
- `deduplicate.sql` - Unique tool
- `regex_functions.sql` - RegEx operations
- `s3_source.sql` - S3 source reading
- And 15 more...

## Medallion Architecture Mapping

| Alteryx Component | DBT Layer | Model Prefix | Starburst Schema |
|-------------------|-----------|--------------|------------------|
| Input tools | Bronze (Staging) | `stg_` | `staging` |
| Transformations | Silver (Intermediate) | `int_` | `intermediate` |
| Final outputs | Gold (Marts) | `fct_` / `dim_` | `marts` |

## Starburst/Trino SQL Features Used

The generated DBT models use Trino-compatible SQL syntax:

- **Window Functions**: `ROW_NUMBER()`, `LAG()`, `LEAD()`, `SUM() OVER()`
- **Array Operations**: `UNNEST()`, `ARRAY[]`, `SPLIT()`
- **String Functions**: `REGEXP_EXTRACT()`, `REGEXP_REPLACE()`, `TRIM()`
- **Conditional**: `CASE WHEN`, `COALESCE()`, `NULLIF()`
- **Joins**: `LEFT/RIGHT/INNER/FULL/CROSS JOIN`

## Formula Conversion

The tool converts Alteryx formulas to Trino SQL (60+ function mappings):

| Category | Alteryx Functions | Trino Equivalent |
|----------|-------------------|------------------|
| **String** | `Trim()`, `Left()`, `Right()`, `Replace()` | `TRIM()`, `SUBSTR()`, `REPLACE()` |
| **Math** | `Abs()`, `Ceil()`, `Floor()`, `Round()` | `ABS()`, `CEIL()`, `FLOOR()`, `ROUND()` |
| **Date** | `DateTimeNow()`, `DateTimeYear()` | `CURRENT_TIMESTAMP`, `YEAR()` |
| **Conditional** | `IIF()`, `IsNull()`, `IsEmpty()` | `CASE WHEN`, `IS NULL` |

## Interactive Macro Resolution

When a macro cannot be found automatically, you'll be prompted:

```
============================================================
Macro not found: "DataValidator.yxmc"
Referenced in: customer_orders.yxmd
============================================================

Options:
[1] Enter path to macro file
[2] Enter directory containing macros
[3] Skip this macro (document as missing)
[4] Skip all missing macros

Your choice: _
```

The tool searches for macros in:
1. Path specified in workflow XML
2. Same directory as the workflow
3. `macros/` subdirectory
4. Previously provided paths (cached during session)

## Project Structure

```
AlteryxToStarburstMigration/
├── main.py                       # CLI entry point
├── alteryx_parser.py             # XML parsing of .yxmd/.yxmc files
├── transformation_analyzer.py    # Data lineage & flow analysis
├── macro_handler.py              # Macro resolution
├── doc_generator.py              # Markdown documentation generation
├── dbt_generator.py              # DBT project scaffolding (macro-first)
├── tool_mappings.py              # Alteryx → SQL/DBT mappings
├── macro_mappings.py             # Alteryx tool → DBT macro mappings
├── formula_converter.py          # Alteryx formula → Trino SQL conversion
├── quality_validator.py          # Parallel validation tests
├── models.py                     # Data classes & enums
├── column_detector.py            # Column header detection (CSV, Excel, JSON, Parquet)
├── logging_config.py             # Structured logging configuration
├── s3_config.py                  # S3 source configuration
├── s3_mappings_generator.py      # Interactive S3 config wizard
├── trino_s3_templates.py         # Trino external table templates
├── dbt_macros/                   # 23 reusable DBT macros
├── tests/                        # Test suite
│   ├── conftest.py               # Shared test fixtures
│   ├── test_source_columns.py
│   ├── test_formula_converter.py
│   ├── test_s3_config.py
│   ├── test_s3_integration.py
│   ├── test_s3_mappings_generator.py
│   └── test_trino_s3_templates.py
└── samples/                      # Sample workflows and configs
    ├── *.yxmd
    ├── s3_config.json
    ├── s3_mappings.json
    └── macros/
```

## Running Tests

```bash
# Run all tests
python tests/test_source_columns.py
python tests/test_formula_converter.py
python tests/test_s3_config.py
python tests/test_s3_integration.py
python tests/test_s3_mappings_generator.py
python tests/test_trino_s3_templates.py
```

## Example

```bash
# Full migration workflow
python main.py analyze ./samples \
    --recursive \
    --output ./test_docs \
    --generate-dbt ./test_dbt \
    --macro-dir ./samples/macros \
    --s3-bucket my-data-lake \
    --non-interactive
```

## Contributing

1. Add new tool mappings in `tool_mappings.py`
2. Add macro mappings in `macro_mappings.py`
3. Create new DBT macros in `dbt_macros/`
4. Extend parsing logic in `alteryx_parser.py`
5. Add formula conversions in `formula_converter.py`

## License

MIT
