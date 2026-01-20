# Alteryx to Starburst/DBT Migration Tool

A Python tool that parses Alteryx workflows (.yxmd files) and generates documentation and DBT models to facilitate migration from Alteryx ETL to **Starburst (Trino-based)** ELT architecture with medallion pattern (Bronze → Silver → Gold).

## Target Platform

- **Data Platform**: [Starburst](https://www.starburst.io/) (enterprise Trino distribution)
- **Transformation Layer**: [dbt](https://www.getdbt.com/) (data build tool)
- **SQL Dialect**: Trino SQL syntax

## Features

- **Workflow Parsing**: Extracts nodes, connections, and configurations from Alteryx XML files
- **Data Lineage**: Builds flow diagrams showing data transformations
- **Macro Handling**: Recursively parses macros with interactive prompts for missing ones
- **Markdown Documentation**: Generates comprehensive docs with Mermaid diagrams
- **DBT Scaffolding**: Creates starter DBT models organized by medallion layers
- **Portable**: Runs from any directory containing workflows

## Installation

No external dependencies required - uses only Python standard library (Python 3.7+).

```bash
# Clone or copy the tool
cd alteryx_to_dbt

# Run directly
python main.py --help
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

### Generate Documentation

```bash
# Output to specific directory
python main.py analyze ./workflows --output ./docs

# Also generate DBT project scaffolding
python main.py analyze ./workflows --output ./docs --generate-dbt ./dbt_project
```

### Macro Handling

```bash
# Pre-specify macro directories (avoids prompts)
python main.py analyze . --macro-dir ./shared_macros --macro-dir ./team_macros

# Non-interactive mode (skip missing macros)
python main.py analyze . --non-interactive
```

### Full Example

```bash
python main.py analyze ./workflows \
    --recursive \
    --output ./migration_docs \
    --generate-dbt ./dbt_project \
    --macro-dir ./macros \
    --verbose
```

## Output Structure

### Documentation (`--output`)

```
docs/
├── index.md                    # Overview with workflow summary
├── workflows/
│   └── workflow_name.md        # Per-workflow documentation
├── sources.md                  # All data sources inventory
├── targets.md                  # All output targets
├── macros.md                   # Macro inventory
└── medallion_mapping.md        # Suggested DBT layer assignments
```

### DBT Project (`--generate-dbt`)

```
dbt_project/
├── dbt_project.yml
├── models/
│   ├── staging/           # Bronze layer (stg_*)
│   │   ├── _sources.yml
│   │   └── stg_*.sql
│   ├── intermediate/      # Silver layer (int_*)
│   │   └── int_*.sql
│   └── marts/             # Gold layer
│       ├── core/          # Fact tables (fct_*)
│       └── dimensions/    # Dimension tables (dim_*)
└── models/_schema.yml
```

## Supported Alteryx Tools

| Category | Tools |
|----------|-------|
| Input | Input Data, Text Input, Database connections |
| Output | Output Data, Browse |
| Preparation | Filter, Formula, Select, Sort, Sample, Unique, Data Cleansing |
| Join | Join, Union, Append Fields |
| Transform | Summarize, Transpose, Cross Tab |
| Parse | RegEx, Text To Columns, DateTime |
| Macros | Standard macros (.yxmc) |

## Medallion Architecture Mapping

| Alteryx Component | DBT Layer | Model Prefix | Starburst Schema |
|-------------------|-----------|--------------|------------------|
| Input tools | Bronze (Staging) | `stg_` | `staging` |
| Transformations | Silver (Intermediate) | `int_` | `intermediate` |
| Final outputs | Gold (Marts) | `fct_` / `dim_` | `marts` |

## Starburst/Trino SQL Features Used

The generated dbt models use Trino-compatible SQL syntax:

- **Window Functions**: `ROW_NUMBER()`, `LAG()`, `LEAD()`, `SUM() OVER()`
- **Array Operations**: `UNNEST()`, `ARRAY[]`, `SPLIT()`
- **String Functions**: `REGEXP_EXTRACT()`, `REGEXP_REPLACE()`, `TRIM()`
- **Conditional**: `CASE WHEN`, `COALESCE()`, `NULLIF()`
- **Joins**: `LEFT/RIGHT/INNER/FULL/CROSS JOIN`

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

## Example Documentation Output

See the `test_docs/` directory after running:

```bash
python main.py analyze ./samples --recursive --output ./test_docs --generate-dbt ./test_dbt --macro-dir ./samples/macros --non-interactive
```

## File Structure

```
alteryx_to_dbt/
├── main.py                 # CLI entry point
├── alteryx_parser.py       # XML parsing
├── transformation_analyzer.py  # Data lineage
├── macro_handler.py        # Macro resolution
├── doc_generator.py        # Markdown output
├── dbt_generator.py        # DBT scaffolding
├── tool_mappings.py        # Alteryx → SQL mappings
├── models.py               # Data classes
├── requirements.txt        # Dependencies (minimal)
└── samples/                # Test workflows
```

## Contributing

1. Add new tool mappings in `tool_mappings.py`
2. Extend parsing logic in `alteryx_parser.py`
3. Add SQL translations in `dbt_generator.py`

## License

MIT
