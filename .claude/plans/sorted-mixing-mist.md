# Refactoring Plan: Split DBTGenerator God Class

## Overview
Split the 3,278-line `DBTGenerator` class (71 methods) into focused, testable components.

## Current State Analysis

**Total:** 71 methods across 13 responsibility areas
- Bronze/Staging: 3 methods (~182 lines)
- Silver/Intermediate: 6 methods (~342 lines)
- Gold/Marts: 1 method (~110 lines)
- Sources: 2 methods (~100 lines)
- S3 Handler: 3 methods (~129 lines)
- Column Resolution: 19 methods (~501 lines) - **CRITICAL DEPENDENCY**
- Macro Generation: 11 methods (~472 lines)
- Transformation SQL: 2 methods (~293 lines)
- Formula Conversion: 6 methods (~177 lines)
- TODO Tracking: 4 methods (~145 lines)
- Validation: 3 methods (~166 lines)
- Project Config: 5 methods (~245 lines)
- Orchestration: 2 methods (~48 lines)

## Target Architecture

```
dbt_generator/
├── __init__.py              # DBTGenerator facade (public API)
├── orchestrator.py          # Main coordination (~150 lines)
├── column_resolver.py       # Column detection/caching (~500 lines)
├── transformation_sql.py    # SQL generation (~500 lines)
├── generators/
│   ├── __init__.py
│   ├── bronze.py            # Bronze/staging models (~220 lines)
│   ├── silver.py            # Silver/intermediate (~350 lines)
│   ├── gold.py              # Gold/marts (~150 lines)
│   ├── sources.py           # Sources YAML (~160 lines)
│   ├── macros.py            # Macro generation (~400 lines)
│   ├── validation.py        # Tests/validation (~200 lines)
│   └── project.py           # Project config (~200 lines)
├── handlers/
│   ├── __init__.py
│   ├── s3_source.py         # S3 handling (~150 lines)
│   └── todo_manager.py      # TODO tracking (~150 lines)
└── utils/
    ├── __init__.py
    ├── expression.py        # Formula conversion (~250 lines)
    └── naming.py            # Name sanitization (~50 lines)
```

## Implementation Phases

### Phase 1: Foundation (Low Risk)
Extract self-contained components with no circular dependencies.

**1.1 TodoManager** - `handlers/todo_manager.py`
- Methods: `_add_todo`, `_calculate_todo_priority`, `_add_missing_macro_todos`, `get_todos_summary`
- State: `todos: List[TodoItem]`, `_current_model_name`, `_current_layer`

**1.2 ExpressionConverter** - `utils/expression.py`
- Methods: `_convert_expression`, `_convert_iif_to_case`, `_convert_isnull`, `_convert_isempty`
- Note: Consider consolidating with existing `FormulaConverter` class

**1.3 S3SourceHandler** - `handlers/s3_source.py`
- Methods: `_generate_bronze_model_s3`, `generate_s3_setup_sql`, `get_s3_source_summary`
- State: `s3_sources: Dict[str, SourceInfo]`

### Phase 2: Core Infrastructure
Extract foundational components that others depend on.

**2.1 ColumnResolver** - `column_resolver.py`
- 19 methods including `_get_node_columns` (CRITICAL - called by 15+ methods)
- State: `_node_columns: Dict[int, List[str]]`, `_column_detector`
- Interface: `get_columns(node, workflow) -> List[str]`

**2.2 TransformationSQLGenerator** - `transformation_sql.py`
- Methods: `_generate_transformation_sql`, `_generate_macro_call_sql`, `_generate_transformation_sql_legacy`
- Depends on: ColumnResolver, FormulaConverter

### Phase 3: Layer Generators
Extract model generation by medallion layer.

**3.1 BronzeModelGenerator** - `generators/bronze.py`
**3.2 SilverModelGenerator** - `generators/silver.py`
**3.3 GoldModelGenerator** - `generators/gold.py`

### Phase 4: Supporting Generators

**4.1 SourcesGenerator** - `generators/sources.py`
**4.2 MacroGenerator** - `generators/macros.py`
**4.3 ValidationGenerator** - `generators/validation.py`
**4.4 ProjectGenerator** - `generators/project.py`

### Phase 5: Orchestrator Refactoring
Refactor main class to delegate to components.

**5.1 DBTGeneratorOrchestrator** - `orchestrator.py`
**5.2 DBTGenerator Facade** - `__init__.py` (maintains backward compatibility)

## Shared Context Object

```python
@dataclass
class GeneratorContext:
    output_dir: Path
    project_name: str
    interactive: bool
    default_schema: str
    sources: Dict[str, Dict[str, SourceInfo]]
    models_info: Dict[str, ModelInfo]
    current_workflow: Optional[AlteryxWorkflow]
    column_resolver: ColumnResolver
    todo_manager: TodoManager
    formula_converter: FormulaConverter
```

## Files to Modify/Create

**Create:**
- `dbt_generator/__init__.py`
- `dbt_generator/orchestrator.py`
- `dbt_generator/column_resolver.py`
- `dbt_generator/transformation_sql.py`
- `dbt_generator/generators/*.py` (7 files)
- `dbt_generator/handlers/*.py` (2 files)
- `dbt_generator/utils/*.py` (2 files)

**Modify:**
- `dbt_generator.py` - Convert to facade importing from package

**Update imports in:**
- `main.py`
- `doc_generator.py`

## Verification Plan

1. Run existing test suite:
   ```bash
   python tests/test_source_columns.py
   python tests/test_s3_integration.py
   ```

2. End-to-end generation test:
   ```bash
   python main.py analyze samples/customer_orders.yxmd --generate-dbt ./test_dbt --non-interactive
   ```

3. Compare output: Diff generated DBT project before/after refactoring

## Risk Mitigation

1. **Backward Compatibility:** Keep `dbt_generator.py` as facade
2. **Incremental:** Each phase can be merged independently
3. **Feature Flags:** Add `USE_NEW_GENERATOR` flag for gradual rollout
