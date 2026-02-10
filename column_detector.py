"""
Column detection utilities for data files.

MED-01 fix: Extracted from dbt_generator.py for better modularity.

Supports reading column headers from:
- CSV files (with automatic delimiter detection)
- Excel files (.xlsx, .xls)
- JSON files (arrays of objects or nested data structures)
- Parquet files (requires pyarrow)

Also supports inferring column data types (as Trino SQL types) by
reading up to 100 rows of data. Null-only columns default to VARCHAR.

Usage:
    from column_detector import ColumnDetector

    detector = ColumnDetector()
    columns = detector.read_file_columns('/path/to/data.csv')
    column_types = detector.infer_column_types('/path/to/data.csv')
"""
import csv
import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Callable

from logging_config import get_logger

# Module logger
logger = get_logger(__name__)


class ColumnDetector:
    """Utility class for detecting column names from data files.

    Supports CSV, Excel, JSON, and Parquet file formats.
    """

    def __init__(self, on_missing_dependency: Optional[Callable[[str, str], None]] = None):
        """Initialize column detector.

        Args:
            on_missing_dependency: Optional callback for missing dependencies.
                                   Called with (dependency_name, context_message).
        """
        self._on_missing_dependency = on_missing_dependency

    def read_file_columns(self, file_path: str) -> List[str]:
        """Read column names from a data file.

        Automatically detects file type from extension and dispatches
        to the appropriate reader.

        Args:
            file_path: Path to the data file.

        Returns:
            List of column names, or empty list if unable to read.
        """
        path = Path(file_path)
        if not path.exists():
            return []

        suffix = path.suffix.lower()
        columns = []

        try:
            if suffix == '.csv':
                columns = self.read_csv_columns(file_path)
            elif suffix in ['.xlsx', '.xls']:
                columns = self.read_excel_columns(file_path)
            elif suffix == '.json':
                columns = self.read_json_columns(file_path)
            elif suffix == '.parquet':
                columns = self.read_parquet_columns(file_path)
            elif suffix in ['.txt', '.tsv']:
                # Try as tab-separated or delimited file
                columns = self.read_csv_columns(file_path, delimiter='\t')
        except Exception as e:
            logger.warning(f"Could not read {file_path}: {e}")

        return columns

    def read_csv_columns(self, file_path: str, delimiter: str = ',') -> List[str]:
        """Read column headers from a CSV file.

        Args:
            file_path: Path to the CSV file.
            delimiter: Field delimiter (default: comma). Auto-detected if possible.

        Returns:
            List of column names from the header row.
        """
        columns = []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                # Try to detect delimiter if comma doesn't work well
                sample = f.read(4096)
                f.seek(0)

                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
                    delimiter = dialect.delimiter
                except csv.Error:
                    pass  # Use default delimiter

                reader = csv.reader(f, delimiter=delimiter)
                header_row = next(reader, None)
                if header_row:
                    columns = [col.strip() for col in header_row if col.strip()]
        except Exception as e:
            logger.warning(f"Error reading CSV {file_path}: {e}")

        return columns

    def read_excel_columns(self, file_path: str) -> List[str]:
        """Read column headers from an Excel file.

        Supports both .xlsx (openpyxl) and .xls (xlrd) formats.

        Args:
            file_path: Path to the Excel file.

        Returns:
            List of column names from the first row of the first sheet.
        """
        columns = []
        try:
            # Try openpyxl for .xlsx
            import openpyxl
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            ws = wb.active
            if ws:
                first_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if first_row:
                    columns = [str(cell).strip() for cell in first_row if cell is not None]
            wb.close()
        except ImportError:
            logger.info("Install openpyxl for Excel support: pip install openpyxl")
            self._report_missing_dependency("openpyxl", file_path)
        except Exception as e:
            # Try xlrd for .xls
            try:
                import xlrd
                wb = xlrd.open_workbook(file_path)
                ws = wb.sheet_by_index(0)
                if ws.nrows > 0:
                    columns = [str(cell.value).strip() for cell in ws.row(0) if cell.value]
            except ImportError:
                logger.info("Install xlrd for .xls support: pip install xlrd")
                self._report_missing_dependency("xlrd", file_path)
            except Exception as e2:
                logger.warning(f"Error reading Excel {file_path}: {e2}")

        return columns

    def read_json_columns(self, file_path: str) -> List[str]:
        """Read column/field names from a JSON file.

        Handles multiple JSON structures:
        - Array of objects: extracts keys from first object
        - Single flat object: extracts all keys
        - Nested with 'data' or 'records' key: extracts from nested array

        Args:
            file_path: Path to the JSON file.

        Returns:
            List of field/column names.
        """
        columns = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Handle different JSON structures
            if isinstance(data, list) and len(data) > 0:
                # Array of objects - get keys from first object
                if isinstance(data[0], dict):
                    columns = list(data[0].keys())
            elif isinstance(data, dict):
                # Single object or nested structure
                if all(isinstance(v, (str, int, float, bool, type(None))) for v in data.values()):
                    # Flat object
                    columns = list(data.keys())
                elif 'data' in data and isinstance(data['data'], list):
                    # Common pattern: {"data": [...]}
                    if len(data['data']) > 0 and isinstance(data['data'][0], dict):
                        columns = list(data['data'][0].keys())
                elif 'records' in data and isinstance(data['records'], list):
                    # Common pattern: {"records": [...]}
                    if len(data['records']) > 0 and isinstance(data['records'][0], dict):
                        columns = list(data['records'][0].keys())
        except Exception as e:
            logger.warning(f"Error reading JSON {file_path}: {e}")

        return columns

    def read_parquet_columns(self, file_path: str) -> List[str]:
        """Read column names from a Parquet file.

        Requires pyarrow to be installed.

        Args:
            file_path: Path to the Parquet file.

        Returns:
            List of column names from the Parquet schema.
        """
        columns = []
        try:
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(file_path)
            columns = parquet_file.schema.names
        except ImportError:
            logger.warning(f"Cannot read Parquet columns from {file_path}")
            logger.warning("         pyarrow is not installed. Install with: pip install pyarrow")
            logger.warning("         Columns will need to be specified manually in generated models.")
            self._report_missing_dependency("pyarrow", file_path)
        except Exception as e:
            logger.warning(f"Error reading Parquet {file_path}: {e}")

        return columns

    # ----------------------------------------------------------------
    # Data type inference (reads up to 100 rows)
    # ----------------------------------------------------------------

    # Maximum number of data rows to sample for type inference
    MAX_SAMPLE_ROWS = 100

    # Regex patterns for date/datetime detection
    _DATE_PATTERNS = [
        re.compile(r'^\d{4}-\d{2}-\d{2}$'),                # YYYY-MM-DD
        re.compile(r'^\d{2}/\d{2}/\d{4}$'),                # MM/DD/YYYY
        re.compile(r'^\d{2}-\d{2}-\d{4}$'),                # DD-MM-YYYY
        re.compile(r'^\d{4}/\d{2}/\d{2}$'),                # YYYY/MM/DD
    ]
    _TIMESTAMP_PATTERNS = [
        re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}'),  # ISO-like
        re.compile(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}'),            # MM/DD/YYYY HH:MM
    ]

    # Mapping from pyarrow type strings to Trino SQL types
    _PYARROW_TO_TRINO = {
        'bool': 'BOOLEAN',
        'int8': 'TINYINT',
        'int16': 'SMALLINT',
        'int32': 'INTEGER',
        'int64': 'BIGINT',
        'uint8': 'SMALLINT',
        'uint16': 'INTEGER',
        'uint32': 'BIGINT',
        'uint64': 'BIGINT',
        'float16': 'REAL',
        'float': 'REAL',
        'float32': 'REAL',
        'double': 'DOUBLE',
        'float64': 'DOUBLE',
        'string': 'VARCHAR',
        'utf8': 'VARCHAR',
        'large_string': 'VARCHAR',
        'large_utf8': 'VARCHAR',
        'binary': 'VARBINARY',
        'large_binary': 'VARBINARY',
        'date32': 'DATE',
        'date64': 'DATE',
        'date32[day]': 'DATE',
        'timestamp[ns]': 'TIMESTAMP',
        'timestamp[us]': 'TIMESTAMP',
        'timestamp[ms]': 'TIMESTAMP',
        'timestamp[s]': 'TIMESTAMP',
        'time32[ms]': 'TIME',
        'time64[us]': 'TIME',
        'time64[ns]': 'TIME',
    }

    def infer_column_types(self, file_path: str) -> Dict[str, str]:
        """Infer column data types from a data file by sampling up to 100 rows.

        Automatically detects file type from extension and dispatches
        to the appropriate inference method.

        Null-only columns default to VARCHAR to avoid wrong type assignment.

        Args:
            file_path: Path to the data file.

        Returns:
            Dict mapping column names to Trino SQL type strings.
            Empty dict if unable to infer.
        """
        path = Path(file_path)
        if not path.exists():
            return {}

        suffix = path.suffix.lower()
        column_types: Dict[str, str] = {}

        try:
            if suffix == '.csv':
                column_types = self.infer_csv_column_types(file_path)
            elif suffix == '.json':
                column_types = self.infer_json_column_types(file_path)
            elif suffix == '.parquet':
                column_types = self.infer_parquet_column_types(file_path)
            elif suffix in ['.txt', '.tsv']:
                column_types = self.infer_csv_column_types(file_path, delimiter='\t')
        except Exception as e:
            logger.warning(f"Could not infer column types from {file_path}: {e}")

        return column_types

    def infer_csv_column_types(self, file_path: str, delimiter: str = ',') -> Dict[str, str]:
        """Infer column data types from a CSV file by sampling up to 100 rows.

        For each column, examines non-null/non-empty values and determines
        the most specific Trino type that fits all values.

        Inference priority: BOOLEAN > INTEGER > BIGINT > DOUBLE > DATE > TIMESTAMP > VARCHAR

        Args:
            file_path: Path to the CSV file.
            delimiter: Field delimiter (auto-detected if possible).

        Returns:
            Dict mapping column names to Trino SQL type strings.
        """
        column_types: Dict[str, str] = {}
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                sample = f.read(4096)
                f.seek(0)

                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
                    delimiter = dialect.delimiter
                except csv.Error:
                    pass

                reader = csv.reader(f, delimiter=delimiter)
                header_row = next(reader, None)
                if not header_row:
                    return {}

                columns = [col.strip() for col in header_row if col.strip()]
                if not columns:
                    return {}

                # Collect values per column (up to MAX_SAMPLE_ROWS)
                col_values: Dict[str, list] = {col: [] for col in columns}
                for i, row in enumerate(reader):
                    if i >= self.MAX_SAMPLE_ROWS:
                        break
                    for j, col in enumerate(columns):
                        if j < len(row):
                            val = row[j].strip()
                            col_values[col].append(val)
                        else:
                            col_values[col].append('')

                # Infer type for each column
                for col in columns:
                    column_types[col] = self._infer_type_from_string_values(col_values[col])

        except Exception as e:
            logger.warning(f"Error inferring CSV column types from {file_path}: {e}")

        return column_types

    def infer_json_column_types(self, file_path: str) -> Dict[str, str]:
        """Infer column data types from a JSON file by examining up to 100 records.

        Uses Python's native JSON types for initial classification, then
        applies string-based inference for string values (dates, etc.).

        Args:
            file_path: Path to the JSON file.

        Returns:
            Dict mapping column names to Trino SQL type strings.
        """
        column_types: Dict[str, str] = {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            records = self._extract_json_records(data)
            if not records:
                return {}

            # Limit to MAX_SAMPLE_ROWS
            records = records[:self.MAX_SAMPLE_ROWS]

            # Collect all column names across sampled records
            all_columns: list = []
            for record in records:
                if isinstance(record, dict):
                    for key in record.keys():
                        if key not in all_columns:
                            all_columns.append(key)

            # Collect non-null values per column with their Python types
            col_values: Dict[str, list] = {col: [] for col in all_columns}
            for record in records:
                if not isinstance(record, dict):
                    continue
                for col in all_columns:
                    val = record.get(col)
                    col_values[col].append(val)

            # Infer types
            for col in all_columns:
                column_types[col] = self._infer_type_from_python_values(col_values[col])

        except Exception as e:
            logger.warning(f"Error inferring JSON column types from {file_path}: {e}")

        return column_types

    def infer_parquet_column_types(self, file_path: str) -> Dict[str, str]:
        """Infer column data types from a Parquet file schema.

        Parquet files have explicit schema types which are mapped to Trino types.

        Args:
            file_path: Path to the Parquet file.

        Returns:
            Dict mapping column names to Trino SQL type strings.
        """
        column_types: Dict[str, str] = {}
        try:
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema_arrow

            for i in range(len(schema)):
                field = schema.field(i)
                col_name = field.name
                pa_type = str(field.type)
                trino_type = self._map_pyarrow_type_to_trino(pa_type)
                column_types[col_name] = trino_type

        except ImportError:
            logger.warning(f"Cannot infer Parquet column types from {file_path}")
            logger.warning("         pyarrow is not installed. Install with: pip install pyarrow")
            self._report_missing_dependency("pyarrow", file_path)
        except Exception as e:
            logger.warning(f"Error inferring Parquet column types from {file_path}: {e}")

        return column_types

    def _extract_json_records(self, data) -> list:
        """Extract a list of record dicts from various JSON structures."""
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            return data
        elif isinstance(data, dict):
            if 'data' in data and isinstance(data['data'], list):
                records = data['data']
                if records and isinstance(records[0], dict):
                    return records
            elif 'records' in data and isinstance(data['records'], list):
                records = data['records']
                if records and isinstance(records[0], dict):
                    return records
        return []

    def _infer_type_from_string_values(self, values: list) -> str:
        """Infer the Trino SQL type from a list of string values (from CSV).

        Null/empty values are ignored. If ALL values are null/empty,
        returns VARCHAR to avoid assigning a wrong type.

        Inference priority: BOOLEAN > INTEGER > BIGINT > DOUBLE > DATE > TIMESTAMP > VARCHAR
        """
        non_null = [v for v in values if v not in ('', None, 'NULL', 'null', 'None', 'none', '\\N')]
        if not non_null:
            return 'VARCHAR'

        # Check BOOLEAN
        if all(v.lower() in ('true', 'false', '0', '1', 'yes', 'no', 't', 'f', 'y', 'n') for v in non_null):
            return 'BOOLEAN'

        # Check INTEGER / BIGINT
        if self._all_integer(non_null):
            # Distinguish INTEGER vs BIGINT by value range
            max_abs = max(abs(int(v)) for v in non_null)
            if max_abs <= 2_147_483_647:
                return 'INTEGER'
            else:
                return 'BIGINT'

        # Check DOUBLE (numeric with decimals or scientific notation)
        if self._all_float(non_null):
            return 'DOUBLE'

        # Check DATE
        if self._all_date(non_null):
            return 'DATE'

        # Check TIMESTAMP
        if self._all_timestamp(non_null):
            return 'TIMESTAMP'

        return 'VARCHAR'

    def _infer_type_from_python_values(self, values: list) -> str:
        """Infer Trino SQL type from a list of Python-typed values (from JSON).

        None values are ignored. If ALL values are None, returns VARCHAR.
        """
        non_null = [v for v in values if v is not None]
        if not non_null:
            return 'VARCHAR'

        types_seen = set()
        for v in non_null:
            if isinstance(v, bool):
                types_seen.add('bool')
            elif isinstance(v, int):
                types_seen.add('int')
            elif isinstance(v, float):
                types_seen.add('float')
            elif isinstance(v, str):
                types_seen.add('str')
            elif isinstance(v, (list, dict)):
                types_seen.add('complex')
            else:
                types_seen.add('str')

        # If mixed numeric types (int + float), promote to DOUBLE
        if types_seen == {'bool'}:
            return 'BOOLEAN'
        if types_seen == {'int'}:
            max_abs = max(abs(v) for v in non_null if isinstance(v, int))
            if max_abs <= 2_147_483_647:
                return 'INTEGER'
            return 'BIGINT'
        if types_seen <= {'int', 'float'}:
            return 'DOUBLE'
        if types_seen == {'float'}:
            return 'DOUBLE'
        if types_seen == {'str'}:
            # Check if all strings are dates or timestamps
            str_values = [v for v in non_null if isinstance(v, str)]
            if self._all_date(str_values):
                return 'DATE'
            if self._all_timestamp(str_values):
                return 'TIMESTAMP'
            return 'VARCHAR'
        if 'complex' in types_seen:
            return 'VARCHAR'

        # Mixed types → VARCHAR
        return 'VARCHAR'

    def _all_integer(self, values: list) -> bool:
        """Check if all string values represent integers."""
        for v in values:
            try:
                int(v)
            except (ValueError, OverflowError):
                return False
        return True

    def _all_float(self, values: list) -> bool:
        """Check if all string values represent floats (including integers)."""
        for v in values:
            try:
                float(v)
            except (ValueError, OverflowError):
                return False
        return True

    def _all_date(self, values: list) -> bool:
        """Check if all string values match common date patterns."""
        for v in values:
            if not any(p.match(v) for p in self._DATE_PATTERNS):
                return False
        return True

    def _all_timestamp(self, values: list) -> bool:
        """Check if all string values match common timestamp patterns."""
        for v in values:
            if not any(p.match(v) for p in self._TIMESTAMP_PATTERNS):
                return False
        return True

    def _map_pyarrow_type_to_trino(self, pa_type_str: str) -> str:
        """Map a pyarrow type string to a Trino SQL type.

        Handles exact matches first, then prefix-based matching for
        parameterized types like decimal128(18, 2) or timestamp[ns, tz=UTC].
        """
        # Exact match
        if pa_type_str in self._PYARROW_TO_TRINO:
            return self._PYARROW_TO_TRINO[pa_type_str]

        # Prefix matching for parameterized types
        lower = pa_type_str.lower()
        if lower.startswith('decimal') or lower.startswith('fixed_size'):
            return 'DECIMAL'
        if lower.startswith('timestamp'):
            return 'TIMESTAMP'
        if lower.startswith('time'):
            return 'TIME'
        if lower.startswith('date'):
            return 'DATE'
        if lower.startswith('int'):
            return 'INTEGER'
        if lower.startswith('uint'):
            return 'BIGINT'
        if lower.startswith('float') or lower.startswith('double'):
            return 'DOUBLE'
        if lower.startswith('list') or lower.startswith('struct') or lower.startswith('map'):
            return 'VARCHAR'

        return 'VARCHAR'

    def _report_missing_dependency(self, dependency: str, context: str) -> None:
        """Report a missing dependency through the callback if configured."""
        if self._on_missing_dependency:
            self._on_missing_dependency(dependency, context)


# Convenience functions for simple usage
def read_columns_from_file(file_path: str) -> List[str]:
    """Read column names from a data file.

    Convenience function that creates a ColumnDetector and reads columns.

    Args:
        file_path: Path to the data file.

    Returns:
        List of column names.
    """
    detector = ColumnDetector()
    return detector.read_file_columns(file_path)


def infer_column_types_from_file(file_path: str) -> Dict[str, str]:
    """Infer column data types from a data file.

    Convenience function that creates a ColumnDetector and infers types.

    Args:
        file_path: Path to the data file.

    Returns:
        Dict mapping column names to Trino SQL type strings.
    """
    detector = ColumnDetector()
    return detector.infer_column_types(file_path)
