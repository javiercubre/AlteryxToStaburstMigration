"""
Column detection utilities for data files.

MED-01 fix: Extracted from dbt_generator.py for better modularity.

Supports reading column headers from:
- CSV files (with automatic delimiter detection)
- Excel files (.xlsx, .xls)
- JSON files (arrays of objects or nested data structures)
- Parquet files (requires pyarrow)

Usage:
    from column_detector import ColumnDetector

    detector = ColumnDetector()
    columns = detector.read_file_columns('/path/to/data.csv')
"""
import csv
import json
from pathlib import Path
from typing import List, Optional, Callable

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

    def _report_missing_dependency(self, dependency: str, context: str) -> None:
        """Report a missing dependency through the callback if configured."""
        if self._on_missing_dependency:
            self._on_missing_dependency(dependency, context)


# Convenience function for simple usage
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
