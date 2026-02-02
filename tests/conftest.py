"""
Pytest configuration and shared fixtures.

LOW-06 fix: Added pytest configuration.
"""
import os
import sys
import tempfile

import pytest

# Add parent directory to path so imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def test_data_dir():
    """Return the path to the test data directory."""
    return os.path.join(os.path.dirname(__file__), 'test_data')


@pytest.fixture(scope="session")
def samples_dir(project_root):
    """Return the path to the samples directory."""
    return os.path.join(project_root, 'samples')


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test output."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup is optional - pytest handles this with tmpdir fixture


@pytest.fixture
def dbt_generator_factory():
    """Factory for creating DBTGenerator instances."""
    from dbt_generator import DBTGenerator

    def _create(output_dir=None, interactive=False, **kwargs):
        if output_dir is None:
            output_dir = tempfile.mkdtemp()
        return DBTGenerator(output_dir, interactive=interactive, **kwargs)

    return _create


@pytest.fixture
def formula_converter():
    """Create a FormulaConverter instance."""
    from formula_converter import FormulaConverter
    return FormulaConverter()
