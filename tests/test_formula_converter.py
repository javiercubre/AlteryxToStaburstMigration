"""
Tests for the Alteryx formula to Trino SQL converter.

Tests that Alteryx formulas are correctly converted to their
Trino SQL equivalents.

LOW-06 fix: Migrated to pytest framework.
"""
import os
import sys

import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from formula_converter import (
    FormulaConverter,
    convert_alteryx_expression,
    convert_aggregation
)


@pytest.fixture
def converter():
    """Create a FormulaConverter instance for testing."""
    return FormulaConverter()


class TestFieldReferences:
    """Tests for field reference conversion."""

    def test_basic_field_reference(self, converter):
        """Test that Alteryx field references [FieldName] are converted to SQL."""
        result = converter.convert('[CustomerName]')
        assert '"CustomerName"' in result

    def test_multiple_field_references(self, converter):
        """Test multiple field references in one expression."""
        result = converter.convert('[Field1] + [Field2]')
        assert '"Field1"' in result and '"Field2"' in result


class TestOperatorConversion:
    """Tests for operator conversion."""

    def test_equality_operator(self, converter):
        """Test that == is converted to =."""
        result = converter.convert('[Status] == "Active"')
        assert '=' in result and '==' not in result

    def test_and_operator(self, converter):
        """Test && to AND conversion."""
        result = converter.convert('[A] && [B]')
        assert ' AND ' in result

    def test_or_operator(self, converter):
        """Test || to OR conversion."""
        result = converter.convert('[A] || [B]')
        assert ' OR ' in result


class TestIIFConversion:
    """Tests for IIF to CASE WHEN conversion."""

    def test_simple_iif(self, converter):
        """Test simple IIF conversion."""
        result = converter.convert('IIF([Status]="Active", 1, 0)')
        assert 'CASE WHEN' in result
        assert 'THEN' in result
        assert 'ELSE' in result
        assert 'END' in result

    def test_nested_iif(self, converter):
        """Test nested IIF conversion."""
        result = converter.convert('IIF([A]>10, IIF([B]>5, "High", "Med"), "Low")')
        assert result.count('CASE WHEN') == 2


class TestNullHandling:
    """Tests for null handling functions."""

    def test_isnull_conversion(self, converter):
        """Test IsNull function conversion."""
        result = converter.convert('IsNull([Field])')
        assert 'IS NULL' in result

    def test_isempty_conversion(self, converter):
        """Test IsEmpty function conversion."""
        result = converter.convert('IsEmpty([Field])')
        assert "= ''" in result


class TestStringFunctions:
    """Tests for string function conversions."""

    def test_trim(self, converter):
        """Test Trim function conversion."""
        result = converter.convert('Trim([Name])')
        assert 'TRIM' in result

    def test_uppercase(self, converter):
        """Test UpperCase function conversion."""
        result = converter.convert('UpperCase([Name])')
        assert 'UPPER' in result

    def test_lowercase(self, converter):
        """Test LowerCase function conversion."""
        result = converter.convert('LowerCase([Name])')
        assert 'LOWER' in result

    def test_length(self, converter):
        """Test Length function conversion."""
        result = converter.convert('Length([Name])')
        assert 'LENGTH' in result

    def test_replace(self, converter):
        """Test Replace function conversion."""
        result = converter.convert('Replace([Text], "old", "new")')
        assert 'REPLACE' in result

    def test_contains(self, converter):
        """Test Contains function conversion."""
        result = converter.convert('Contains([Text], "search")')
        assert 'STRPOS' in result and '> 0' in result


class TestMathFunctions:
    """Tests for math function conversions."""

    def test_abs(self, converter):
        """Test Abs function conversion."""
        result = converter.convert('Abs([Value])')
        assert 'ABS' in result

    def test_ceil(self, converter):
        """Test Ceil function conversion."""
        result = converter.convert('Ceil([Value])')
        assert 'CEIL' in result

    def test_floor(self, converter):
        """Test Floor function conversion."""
        result = converter.convert('Floor([Value])')
        assert 'FLOOR' in result

    def test_round(self, converter):
        """Test Round function conversion."""
        result = converter.convert('Round([Value], 2)')
        assert 'ROUND' in result

    def test_sqrt(self, converter):
        """Test Sqrt function conversion."""
        result = converter.convert('Sqrt([Value])')
        assert 'SQRT' in result


class TestDateFunctions:
    """Tests for date/time function conversions."""

    def test_datetimenow(self, converter):
        """Test DateTimeNow function conversion."""
        result = converter.convert('DateTimeNow()')
        assert 'CURRENT_TIMESTAMP' in result

    def test_datetimeyear(self, converter):
        """Test DateTimeYear function conversion."""
        result = converter.convert('DateTimeYear([Date])')
        assert 'YEAR' in result

    def test_datetimemonth(self, converter):
        """Test DateTimeMonth function conversion."""
        result = converter.convert('DateTimeMonth([Date])')
        assert 'MONTH' in result

    def test_datetimeday(self, converter):
        """Test DateTimeDay function conversion."""
        result = converter.convert('DateTimeDay([Date])')
        assert 'DAY' in result


class TestConversionFunctions:
    """Tests for type conversion function conversions."""

    def test_tonumber(self, converter):
        """Test ToNumber function conversion."""
        result = converter.convert('ToNumber([StringField])')
        assert 'CAST' in result and 'DOUBLE' in result

    def test_tostring(self, converter):
        """Test ToString function conversion."""
        result = converter.convert('ToString([NumField])')
        assert 'CAST' in result and 'VARCHAR' in result

    def test_tointeger(self, converter):
        """Test ToInteger function conversion."""
        result = converter.convert('ToInteger([StringField])')
        assert 'CAST' in result and 'BIGINT' in result


class TestMinMaxFunctions:
    """Tests for Min/Max function conversions."""

    def test_min(self, converter):
        """Test Min function conversion."""
        result = converter.convert('Min([A], [B], [C])')
        assert 'LEAST' in result

    def test_max(self, converter):
        """Test Max function conversion."""
        result = converter.convert('Max([A], [B], [C])')
        assert 'GREATEST' in result


class TestCoalesce:
    """Tests for Coalesce function conversion."""

    def test_coalesce(self, converter):
        """Test Coalesce function conversion."""
        result = converter.convert('Coalesce([A], [B], "default")')
        assert 'COALESCE' in result


class TestStringLiteralProtection:
    """Tests that string literals are not corrupted by operator replacement."""

    def test_exclamation_in_string_literal(self, converter):
        """Test that ! inside string literals is not replaced with NOT."""
        result = converter.convert("[Field] = 'Hello!'")
        assert "'Hello!'" in result
        assert "NOT" not in result

    def test_pipe_in_string_literal(self, converter):
        """Test that || inside string literals is not replaced with OR."""
        result = converter.convert("[Field] = 'a||b'")
        assert "'a||b'" in result
        assert " OR " not in result

    def test_not_equals_in_string_literal(self, converter):
        """Test that != inside string literals is not replaced with <>."""
        result = converter.convert("[Field] = 'x!=y'")
        assert "'x!=y'" in result

    def test_operators_outside_strings_still_converted(self, converter):
        """Test that operators outside string literals are still converted."""
        result = converter.convert("[A] != 'test' && [B] = 'ok'")
        assert '<>' in result
        assert ' AND ' in result
        assert "'test'" in result
        assert "'ok'" in result

    def test_not_operator_before_function(self, converter):
        """Test that ! before a function call is converted to NOT."""
        result = converter.convert('!IsNull([Field])')
        assert 'NOT' in result
        assert 'IS NULL' in result


class TestComplexExpressions:
    """Tests for complex nested expressions."""

    def test_complex_nested_expression(self, converter):
        """Test conversion of complex nested expressions."""
        expr = 'IIF(IsNull([Amount]), 0, Round([Amount] * 1.1, 2))'
        result = converter.convert(expr)

        assert 'CASE WHEN' in result
        assert 'IS NULL' in result
        assert 'ROUND' in result


class TestAggregationConversion:
    """Tests for aggregation function conversions."""

    def test_sum(self):
        """Test Sum aggregation conversion."""
        result = convert_aggregation('Sum', '"Amount"')
        assert result == 'SUM("Amount")'

    def test_count(self):
        """Test Count aggregation conversion."""
        result = convert_aggregation('Count', '"ID"')
        assert result == 'COUNT("ID")'

    def test_count_distinct(self):
        """Test CountDistinct aggregation conversion."""
        result = convert_aggregation('CountDistinct', '"CustomerID"')
        assert result == 'COUNT(DISTINCT "CustomerID")'

    def test_avg(self):
        """Test Avg aggregation conversion."""
        result = convert_aggregation('Avg', '"Price"')
        assert result == 'AVG("Price")'


class TestConvenienceFunction:
    """Tests for the convenience convert_alteryx_expression function."""

    def test_convenience_function(self):
        """Test the convenience convert_alteryx_expression function."""
        result = convert_alteryx_expression('[Field1] + [Field2]')
        assert '"Field1"' in result and '"Field2"' in result


# Backwards compatibility: allow running directly
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
