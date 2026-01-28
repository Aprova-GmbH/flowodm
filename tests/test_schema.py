"""Unit tests for schema utilities."""

import json
import tempfile

import pytest

from flowodm import FlowBaseModel
from flowodm.schema import (
    ValidationResult,
    _avro_type_to_python,
    _types_compatible,
    _validate_model_against_schema,
    load_schema_from_file,
    validate_against_file,
)


class SampleModel(FlowBaseModel):
    """Sample model for schema validation tests."""

    class Settings:
        topic = "test-topic"

    user_id: str
    name: str
    age: int | None = None


@pytest.mark.unit
class TestLoadSchema:
    """Tests for schema loading."""

    def test_load_schema_from_file(self, sample_avro_schema):
        """Test loading schema from .avsc file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".avsc", delete=False) as f:
            json.dump(sample_avro_schema, f)
            f.flush()

            schema = load_schema_from_file(f.name)

        assert schema["type"] == "record"
        assert schema["name"] == "UserEvent"

    def test_load_schema_from_file_not_found(self):
        """Test error when schema file not found."""
        with pytest.raises(FileNotFoundError):
            load_schema_from_file("/nonexistent/path.avsc")


@pytest.mark.unit
class TestAvroTypeConversion:
    """Tests for Avro type to Python type conversion."""

    def test_primitive_types(self):
        """Test primitive type conversions."""
        assert _avro_type_to_python("string") is str
        assert _avro_type_to_python("int") is int
        assert _avro_type_to_python("long") is int
        assert _avro_type_to_python("float") is float
        assert _avro_type_to_python("double") is float
        assert _avro_type_to_python("boolean") is bool
        assert _avro_type_to_python("bytes") is bytes
        assert _avro_type_to_python("null") is type(None)

    def test_nullable_types(self):
        """Test nullable union type conversion."""
        result = _avro_type_to_python(["null", "string"])
        # Should return Optional[str] or str | None
        assert result == (str | None) or "Optional" in str(result)

    def test_timestamp_logical_type(self):
        """Test timestamp logical type conversion."""
        from datetime import datetime

        avro_type = {"type": "long", "logicalType": "timestamp-millis"}
        result = _avro_type_to_python(avro_type)

        assert result == datetime


@pytest.mark.unit
class TestValidation:
    """Tests for schema validation."""

    def test_valid_model(self):
        """Test validation of a valid model against schema."""
        schema = {
            "type": "record",
            "name": "SampleModel",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": ["null", "long"]},
                {"name": "message_id", "type": "string"},
            ],
        }

        result = _validate_model_against_schema(SampleModel, schema)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_missing_required_field(self):
        """Test validation detects missing required fields."""
        schema = {
            "type": "record",
            "name": "SampleModel",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},  # Missing in model
                {"name": "message_id", "type": "string"},
            ],
        }

        result = _validate_model_against_schema(SampleModel, schema)

        assert result.is_valid is False
        assert any("email" in error for error in result.errors)

    def test_extra_field_in_model(self):
        """Test validation detects extra fields in model."""
        schema = {
            "type": "record",
            "name": "SampleModel",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "message_id", "type": "string"},
                # Missing 'name' and 'age' that are in model
            ],
        }

        result = _validate_model_against_schema(SampleModel, schema)

        assert result.is_valid is False
        assert any("name" in error for error in result.errors)

    def test_optional_field_missing(self):
        """Test validation allows missing optional fields."""
        schema = {
            "type": "record",
            "name": "SampleModel",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": ["null", "long"]},
                {"name": "message_id", "type": "string"},
                {"name": "optional_extra", "type": ["null", "string"], "default": None},
            ],
        }

        result = _validate_model_against_schema(SampleModel, schema)

        # Should have warning but no error for optional field
        assert result.is_valid is True
        assert len(result.warnings) > 0 or len(result.errors) == 0

    def test_validate_against_file(self, sample_avro_schema):
        """Test validate_against_file function."""

        # Create a model that matches the sample schema
        class MatchingModel(FlowBaseModel):
            class Settings:
                topic = "test"

            user_id: str
            action: str
            timestamp: int  # timestamp-millis stored as int

        with tempfile.NamedTemporaryFile(mode="w", suffix=".avsc", delete=False) as f:
            json.dump(sample_avro_schema, f)
            f.flush()

            result = validate_against_file(MatchingModel, f.name)

        # The result depends on exact field matching
        assert isinstance(result, ValidationResult)


@pytest.mark.unit
class TestTypeCompatibility:
    """Tests for type compatibility checking."""

    def test_exact_match(self):
        """Test exact type match."""
        assert _types_compatible(str, "string") is True
        assert _types_compatible(int, "long") is True
        assert _types_compatible(float, "double") is True

    def test_numeric_compatibility(self):
        """Test numeric types are compatible."""
        assert _types_compatible(int, "int") is True
        assert _types_compatible(int, "long") is True
        assert _types_compatible(float, "float") is True
        assert _types_compatible(float, "double") is True


@pytest.mark.unit
class TestAvroTypeToPythonAdvanced:
    """Advanced tests for Avro type to Python type conversion."""

    def test_union_with_multiple_non_null_types(self):
        """Test union with multiple non-null types returns Any."""
        from typing import Any

        result = _avro_type_to_python(["string", "int"])
        assert result == Any

    def test_union_with_only_null(self):
        """Test union with only null returns None type."""
        result = _avro_type_to_python(["null"])
        assert result is type(None)

    def test_logical_type_date(self):
        """Test date logical type conversion."""
        from datetime import date

        avro_type = {"type": "int", "logicalType": "date"}
        result = _avro_type_to_python(avro_type)
        assert result == date

    def test_logical_type_decimal(self):
        """Test decimal logical type conversion."""
        from decimal import Decimal

        avro_type = {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}
        result = _avro_type_to_python(avro_type)
        assert result == Decimal

    def test_logical_type_uuid(self):
        """Test UUID logical type conversion."""
        avro_type = {"type": "string", "logicalType": "uuid"}
        result = _avro_type_to_python(avro_type)
        assert result is str

    def test_complex_type_without_logical_type(self):
        """Test complex type with base type but no logical type."""
        avro_type = {"type": "string"}
        result = _avro_type_to_python(avro_type)
        assert result is str

    def test_unknown_complex_type(self):
        """Test unknown complex type returns Any."""
        from typing import Any

        avro_type = {"unknown": "value"}
        result = _avro_type_to_python(avro_type)
        assert result == Any


@pytest.mark.unit
class TestGenerateModelFromSchema:
    """Tests for model generation from schema."""

    def test_generate_from_schema_dict(self):
        """Test generating model from schema dictionary."""
        from flowodm.schema import generate_model_from_schema

        schema = {
            "type": "record",
            "name": "TestEvent",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "count", "type": "int"},
            ],
        }

        Model = generate_model_from_schema(schema, topic="test-topic")

        assert Model.__name__ == "TestEvent"
        assert Model._get_topic() == "test-topic"
        # Create instance to verify fields
        instance = Model(event_id="123", count=42, message_id="test-id")
        assert instance.event_id == "123"
        assert instance.count == 42

    def test_generate_from_schema_with_optional_field(self):
        """Test generating model with optional field (has default)."""
        from flowodm.schema import generate_model_from_schema

        schema = {
            "type": "record",
            "name": "TestEvent",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "optional_field", "type": ["null", "string"], "default": None},
            ],
        }

        Model = generate_model_from_schema(schema, topic="test-topic")
        instance = Model(event_id="123", message_id="test-id")
        assert instance.optional_field is None

    def test_generate_from_schema_file(self, tmp_path):
        """Test generating model from schema file."""
        from flowodm.schema import generate_model_from_schema

        schema_file = tmp_path / "test.avsc"
        schema = {
            "type": "record",
            "name": "FileEvent",
            "fields": [
                {"name": "name", "type": "string"},
            ],
        }

        with open(schema_file, "w") as f:
            json.dump(schema, f)

        Model = generate_model_from_schema(str(schema_file), topic="test-topic")
        assert Model.__name__ == "FileEvent"

    def test_generate_with_custom_class_name(self):
        """Test generating model with custom class name."""
        from flowodm.schema import generate_model_from_schema

        schema = {
            "type": "record",
            "name": "OriginalName",
            "fields": [{"name": "field1", "type": "string"}],
        }

        Model = generate_model_from_schema(
            schema,
            topic="test-topic",
            class_name="CustomName",
        )
        assert Model.__name__ == "CustomName"

    def test_generate_with_consumer_group(self):
        """Test generating model with consumer group."""
        from flowodm.schema import generate_model_from_schema

        schema = {
            "type": "record",
            "name": "TestEvent",
            "fields": [{"name": "field1", "type": "string"}],
        }

        Model = generate_model_from_schema(
            schema,
            topic="test-topic",
            consumer_group="test-group",
        )
        assert Model._get_consumer_group() == "test-group"


@pytest.mark.unit
class TestValidationResult:
    """Tests for ValidationResult class."""

    def test_validation_result_str_valid(self):
        """Test ValidationResult string representation when valid."""
        result = ValidationResult(is_valid=True, errors=[], warnings=[])
        result_str = str(result)
        assert "valid" in result_str.lower()

    def test_validation_result_str_invalid(self):
        """Test ValidationResult string representation when invalid."""
        result = ValidationResult(
            is_valid=False,
            errors=["Error 1", "Error 2"],
            warnings=["Warning 1"],
        )
        result_str = str(result)
        assert "Error 1" in result_str
        assert "Error 2" in result_str


@pytest.mark.unit
class TestIsNullableAvroType:
    """Tests for _is_nullable_avro_type helper."""

    def test_nullable_union_type(self):
        """Test detecting nullable union type."""
        from flowodm.schema import _is_nullable_avro_type

        assert _is_nullable_avro_type(["null", "string"]) is True

    def test_non_nullable_union_type(self):
        """Test detecting non-nullable union type."""
        from flowodm.schema import _is_nullable_avro_type

        assert _is_nullable_avro_type(["string", "int"]) is False

    def test_simple_type_not_nullable(self):
        """Test simple type is not nullable."""
        from flowodm.schema import _is_nullable_avro_type

        assert _is_nullable_avro_type("string") is False

    def test_null_type_is_nullable(self):
        """Test null type is nullable."""
        from flowodm.schema import _is_nullable_avro_type

        assert _is_nullable_avro_type("null") is True


@pytest.mark.unit
class TestValidationErrors:
    """Tests for validation error cases."""

    def test_validation_type_mismatch_in_common_fields(self):
        """Test validation detects type mismatches in common fields."""

        class WrongTypeModel(FlowBaseModel):
            class Settings:
                topic = "test"

            user_id: int  # Should be string

        schema = {
            "type": "record",
            "name": "WrongTypeModel",
            "fields": [
                {"name": "user_id", "type": "string"},  # Expects string
                {"name": "message_id", "type": "string"},
            ],
        }

        result = _validate_model_against_schema(WrongTypeModel, schema)

        # Should have error for type mismatch
        assert result.is_valid is False
        assert any("user_id" in error and "type mismatch" in error for error in result.errors)
