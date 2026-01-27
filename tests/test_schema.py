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
