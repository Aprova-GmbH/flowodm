"""Unit tests for custom exceptions."""

import pytest

from flowodm.exceptions import (
    ConfigurationError,
    ConnectionError,
    ConsumerError,
    DeserializationError,
    FlowODMError,
    ProducerError,
    SchemaCompatibilityError,
    SchemaError,
    SchemaRegistryError,
    SchemaValidationError,
    SerializationError,
    SettingsError,
    TopicError,
)


@pytest.mark.unit
class TestExceptions:
    """Tests for custom exception classes."""

    def test_base_exception(self):
        """Test FlowODMError base exception."""
        exc = FlowODMError("test error")
        assert str(exc) == "test error"
        assert isinstance(exc, Exception)

    def test_connection_error(self):
        """Test ConnectionError exception."""
        exc = ConnectionError("connection failed")
        assert str(exc) == "connection failed"
        assert isinstance(exc, FlowODMError)

    def test_configuration_error(self):
        """Test ConfigurationError exception."""
        exc = ConfigurationError("invalid config")
        assert str(exc) == "invalid config"
        assert isinstance(exc, FlowODMError)

    def test_schema_error(self):
        """Test SchemaError base exception."""
        exc = SchemaError("schema error")
        assert str(exc) == "schema error"
        assert isinstance(exc, FlowODMError)

    def test_schema_validation_error_with_errors(self):
        """Test SchemaValidationError with errors list."""
        errors = ["Field 'name' is missing", "Field 'age' has wrong type"]
        exc = SchemaValidationError("Validation failed", errors=errors)

        assert str(exc) == "Validation failed"
        assert exc.errors == errors
        assert len(exc.errors) == 2
        assert isinstance(exc, SchemaError)

    def test_schema_validation_error_without_errors(self):
        """Test SchemaValidationError without errors list."""
        exc = SchemaValidationError("Validation failed")

        assert str(exc) == "Validation failed"
        assert exc.errors == []
        assert isinstance(exc, SchemaError)

    def test_schema_registry_error(self):
        """Test SchemaRegistryError exception."""
        exc = SchemaRegistryError("registry unavailable")
        assert str(exc) == "registry unavailable"
        assert isinstance(exc, SchemaError)

    def test_schema_compatibility_error_with_level(self):
        """Test SchemaCompatibilityError with compatibility level."""
        exc = SchemaCompatibilityError("Schema not compatible", compatibility_level="BACKWARD")

        assert str(exc) == "Schema not compatible"
        assert exc.compatibility_level == "BACKWARD"
        assert isinstance(exc, SchemaError)

    def test_schema_compatibility_error_without_level(self):
        """Test SchemaCompatibilityError without compatibility level."""
        exc = SchemaCompatibilityError("Schema not compatible")

        assert str(exc) == "Schema not compatible"
        assert exc.compatibility_level is None
        assert isinstance(exc, SchemaError)

    def test_serialization_error(self):
        """Test SerializationError exception."""
        exc = SerializationError("serialization failed")
        assert str(exc) == "serialization failed"
        assert isinstance(exc, FlowODMError)

    def test_deserialization_error(self):
        """Test DeserializationError exception."""
        exc = DeserializationError("deserialization failed")
        assert str(exc) == "deserialization failed"
        assert isinstance(exc, FlowODMError)

    def test_producer_error(self):
        """Test ProducerError exception."""
        exc = ProducerError("produce failed")
        assert str(exc) == "produce failed"
        assert isinstance(exc, FlowODMError)

    def test_consumer_error(self):
        """Test ConsumerError exception."""
        exc = ConsumerError("consume failed")
        assert str(exc) == "consume failed"
        assert isinstance(exc, FlowODMError)

    def test_topic_error(self):
        """Test TopicError exception."""
        exc = TopicError("topic error")
        assert str(exc) == "topic error"
        assert isinstance(exc, FlowODMError)

    def test_settings_error(self):
        """Test SettingsError exception."""
        exc = SettingsError("settings invalid")
        assert str(exc) == "settings invalid"
        assert isinstance(exc, FlowODMError)
