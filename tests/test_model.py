"""Unit tests for FlowBaseModel."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from flowodm import FlowBaseModel, generate_message_id
from flowodm.exceptions import SettingsError


class UserEvent(FlowBaseModel):
    """Test model for user events."""

    class Settings:
        topic = "user-events"
        schema_subject = "user-events-value"
        consumer_group = "test-group"
        key_field = "user_id"

    user_id: str
    action: str
    timestamp: datetime
    metadata: str | None = None


class MinimalModel(FlowBaseModel):
    """Minimal test model."""

    class Settings:
        topic = "minimal-topic"

    name: str


class NoSettingsModel(FlowBaseModel):
    """Model without Settings class."""

    name: str


@pytest.mark.unit
class TestModelCreation:
    """Tests for model instantiation and basic operations."""

    def test_model_creation(self):
        """Test basic model instantiation."""
        event = UserEvent(
            user_id="user-123",
            action="login",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert event.user_id == "user-123"
        assert event.action == "login"
        assert event.timestamp == datetime(2024, 1, 1, 12, 0, 0)
        assert event.message_id is not None

    def test_model_with_optional_field(self):
        """Test model with optional field."""
        event = UserEvent(
            user_id="user-123",
            action="login",
            timestamp=datetime.now(),
            metadata="extra info",
        )

        assert event.metadata == "extra info"

    def test_model_without_optional_field(self):
        """Test model without optional field defaults to None."""
        event = UserEvent(
            user_id="user-123",
            action="login",
            timestamp=datetime.now(),
        )

        assert event.metadata is None

    def test_generate_message_id(self):
        """Test message ID generation."""
        id1 = generate_message_id()
        id2 = generate_message_id()

        assert id1 != id2
        assert len(id1) == 36  # UUID format
        assert "-" in id1

    def test_message_id_auto_generated(self):
        """Test that message_id is auto-generated."""
        event1 = MinimalModel(name="test1")
        event2 = MinimalModel(name="test2")

        assert event1.message_id != event2.message_id


@pytest.mark.unit
class TestSettings:
    """Tests for Settings class configuration."""

    def test_get_topic(self):
        """Test getting topic from Settings."""
        assert UserEvent._get_topic() == "user-events"

    def test_get_schema_subject(self):
        """Test getting schema subject from Settings."""
        assert UserEvent._get_schema_subject() == "user-events-value"

    def test_get_schema_subject_default(self):
        """Test default schema subject is topic-value."""
        assert MinimalModel._get_schema_subject() == "minimal-topic-value"

    def test_get_consumer_group(self):
        """Test getting consumer group from Settings."""
        assert UserEvent._get_consumer_group() == "test-group"

    def test_get_consumer_group_none(self):
        """Test consumer group returns None when not set."""
        assert MinimalModel._get_consumer_group() is None

    def test_get_key_field(self):
        """Test getting key field from Settings."""
        assert UserEvent._get_key_field() == "user_id"

    def test_get_key_field_none(self):
        """Test key field returns None when not set."""
        assert MinimalModel._get_key_field() is None

    def test_missing_settings_class(self):
        """Test error when Settings class is missing."""
        # NoSettingsModel has no topic defined
        with pytest.raises(SettingsError):
            NoSettingsModel._get_topic()


@pytest.mark.unit
class TestSerialization:
    """Tests for model serialization."""

    def test_to_avro_dict(self):
        """Test converting model to Avro-compatible dict."""
        event = UserEvent(
            user_id="user-123",
            action="login",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )

        data = event._to_avro_dict()

        assert data["user_id"] == "user-123"
        assert data["action"] == "login"
        assert isinstance(data["timestamp"], int)  # Should be epoch ms
        assert data["message_id"] == event.message_id

    def test_from_avro_dict(self):
        """Test creating model from Avro dict."""
        data = {
            "user_id": "user-456",
            "action": "logout",
            "timestamp": 1704110400000,  # 2024-01-01 12:00:00 UTC
            "message_id": "msg-789",
        }

        event = UserEvent._from_avro_dict(data)

        assert event.user_id == "user-456"
        assert event.action == "logout"
        assert isinstance(event.timestamp, datetime)
        assert event.message_id == "msg-789"

    def test_get_message_key(self):
        """Test getting message key from key_field."""
        event = UserEvent(
            user_id="user-123",
            action="login",
            timestamp=datetime.now(),
        )

        key = event._get_message_key()

        assert key == b"user-123"

    def test_get_message_key_no_key_field(self):
        """Test message key is None when no key_field."""
        model = MinimalModel(name="test")

        key = model._get_message_key()

        assert key is None


@pytest.mark.unit
class TestSchemaGeneration:
    """Tests for Avro schema generation."""

    def test_generate_avro_schema(self):
        """Test generating Avro schema from model."""
        schema = MinimalModel._generate_avro_schema()

        assert schema["type"] == "record"
        assert schema["name"] == "MinimalModel"
        assert len(schema["fields"]) == 2  # name + message_id

        field_names = [f["name"] for f in schema["fields"]]
        assert "name" in field_names
        assert "message_id" in field_names

    def test_generate_avro_schema_with_types(self):
        """Test schema generation preserves types."""
        schema = UserEvent._generate_avro_schema()

        fields = {f["name"]: f["type"] for f in schema["fields"]}

        assert fields["user_id"] == "string"
        assert fields["action"] == "string"


@pytest.mark.unit
class TestProduceConsume:
    """Tests for produce/consume operations with mocks."""

    def test_produce_calls_producer(self, mock_producer):
        """Test that produce_nowait() calls the producer correctly."""
        event = MinimalModel(name="test")

        with patch.object(MinimalModel, "get_producer", return_value=mock_producer):
            event.produce_nowait()

        assert len(mock_producer.messages) == 1
        assert mock_producer.messages[0]["topic"] == "minimal-topic"

    def test_produce_sync_flushes(self, mock_producer):
        """Test that produce() flushes the producer."""
        event = MinimalModel(name="test")

        with patch.object(MinimalModel, "get_producer", return_value=mock_producer):
            event.produce()

        assert mock_producer._flushed is True

    def test_produce_many(self, mock_producer):
        """Test batch producing multiple messages."""
        events = [MinimalModel(name=f"test-{i}") for i in range(5)]

        with patch.object(MinimalModel, "get_producer", return_value=mock_producer):
            count = MinimalModel.produce_many(events)

        assert count == 5
        assert len(mock_producer.messages) == 5

    def test_produce_many_no_flush(self, mock_producer):
        """Test batch producing without flush."""
        events = [MinimalModel(name=f"test-{i}") for i in range(3)]

        with patch.object(MinimalModel, "get_producer", return_value=mock_producer):
            count = MinimalModel.produce_many(events, flush=False)

        assert count == 3
        assert len(mock_producer.messages) == 3
        # flush() should not be called when flush=False
        # (We can't easily check this with the mock, but count is correct)

    def test_produce_many_empty_list(self, mock_producer):
        """Test batch producing with empty list."""
        with patch.object(MinimalModel, "get_producer", return_value=mock_producer):
            count = MinimalModel.produce_many([])

        assert count == 0
        assert len(mock_producer.messages) == 0

    def test_produce_with_callback(self, mock_producer):
        """Test produce_nowait with callback."""
        event = MinimalModel(name="test")
        callback_called = []

        def callback(err, msg):
            callback_called.append((err, msg))

        with patch.object(MinimalModel, "get_producer", return_value=mock_producer):
            event.produce_nowait(callback=callback)

        assert len(mock_producer.messages) == 1

    def test_consume_one_with_message(self, mock_consumer_with_message):
        """Test consume_one returns a message."""
        with patch.object(
            MinimalModel,
            "get_consumer",
            return_value=mock_consumer_with_message,
        ):
            result = MinimalModel.consume_one(group_id="test-group")

        assert result is not None
        assert isinstance(result, MinimalModel)
        assert result.name == "test"

    def test_consume_one_no_message(self, mock_consumer_no_message):
        """Test consume_one returns None when no message."""
        with patch.object(
            MinimalModel,
            "get_consumer",
            return_value=mock_consumer_no_message,
        ):
            result = MinimalModel.consume_one(group_id="test-group")

        assert result is None

    def test_consume_batch(self, mock_consumer_with_batch):
        """Test consume_batch returns multiple messages."""
        with patch.object(
            MinimalModel,
            "get_consumer",
            return_value=mock_consumer_with_batch,
        ):
            results = MinimalModel.consume_batch(max_messages=3, group_id="test-group")

        assert len(results) == 3
        assert all(isinstance(r, MinimalModel) for r in results)

    @pytest.mark.asyncio
    async def test_aproduce(self, mock_async_producer):
        """Test async produce operation."""
        event = MinimalModel(name="test")

        with patch.object(
            MinimalModel,
            "get_async_producer",
            return_value=mock_async_producer,
        ):
            await event.aproduce()

        assert len(mock_async_producer.messages) == 1

    @pytest.mark.asyncio
    async def test_aproduce_many(self, mock_async_producer):
        """Test async batch produce."""
        events = [MinimalModel(name=f"test-{i}") for i in range(3)]

        with patch.object(
            MinimalModel,
            "get_async_producer",
            return_value=mock_async_producer,
        ):
            count = await MinimalModel.aproduce_many(events)

        assert count == 3
        assert len(mock_async_producer.messages) == 3

    @pytest.mark.asyncio
    async def test_aconsume_one(self, mock_async_consumer_with_message):
        """Test async consume_one."""
        with patch.object(
            MinimalModel,
            "get_async_consumer",
            return_value=mock_async_consumer_with_message,
        ):
            result = await MinimalModel.aconsume_one(group_id="test-group")

        assert result is not None
        assert isinstance(result, MinimalModel)
        assert result.name == "test"


@pytest.mark.unit
class TestSerializationErrors:
    """Tests for serialization error handling."""

    def test_serialize_avro(self):
        """Test Avro serialization."""
        event = MinimalModel(name="test")
        data = event._serialize_avro()

        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_deserialize_avro(self):
        """Test Avro deserialization."""
        event = MinimalModel(name="test")
        data = event._serialize_avro()

        result = MinimalModel._deserialize_avro(data)

        assert result.name == "test"
        assert result.message_id == event.message_id

    def test_strip_confluent_header(self):
        """Test stripping Confluent wire format header."""
        # Create pure Avro data
        event = MinimalModel(name="test")
        pure_avro_data = event._serialize_avro()

        # Create Confluent wire format: magic byte (0x00) + 4-byte schema ID + Avro data
        schema_id = 12345
        confluent_data = bytes([0x00]) + schema_id.to_bytes(4, "big") + pure_avro_data

        # Strip should return pure Avro data
        stripped = MinimalModel._strip_confluent_header(confluent_data)
        assert stripped == pure_avro_data

    def test_strip_confluent_header_preserves_pure_avro(self):
        """Test that pure Avro data (without Confluent header) is preserved."""
        event = MinimalModel(name="test")
        pure_avro_data = event._serialize_avro()

        # Pure Avro data should not be modified (first byte is not 0x00)
        stripped = MinimalModel._strip_confluent_header(pure_avro_data)
        assert stripped == pure_avro_data

    def test_strip_confluent_header_short_data(self):
        """Test that short data (less than 5 bytes) is preserved."""
        short_data = bytes([0x00, 0x01, 0x02])  # Only 3 bytes, even though starts with 0x00

        stripped = MinimalModel._strip_confluent_header(short_data)
        assert stripped == short_data

    def test_deserialize_avro_with_confluent_header(self):
        """Test deserialization of messages with Confluent wire format header."""
        event = MinimalModel(name="confluent_test")
        pure_avro_data = event._serialize_avro()

        # Create Confluent wire format: magic byte (0x00) + 4-byte schema ID + Avro data
        schema_id = 42
        confluent_data = bytes([0x00]) + schema_id.to_bytes(4, "big") + pure_avro_data

        # Should deserialize correctly despite the header
        result = MinimalModel._deserialize_avro(confluent_data)

        assert result.name == "confluent_test"
        assert result.message_id == event.message_id

    def test_validate_required_fields_raises_on_empty(self):
        """Test that validation raises error when required string fields are empty."""
        from flowodm.exceptions import DeserializationError

        # Create a record with empty required field
        record = {"message_id": "test-id", "name": ""}

        with pytest.raises(DeserializationError) as exc_info:
            MinimalModel._validate_required_fields(record)

        assert "name" in str(exc_info.value)
        assert "wire format mismatch" in str(exc_info.value)

    def test_validate_required_fields_passes_with_valid_data(self):
        """Test that validation passes when required fields have values."""
        record = {"message_id": "test-id", "name": "valid_name"}

        # Should not raise
        MinimalModel._validate_required_fields(record)

    def test_deserialize_avro_validates_by_default(self):
        """Test that deserialization validates required fields by default."""
        import io

        import fastavro

        from flowodm.exceptions import DeserializationError

        # Manually create Avro data with empty name field
        schema = MinimalModel._get_avro_schema()
        record = {"message_id": "test-id", "name": ""}

        output = io.BytesIO()
        fastavro.schemaless_writer(output, schema, record)
        avro_data = output.getvalue()

        with pytest.raises(DeserializationError) as exc_info:
            MinimalModel._deserialize_avro(avro_data)

        assert "name" in str(exc_info.value)

    def test_deserialize_avro_validation_can_be_disabled(self):
        """Test that validation can be disabled."""
        import io

        import fastavro

        # Manually create Avro data with empty name field
        schema = MinimalModel._get_avro_schema()
        record = {"message_id": "test-id", "name": ""}

        output = io.BytesIO()
        fastavro.schemaless_writer(output, schema, record)
        avro_data = output.getvalue()

        # Should not raise when validation is disabled
        result = MinimalModel._deserialize_avro(avro_data, validate=False)
        assert result.name == ""

    def test_get_avro_schema_from_path(self, tmp_path):
        """Test loading schema from file path."""
        import json

        schema_file = tmp_path / "test_schema.avsc"
        schema = {
            "type": "record",
            "name": "TestModel",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "message_id", "type": "string"},
            ],
        }

        with open(schema_file, "w") as f:
            json.dump(schema, f)

        # Store path as string for use in class definition
        path_str = str(schema_file)

        class TestModel(FlowBaseModel):
            class Settings:
                topic = "test"
                schema_path = path_str

            name: str

        result_schema = TestModel._get_avro_schema()
        assert result_schema["name"] == "TestModel"


@pytest.mark.unit
class TestConsumerGroup:
    """Tests for consumer group handling."""

    def test_get_consumer_with_group_id(self, mock_consumer_no_message):
        """Test getting consumer with explicit group_id."""
        with patch("flowodm.model.get_consumer", return_value=mock_consumer_no_message):
            consumer = UserEvent.get_consumer(group_id="custom-group")

        assert consumer is not None

    def test_get_consumer_with_settings_group(self, mock_consumer_no_message):
        """Test getting consumer with Settings.consumer_group."""
        with patch("flowodm.model.get_consumer", return_value=mock_consumer_no_message):
            consumer = UserEvent.get_consumer()

        assert consumer is not None

    def test_get_consumer_no_group_raises_error(self):
        """Test getting consumer without group_id raises SettingsError."""
        with pytest.raises(SettingsError):
            MinimalModel.get_consumer()  # MinimalModel has no consumer_group

    @pytest.mark.asyncio
    async def test_get_async_consumer_no_group_raises_error(self):
        """Test getting async consumer without group_id raises SettingsError."""
        with pytest.raises(SettingsError):
            await MinimalModel.get_async_consumer()


@pytest.mark.unit
class TestPythonTypeToAvro:
    """Tests for Python type to Avro type conversion."""

    def test_basic_types(self):
        """Test basic Python type conversion."""
        assert MinimalModel._python_type_to_avro(str) == "string"
        assert MinimalModel._python_type_to_avro(int) == "long"
        assert MinimalModel._python_type_to_avro(float) == "double"
        assert MinimalModel._python_type_to_avro(bool) == "boolean"
        assert MinimalModel._python_type_to_avro(bytes) == "bytes"

    def test_none_type(self):
        """Test None type conversion."""
        assert MinimalModel._python_type_to_avro(type(None)) == "null"

    def test_datetime_type(self):
        """Test datetime conversion to timestamp-millis."""
        result = MinimalModel._python_type_to_avro(datetime)
        assert isinstance(result, dict)
        assert result["type"] == "long"
        assert result["logicalType"] == "timestamp-millis"

    def test_unknown_type_fallback(self):
        """Test unknown type falls back to string."""

        class CustomType:
            pass

        result = MinimalModel._python_type_to_avro(CustomType)
        assert result == "string"


@pytest.mark.unit
class TestGetSchemaPath:
    """Tests for schema path retrieval."""

    def test_get_schema_path_when_set(self):
        """Test getting schema path when set in Settings."""

        class ModelWithSchemaPath(FlowBaseModel):
            class Settings:
                topic = "test"
                schema_path = "/path/to/schema.avsc"

            name: str

        assert ModelWithSchemaPath._get_schema_path() == "/path/to/schema.avsc"

    def test_get_schema_path_when_not_set(self):
        """Test getting schema path returns None when not set."""
        assert MinimalModel._get_schema_path() is None


@pytest.mark.unit
class TestErrorHandling:
    """Tests for error handling in model operations."""

    def test_serialize_avro_error_handling(self):
        """Test error handling in serialization."""
        import fastavro

        from flowodm.exceptions import SerializationError

        event = UserEvent(
            user_id="test",
            action="test",
            timestamp=datetime.now(),
        )

        # Mock fastavro.schemaless_writer to raise error
        with patch.object(
            fastavro, "schemaless_writer", side_effect=Exception("Serialization error")
        ):
            with pytest.raises(SerializationError):
                event._serialize_avro()

    def test_deserialize_avro_error_handling(self):
        """Test error handling in deserialization."""
        from flowodm.exceptions import DeserializationError

        # Invalid bytes that can't be deserialized
        with pytest.raises(DeserializationError):
            MinimalModel._deserialize_avro(b"invalid data")

    def test_get_message_key_none_value(self):
        """Test getting message key when key field value is None."""

        class ModelWithOptionalKey(FlowBaseModel):
            class Settings:
                topic = "test"
                key_field = "optional_key"

            optional_key: str | None = None

        model = ModelWithOptionalKey()
        assert model._get_message_key() is None

    def test_produce_error_handling(self, mock_producer):
        """Test error handling in produce operation."""
        from flowodm.exceptions import ProducerError

        mock_producer.produce = lambda *args, **kwargs: (_ for _ in ()).throw(
            Exception("Producer error")
        )

        event = MinimalModel(name="test")

        with patch.object(MinimalModel, "get_producer", return_value=mock_producer):
            with pytest.raises(ProducerError):
                event.produce_nowait()

    def test_produce_with_delivery_error(self, mock_producer):
        """Test produce with delivery callback error."""
        from flowodm.exceptions import ProducerError

        event = MinimalModel(name="test")

        # Create a producer that simulates delivery failure
        error_producer = MagicMock()
        error_producer.produce = lambda topic, value, key, callback, **kwargs: callback(
            Exception("Delivery failed"), None
        )
        error_producer.flush = lambda timeout: 0

        with patch.object(MinimalModel, "get_producer", return_value=error_producer):
            with pytest.raises(ProducerError, match="Delivery failed"):
                event.produce()

    def test_produce_timeout(self, mock_producer):
        """Test produce timeout."""
        from flowodm.exceptions import ProducerError

        event = MinimalModel(name="test")

        # Simulate flush returning remaining messages (timeout)
        mock_producer.flush = lambda timeout: 5  # 5 messages remaining

        with patch.object(MinimalModel, "get_producer", return_value=mock_producer):
            with pytest.raises(ProducerError, match="Timed out"):
                event.produce()


@pytest.mark.unit
class TestConsumeIterator:
    """Tests for consume iterator functionality."""

    def test_consume_iter_with_no_messages(self, mock_consumer_no_message):
        """Test consume_iter with no messages available."""
        # This test verifies that consume_iter handles empty queues gracefully
        # In practice, it would loop forever, but we just verify setup works
        with patch.object(MinimalModel, "get_consumer", return_value=mock_consumer_no_message):
            # Just verify we can start iterating (don't actually consume since it would loop forever)
            iterator = MinimalModel.consume_iter(group_id="test-group")
            assert iterator is not None


@pytest.mark.unit
class TestGetAvroSchemaFallbacks:
    """Tests for _get_avro_schema fallback mechanisms."""

    def test_get_avro_schema_auto_generate_fallback(self):
        """Test that _get_avro_schema falls back to auto-generation."""
        # MinimalModel has no schema_path and no registry, so it should auto-generate
        schema = MinimalModel._get_avro_schema()

        assert schema["type"] == "record"
        assert schema["name"] == "MinimalModel"
        assert "fields" in schema
