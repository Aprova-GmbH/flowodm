"""Unit tests for FlowBaseModel."""

from datetime import datetime
from unittest.mock import patch

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
