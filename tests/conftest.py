"""
Pytest configuration and fixtures for FlowODM tests.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


class MockMessage:
    """Mock Kafka message for testing."""

    def __init__(
        self,
        value: bytes,
        key: bytes | None = None,
        topic: str = "test-topic",
        partition: int = 0,
        offset: int = 0,
        error_val: Any = None,
    ):
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = error_val

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> Any:
        return self._error


class MockProducer:
    """Mock confluent-kafka Producer for unit tests."""

    def __init__(self):
        self.messages: list[dict[str, Any]] = []
        self._flushed = False

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        callback: Any = None,
        **kwargs: Any,
    ) -> None:
        self.messages.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
            }
        )
        if callback:
            # Simulate successful delivery
            callback(None, MockMessage(value, key, topic))

    def flush(self, timeout: float = None) -> int:
        self._flushed = True
        return 0

    def poll(self, timeout: float = 0) -> int:
        return 0


class MockConsumer:
    """Mock confluent-kafka Consumer for unit tests."""

    def __init__(self, messages: list[MockMessage] | None = None):
        self._messages = messages or []
        self._index = 0
        self._subscribed_topics: list[str] = []
        self._committed: list[MockMessage] = []

    def subscribe(self, topics: list[str]) -> None:
        self._subscribed_topics = topics

    def poll(self, timeout: float = 1.0) -> MockMessage | None:
        if self._index < len(self._messages):
            msg = self._messages[self._index]
            self._index += 1
            return msg
        return None

    def commit(self, message: MockMessage | None = None, asynchronous: bool = True) -> None:
        if message:
            self._committed.append(message)

    def close(self) -> None:
        pass

    def add_message(self, msg: MockMessage) -> None:
        """Add a message to the queue for testing."""
        self._messages.append(msg)


class MockSchemaRegistryClient:
    """Mock Schema Registry client for unit tests."""

    def __init__(self):
        self._schemas: dict[str, dict[str, Any]] = {}
        self._next_id = 1
        self._subjects: list[str] = []

    def register_schema(self, subject: str, schema: Any) -> int:
        schema_id = self._next_id
        self._next_id += 1
        self._schemas[subject] = {
            "id": schema_id,
            "schema": schema,
            "version": len(self._schemas.get(subject, {}).get("versions", [])) + 1,
        }
        if subject not in self._subjects:
            self._subjects.append(subject)
        return schema_id

    def get_latest_version(self, subject: str) -> Any:
        if subject not in self._schemas:
            raise Exception(f"Subject not found: {subject}")
        return MagicMock(
            schema=MagicMock(schema_str='{"type": "record", "name": "Test", "fields": []}')
        )

    def get_version(self, subject: str, version: int) -> Any:
        return self.get_latest_version(subject)

    def get_subjects(self) -> list[str]:
        return self._subjects

    def get_versions(self, subject: str) -> list[int]:
        return [1]

    def test_compatibility(self, subject: str, schema: Any) -> bool:
        return True

    def set_compatibility(self, subject: str, level: str) -> None:
        pass

    def delete_subject(self, subject: str, permanent: bool = False) -> list[int]:
        if subject in self._subjects:
            self._subjects.remove(subject)
        return [1]


@pytest.fixture
def mock_producer():
    """Create a mock Kafka producer."""
    return MockProducer()


@pytest.fixture
def mock_consumer():
    """Create a mock Kafka consumer."""
    return MockConsumer()


@pytest.fixture
def mock_schema_registry():
    """Create a mock Schema Registry client."""
    return MockSchemaRegistryClient()


@pytest.fixture(autouse=True)
def reset_connection():
    """Reset KafkaConnection singleton between tests."""
    import flowodm.connection as conn_module
    from flowodm.connection import KafkaConnection

    # Reset singleton state
    KafkaConnection._instance = None
    KafkaConnection._bootstrap_servers = None
    KafkaConnection._producer = None
    KafkaConnection._schema_registry = None
    conn_module._kafka_conn = None

    yield

    # Cleanup after test
    if KafkaConnection._instance:
        try:
            KafkaConnection._instance.close_connection()
        except Exception:
            pass
    KafkaConnection._instance = None
    conn_module._kafka_conn = None


@pytest.fixture
def sample_avro_schema():
    """Sample Avro schema for testing."""
    return {
        "type": "record",
        "name": "UserEvent",
        "namespace": "test",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "action", "type": "string"},
            {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "message_id", "type": "string"},
        ],
    }


@pytest.fixture
def sample_user_event_bytes(sample_avro_schema):
    """Sample serialized Avro message bytes."""
    import io

    import fastavro

    data = {
        "user_id": "user-123",
        "action": "login",
        "timestamp": 1704067200000,  # 2024-01-01 00:00:00 UTC
        "message_id": "msg-456",
    }

    output = io.BytesIO()
    fastavro.schemaless_writer(output, sample_avro_schema, data)
    return output.getvalue()


@pytest.fixture
def mock_consumer_with_message():
    """Create a mock consumer with a test message."""
    import io

    import fastavro

    from flowodm import FlowBaseModel

    # Create a minimal model for testing
    class MinimalModel(FlowBaseModel):
        class Settings:
            topic = "minimal-topic"

        name: str

    # Serialize a test message
    instance = MinimalModel(name="test")
    schema = instance._generate_avro_schema()
    data = instance._to_avro_dict()

    output = io.BytesIO()
    fastavro.schemaless_writer(output, schema, data)
    value_bytes = output.getvalue()

    message = MockMessage(value=value_bytes)
    consumer = MockConsumer(messages=[message])
    return consumer


@pytest.fixture
def mock_consumer_no_message():
    """Create a mock consumer with no messages."""
    return MockConsumer(messages=[])


@pytest.fixture
def mock_consumer_with_batch():
    """Create a mock consumer with multiple messages."""
    import io

    import fastavro

    from flowodm import FlowBaseModel

    class MinimalModel(FlowBaseModel):
        class Settings:
            topic = "minimal-topic"

        name: str

    schema = MinimalModel._generate_avro_schema()
    messages = []

    for i in range(3):
        instance = MinimalModel(name=f"test-{i}")
        data = instance._to_avro_dict()

        output = io.BytesIO()
        fastavro.schemaless_writer(output, schema, data)
        value_bytes = output.getvalue()

        messages.append(MockMessage(value=value_bytes))

    return MockConsumer(messages=messages)


class MockAsyncProducer:
    """Mock async Kafka producer for unit tests."""

    def __init__(self):
        self.messages: list[dict[str, Any]] = []

    async def produce_async(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        **kwargs: Any,
    ) -> None:
        self.messages.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
            }
        )

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        callback: Any = None,
        **kwargs: Any,
    ) -> None:
        """Fallback sync produce method."""
        self.messages.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
            }
        )

    def poll(self, timeout: float = 0) -> int:
        return 0


class MockAsyncConsumer:
    """Mock async Kafka consumer for unit tests."""

    def __init__(self, messages: list[MockMessage] | None = None):
        self._messages = messages or []
        self._index = 0
        self._committed: list[MockMessage] = []

    async def poll_async(self, timeout: float = 1.0) -> MockMessage | None:
        if self._index < len(self._messages):
            msg = self._messages[self._index]
            self._index += 1
            return msg
        return None

    def poll(self, timeout: float = 1.0) -> MockMessage | None:
        """Fallback sync poll method."""
        if self._index < len(self._messages):
            msg = self._messages[self._index]
            self._index += 1
            return msg
        return None

    def commit(self, message: MockMessage | None = None, asynchronous: bool = True) -> None:
        if message:
            self._committed.append(message)

    def close(self) -> None:
        pass


@pytest.fixture
def mock_async_producer():
    """Create a mock async Kafka producer."""
    return MockAsyncProducer()


@pytest.fixture
def mock_async_consumer_with_message():
    """Create a mock async consumer with a test message."""
    import io

    import fastavro

    from flowodm import FlowBaseModel

    class MinimalModel(FlowBaseModel):
        class Settings:
            topic = "minimal-topic"

        name: str

    # Serialize a test message
    instance = MinimalModel(name="test")
    schema = instance._generate_avro_schema()
    data = instance._to_avro_dict()

    output = io.BytesIO()
    fastavro.schemaless_writer(output, schema, data)
    value_bytes = output.getvalue()

    message = MockMessage(value=value_bytes)
    consumer = MockAsyncConsumer(messages=[message])
    return consumer
