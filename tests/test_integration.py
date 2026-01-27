"""
Integration tests for FlowODM with real Kafka.

These tests require:
- Kafka broker at KAFKA_BOOTSTRAP_SERVERS (default: localhost:9092)
- Schema Registry at SCHEMA_REGISTRY_URL (default: http://localhost:8081)

Run with: pytest -m integration
"""

import asyncio
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from flowodm import FlowBaseModel
from flowodm.connection import KafkaConnection
from flowodm.consumer import AsyncConsumerLoop, ConsumerLoop
from flowodm.exceptions import DeserializationError, ProducerError


# ==================== Test Models ====================


class IntegrationTestEvent(FlowBaseModel):
    """Test model for integration tests."""

    class Settings:
        topic = f"test-integration-{uuid.uuid4().hex[:8]}"
        schema_subject = None  # Use default {topic}-value
        consumer_group = f"test-group-{uuid.uuid4().hex[:8]}"
        key_field = "event_id"

    event_id: str
    event_type: str
    timestamp: datetime
    payload: dict[str, Any] | None = None


class SimpleTestModel(FlowBaseModel):
    """Simple model without key field."""

    class Settings:
        topic = f"test-simple-{uuid.uuid4().hex[:8]}"

    name: str
    value: int


# ==================== Fixtures ====================


@pytest.fixture(scope="module")
def kafka_bootstrap_servers() -> str:
    """Get Kafka bootstrap servers from environment."""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture(scope="module")
def schema_registry_url() -> str:
    """Get Schema Registry URL from environment."""
    return os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


@pytest.fixture(scope="module")
def kafka_connection(kafka_bootstrap_servers: str, schema_registry_url: str):
    """Initialize Kafka connection for integration tests."""
    conn = KafkaConnection.get_instance(
        bootstrap_servers=kafka_bootstrap_servers,
        schema_registry_url=schema_registry_url,
    )
    yield conn
    # Cleanup
    try:
        conn.close_connection()
    except Exception:
        pass


@pytest.fixture
def test_event() -> IntegrationTestEvent:
    """Create a test event instance."""
    return IntegrationTestEvent(
        event_id=f"evt-{uuid.uuid4().hex[:8]}",
        event_type="test_action",
        timestamp=datetime.now(timezone.utc),
        payload={"key": "value", "count": 42},
    )


# ==================== Sync Producer/Consumer Tests ====================


@pytest.mark.integration
class TestSyncProduceConsume:
    """Test synchronous produce and consume operations."""

    def test_produce_and_consume_one(
        self, kafka_connection: KafkaConnection, test_event: IntegrationTestEvent
    ):
        """Test producing a message and consuming it."""
        # Produce message
        test_event.produce()

        # Consume message
        consumed = IntegrationTestEvent.consume_one(timeout=10.0)

        assert consumed is not None
        assert consumed.event_id == test_event.event_id
        assert consumed.event_type == test_event.event_type
        assert consumed.payload == test_event.payload

    def test_produce_multiple_and_consume_iter(self, kafka_connection: KafkaConnection):
        """Test producing multiple messages and consuming them."""
        # Produce multiple messages
        events = [
            IntegrationTestEvent(
                event_id=f"evt-{i}",
                event_type="batch_test",
                timestamp=datetime.now(timezone.utc),
                payload={"index": i},
            )
            for i in range(5)
        ]

        for event in events:
            event.produce()

        # Consume messages
        consumed_events = []
        for consumed in IntegrationTestEvent.consume_iter(max_messages=5, timeout=10.0):
            consumed_events.append(consumed)
            if len(consumed_events) >= 5:
                break

        assert len(consumed_events) == 5
        consumed_ids = {evt.event_id for evt in consumed_events}
        expected_ids = {evt.event_id for evt in events}
        assert consumed_ids == expected_ids

    def test_produce_with_key(self, kafka_connection: KafkaConnection):
        """Test producing a message with a key field."""
        event = IntegrationTestEvent(
            event_id="key-test-123",
            event_type="key_test",
            timestamp=datetime.now(timezone.utc),
        )

        # Produce with key
        event.produce()

        # Consume and verify
        consumed = IntegrationTestEvent.consume_one(timeout=10.0)
        assert consumed is not None
        assert consumed.event_id == "key-test-123"

    def test_produce_simple_model(self, kafka_connection: KafkaConnection):
        """Test producing a simple model without key field."""
        model = SimpleTestModel(name="test", value=100)

        # Should produce successfully
        model.produce()

        # Consume and verify
        consumed = SimpleTestModel.consume_one(timeout=10.0)
        assert consumed is not None
        assert consumed.name == "test"
        assert consumed.value == 100


# ==================== Async Producer/Consumer Tests ====================


@pytest.mark.integration
class TestAsyncProduceConsume:
    """Test asynchronous produce and consume operations."""

    async def test_async_produce_and_consume(
        self, kafka_connection: KafkaConnection, test_event: IntegrationTestEvent
    ):
        """Test async producing and consuming a message."""
        # Produce message
        await test_event.aproduce()

        # Give Kafka a moment to process
        await asyncio.sleep(0.5)

        # Consume message
        consumed = await IntegrationTestEvent.aconsume_one(timeout=10.0)

        assert consumed is not None
        assert consumed.event_id == test_event.event_id
        assert consumed.event_type == test_event.event_type

    async def test_async_consume_iter(self, kafka_connection: KafkaConnection):
        """Test async consuming multiple messages."""
        # Produce multiple messages
        events = [
            IntegrationTestEvent(
                event_id=f"async-evt-{i}",
                event_type="async_batch_test",
                timestamp=datetime.now(timezone.utc),
                payload={"index": i},
            )
            for i in range(3)
        ]

        for event in events:
            await event.aproduce()

        await asyncio.sleep(0.5)

        # Consume messages
        consumed_events = []
        async for consumed in IntegrationTestEvent.aconsume_iter(
            max_messages=3, timeout=10.0
        ):
            consumed_events.append(consumed)
            if len(consumed_events) >= 3:
                break

        assert len(consumed_events) == 3
        consumed_ids = {evt.event_id for evt in consumed_events}
        expected_ids = {evt.event_id for evt in events}
        assert consumed_ids == expected_ids


# ==================== Consumer Loop Tests ====================


@pytest.mark.integration
class TestConsumerLoops:
    """Test consumer loop functionality."""

    def test_sync_consumer_loop(self, kafka_connection: KafkaConnection):
        """Test synchronous consumer loop."""
        # Produce some messages
        events = [
            IntegrationTestEvent(
                event_id=f"loop-evt-{i}",
                event_type="loop_test",
                timestamp=datetime.now(timezone.utc),
            )
            for i in range(3)
        ]

        for event in events:
            event.produce()

        # Process with consumer loop
        processed = []

        def handler(event: IntegrationTestEvent) -> None:
            processed.append(event)
            if len(processed) >= 3:
                raise KeyboardInterrupt  # Exit loop after processing 3 messages

        loop = ConsumerLoop(
            model_class=IntegrationTestEvent,
            handler=handler,
            max_messages=3,
        )

        try:
            loop.run()
        except KeyboardInterrupt:
            pass

        assert len(processed) == 3

    async def test_async_consumer_loop(self, kafka_connection: KafkaConnection):
        """Test asynchronous consumer loop."""
        # Produce some messages
        events = [
            IntegrationTestEvent(
                event_id=f"async-loop-evt-{i}",
                event_type="async_loop_test",
                timestamp=datetime.now(timezone.utc),
            )
            for i in range(3)
        ]

        for event in events:
            await event.aproduce()

        await asyncio.sleep(0.5)

        # Process with async consumer loop
        processed = []

        async def async_handler(event: IntegrationTestEvent) -> None:
            processed.append(event)
            if len(processed) >= 3:
                raise KeyboardInterrupt  # Exit loop after processing 3 messages

        loop = AsyncConsumerLoop(
            model_class=IntegrationTestEvent,
            handler=async_handler,
            max_messages=3,
        )

        try:
            await loop.run()
        except KeyboardInterrupt:
            pass

        assert len(processed) == 3


# ==================== Schema Registry Tests ====================


@pytest.mark.integration
class TestSchemaRegistry:
    """Test Schema Registry integration."""

    def test_schema_subject_registration(self, kafka_connection: KafkaConnection):
        """Test that producing registers schema in Schema Registry."""
        event = IntegrationTestEvent(
            event_id="schema-test",
            event_type="schema_test",
            timestamp=datetime.now(timezone.utc),
        )

        # Produce message (should register schema)
        event.produce()

        # Verify schema is registered
        registry = kafka_connection.get_schema_registry()
        topic = IntegrationTestEvent._get_topic()
        subject = f"{topic}-value"

        # Get subjects - this should include our subject
        subjects = registry.get_subjects()
        assert subject in subjects

    def test_schema_evolution_compatibility(self, kafka_connection: KafkaConnection):
        """Test schema compatibility checking."""
        # This test verifies that Schema Registry is working
        # In a real scenario, you'd test schema evolution
        event = IntegrationTestEvent(
            event_id="compat-test",
            event_type="compatibility_test",
            timestamp=datetime.now(timezone.utc),
        )

        event.produce()

        # Consume should work with same schema
        consumed = IntegrationTestEvent.consume_one(timeout=10.0)
        assert consumed is not None
        assert consumed.event_id == "compat-test"


# ==================== Error Handling Tests ====================


@pytest.mark.integration
class TestErrorHandling:
    """Test error handling in integration scenarios."""

    def test_consume_timeout(self, kafka_connection: KafkaConnection):
        """Test that consume_one returns None on timeout."""
        # Use a fresh topic with no messages
        class EmptyTopicModel(FlowBaseModel):
            class Settings:
                topic = f"test-empty-{uuid.uuid4().hex[:8]}"
                consumer_group = f"test-group-{uuid.uuid4().hex[:8]}"

            name: str

        # Should timeout and return None
        consumed = EmptyTopicModel.consume_one(timeout=2.0)
        assert consumed is None

    async def test_async_consume_timeout(self, kafka_connection: KafkaConnection):
        """Test that aconsume_one returns None on timeout."""
        # Use a fresh topic with no messages
        class EmptyAsyncTopicModel(FlowBaseModel):
            class Settings:
                topic = f"test-empty-async-{uuid.uuid4().hex[:8]}"
                consumer_group = f"test-group-{uuid.uuid4().hex[:8]}"

            name: str

        # Should timeout and return None
        consumed = await EmptyAsyncTopicModel.aconsume_one(timeout=2.0)
        assert consumed is None


# ==================== Connection Management Tests ====================


@pytest.mark.integration
class TestConnectionManagement:
    """Test connection lifecycle management."""

    def test_connection_singleton(
        self, kafka_bootstrap_servers: str, schema_registry_url: str
    ):
        """Test that KafkaConnection is a singleton."""
        conn1 = KafkaConnection.get_instance(
            bootstrap_servers=kafka_bootstrap_servers,
            schema_registry_url=schema_registry_url,
        )
        conn2 = KafkaConnection.get_instance()

        assert conn1 is conn2

    def test_producer_reuse(self, kafka_connection: KafkaConnection):
        """Test that producer is reused across produce calls."""
        event1 = SimpleTestModel(name="test1", value=1)
        event2 = SimpleTestModel(name="test2", value=2)

        # Both should use the same producer instance
        event1.produce()
        event2.produce()

        # Verify both messages were produced
        consumed1 = SimpleTestModel.consume_one(timeout=5.0)
        consumed2 = SimpleTestModel.consume_one(timeout=5.0)

        assert consumed1 is not None
        assert consumed2 is not None
        names = {consumed1.name, consumed2.name}
        assert names == {"test1", "test2"}
