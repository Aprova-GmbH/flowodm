"""
Integration tests for FlowODM with real Kafka.

These tests require:
- Kafka broker at KAFKA_BOOTSTRAP_SERVERS (default: localhost:9092)
- Schema Registry at SCHEMA_REGISTRY_URL (default: http://localhost:8081)

Run with: pytest -m integration
"""

import asyncio
import json
import os
import uuid
from datetime import UTC, datetime

import pytest

from flowodm import FlowBaseModel
from flowodm.connection import KafkaConnection, connect
from flowodm.consumer import AsyncConsumerLoop, ConsumerLoop
from flowodm.settings import BaseSettings

# ==================== Test Model Factories ====================


def create_test_event_model(topic_suffix: str | None = None):
    """Create a test event model class with a unique topic."""
    if topic_suffix is None:
        topic_suffix = uuid.uuid4().hex[:8]

    class IntegrationTestEvent(FlowBaseModel):
        """Test model for integration tests."""

        class Settings:
            topic = f"test-integration-{topic_suffix}"
            schema_subject = None  # Use default {topic}-value
            consumer_group = f"test-group-{topic_suffix}"
            key_field = "event_id"

        event_id: str
        event_type: str
        timestamp: datetime
        payload: str | None = None

    return IntegrationTestEvent


def create_simple_test_model(topic_suffix: str | None = None):
    """Create a simple test model class with a unique topic."""
    if topic_suffix is None:
        topic_suffix = uuid.uuid4().hex[:8]

    class SimpleTestModel(FlowBaseModel):
        """Simple model without key field."""

        class Settings:
            topic = f"test-simple-{topic_suffix}"
            consumer_group = f"test-group-{topic_suffix}"

        name: str
        value: int

    return SimpleTestModel


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
    conn = connect(
        bootstrap_servers=kafka_bootstrap_servers,
        schema_registry_url=schema_registry_url,
    )
    yield conn
    # Cleanup
    try:
        conn.close()
    except Exception:
        pass


# ==================== Sync Producer/Consumer Tests ====================


@pytest.mark.integration
class TestSyncProduceConsume:
    """Test synchronous produce and consume operations."""

    def test_produce_and_consume_one(self, kafka_connection: KafkaConnection):
        """Test producing a message and consuming it."""
        # Create a model with unique topic
        TestModel = create_test_event_model()

        # Create event with unique topic and consumer group
        event = TestModel(
            event_id=f"evt-{uuid.uuid4().hex[:8]}",
            event_type="test_action",
            timestamp=datetime.now(UTC),
            payload=json.dumps({"key": "value", "count": 42}),
        )

        # Produce message
        event.produce()

        # Give Kafka a moment to commit
        import time

        time.sleep(0.2)

        # Consume message with explicit "earliest" setting
        consumed = TestModel.consume_one(timeout=10.0, settings=BaseSettings())

        assert (
            consumed is not None
        ), "Failed to consume message - check Kafka consumer offset strategy"
        assert consumed.event_id == event.event_id
        assert consumed.event_type == event.event_type
        assert consumed.payload == event.payload  # JSON string comparison

    def test_produce_multiple_and_consume_iter(self, kafka_connection: KafkaConnection):
        """Test producing multiple messages and consuming them."""
        # Create a model with unique topic
        TestModel = create_test_event_model()

        # Produce multiple messages
        events = [
            TestModel(
                event_id=f"evt-batch-{i}",
                event_type="batch_test",
                timestamp=datetime.now(UTC),
                payload=json.dumps({"index": i}),
            )
            for i in range(5)
        ]

        for event in events:
            event.produce()

        # Give Kafka a moment to commit
        import time

        time.sleep(0.2)

        # Consume messages
        consumed_events = []
        for consumed in TestModel.consume_iter(timeout=2.0, settings=BaseSettings()):
            consumed_events.append(consumed)
            if len(consumed_events) >= 5:
                break

        assert len(consumed_events) == 5
        consumed_ids = {evt.event_id for evt in consumed_events}
        expected_ids = {evt.event_id for evt in events}
        assert consumed_ids == expected_ids

    def test_produce_with_key(self, kafka_connection: KafkaConnection):
        """Test producing a message with a key field."""
        # Create a model with unique topic
        TestModel = create_test_event_model()

        unique_id = f"key-test-{uuid.uuid4().hex[:8]}"
        event = TestModel(
            event_id=unique_id,
            event_type="key_test",
            timestamp=datetime.now(UTC),
        )

        # Produce with key
        event.produce()

        # Give Kafka a moment to commit
        import time

        time.sleep(0.2)

        # Consume and verify
        consumed = TestModel.consume_one(timeout=10.0, settings=BaseSettings())
        assert consumed is not None
        assert consumed.event_id == unique_id

    def test_produce_simple_model(self, kafka_connection: KafkaConnection):
        """Test producing a simple model without key field."""
        # Create a model with unique topic
        TestModel = create_simple_test_model()

        model = TestModel(name="test", value=100)

        # Should produce successfully
        model.produce()

        # Give Kafka a moment to commit
        import time

        time.sleep(0.2)

        # Consume and verify
        consumed = TestModel.consume_one(timeout=10.0, settings=BaseSettings())
        assert consumed is not None
        assert consumed.name == "test"
        assert consumed.value == 100


# ==================== Async Producer/Consumer Tests ====================


@pytest.mark.integration
class TestAsyncProduceConsume:
    """Test asynchronous produce and consume operations."""

    async def test_async_produce_and_consume(self, kafka_connection: KafkaConnection):
        """Test async producing and consuming a message."""
        # Create a model with unique topic
        TestModel = create_test_event_model()

        # Create event
        event = TestModel(
            event_id=f"evt-async-{uuid.uuid4().hex[:8]}",
            event_type="test_async",
            timestamp=datetime.now(UTC),
            payload=json.dumps({"async": True}),
        )

        # Produce message
        await event.aproduce()

        # Give Kafka a moment to process
        await asyncio.sleep(0.5)

        # Consume message
        consumed = await TestModel.aconsume_one(timeout=10.0, settings=BaseSettings())

        assert consumed is not None
        assert consumed.event_id == event.event_id
        assert consumed.event_type == event.event_type

    async def test_async_consume_iter(self, kafka_connection: KafkaConnection):
        """Test async consuming multiple messages."""
        # Create a model with unique topic
        TestModel = create_test_event_model()

        # Produce multiple messages
        events = [
            TestModel(
                event_id=f"async-evt-{i}",
                event_type="async_batch_test",
                timestamp=datetime.now(UTC),
                payload=json.dumps({"index": i}),
            )
            for i in range(3)
        ]

        for event in events:
            await event.aproduce()

        await asyncio.sleep(0.5)

        # Consume messages
        consumed_events = []
        async for consumed in TestModel.aconsume_iter(timeout=2.0, settings=BaseSettings()):
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
        # Create a model with unique topic
        TestModel = create_test_event_model()

        # Produce some messages
        events = [
            TestModel(
                event_id=f"loop-evt-{i}",
                event_type="loop_test",
                timestamp=datetime.now(UTC),
            )
            for i in range(3)
        ]

        for event in events:
            event.produce()

        # Give Kafka a moment to commit
        import time

        time.sleep(0.2)

        # Process with consumer loop
        processed = []

        def handler(event) -> None:
            processed.append(event)
            if len(processed) >= 3:
                raise KeyboardInterrupt  # Exit loop after processing 3 messages

        loop = ConsumerLoop(
            model=TestModel,
            handler=handler,
        )

        try:
            loop.run()
        except KeyboardInterrupt:
            pass

        assert len(processed) == 3

    @pytest.mark.skip(reason="Async consumer loop hangs - needs investigation")
    async def test_async_consumer_loop(self, kafka_connection: KafkaConnection):
        """Test asynchronous consumer loop."""
        # Create a model with unique topic
        TestModel = create_test_event_model()

        # Produce some messages
        events = [
            TestModel(
                event_id=f"async-loop-evt-{i}",
                event_type="async_loop_test",
                timestamp=datetime.now(UTC),
            )
            for i in range(3)
        ]

        for event in events:
            await event.aproduce()

        await asyncio.sleep(0.5)

        # Process with async consumer loop
        processed = []

        async def async_handler(event) -> None:
            processed.append(event)
            if len(processed) >= 3:
                raise KeyboardInterrupt  # Exit loop after processing 3 messages

        loop = AsyncConsumerLoop(
            model=TestModel,
            handler=async_handler,
        )

        try:
            await loop.run()
        except KeyboardInterrupt:
            pass

        assert len(processed) == 3


# ==================== Schema Registry Tests ====================


@pytest.mark.integration
@pytest.mark.skip(reason="Requires Schema Registry - docker-compose schema-registry not working")
class TestSchemaRegistry:
    """Test Schema Registry integration."""

    def test_schema_subject_registration(self, kafka_connection: KafkaConnection):
        """Test that producing registers schema in Schema Registry."""
        TestModel = create_test_event_model()

        event = TestModel(
            event_id=f"schema-test-{uuid.uuid4().hex[:8]}",
            event_type="schema_test",
            timestamp=datetime.now(UTC),
        )

        # Produce message (should register schema)
        event.produce()

        # Verify schema is registered
        registry = kafka_connection.schema_registry
        topic = TestModel._get_topic()
        subject = f"{topic}-value"

        # Get subjects - this should include our subject
        subjects = registry.get_subjects()
        assert subject in subjects

    def test_schema_evolution_compatibility(self, kafka_connection: KafkaConnection):
        """Test schema compatibility checking."""
        # This test verifies that Schema Registry is working
        # In a real scenario, you'd test schema evolution
        TestModel = create_test_event_model()

        unique_id = f"compat-test-{uuid.uuid4().hex[:8]}"
        event = TestModel(
            event_id=unique_id,
            event_type="compatibility_test",
            timestamp=datetime.now(UTC),
        )

        event.produce()

        # Consume should work with same schema
        consumed = TestModel.consume_one(timeout=10.0, settings=BaseSettings())
        assert consumed is not None
        assert consumed.event_id == unique_id


# ==================== Error Handling Tests ====================


@pytest.mark.integration
class TestErrorHandling:
    """Test error handling in integration scenarios."""

    def test_consume_timeout(self, kafka_connection: KafkaConnection):
        """Test that consume_one returns None on timeout."""
        # Use a fresh topic with no messages
        EmptyTopicModel = create_simple_test_model()

        # Should timeout and return None
        consumed = EmptyTopicModel.consume_one(timeout=2.0, settings=BaseSettings())
        assert consumed is None

    async def test_async_consume_timeout(self, kafka_connection: KafkaConnection):
        """Test that aconsume_one returns None on timeout."""
        # Use a fresh topic with no messages
        EmptyAsyncTopicModel = create_simple_test_model()

        # Should timeout and return None
        consumed = await EmptyAsyncTopicModel.aconsume_one(timeout=2.0, settings=BaseSettings())
        assert consumed is None


# ==================== Connection Management Tests ====================


@pytest.mark.integration
class TestConnectionManagement:
    """Test connection lifecycle management."""

    def test_connection_singleton(self, kafka_bootstrap_servers: str, schema_registry_url: str):
        """Test that KafkaConnection is a singleton."""
        conn1 = connect(
            bootstrap_servers=kafka_bootstrap_servers,
            schema_registry_url=schema_registry_url,
        )
        conn2 = connect()

        assert conn1 is conn2

    def test_producer_reuse(self, kafka_connection: KafkaConnection):
        """Test that producer is reused across produce calls."""
        # Create a model with unique topic
        TestModel = create_simple_test_model()

        event1 = TestModel(name="test1", value=1)
        event2 = TestModel(name="test2", value=2)

        # Both should use the same producer instance
        event1.produce()
        event2.produce()

        # Give Kafka a moment to commit
        import time

        time.sleep(0.2)

        # Verify both messages were produced
        consumed1 = TestModel.consume_one(timeout=5.0, settings=BaseSettings())
        consumed2 = TestModel.consume_one(timeout=5.0, settings=BaseSettings())

        assert consumed1 is not None
        assert consumed2 is not None
        names = {consumed1.name, consumed2.name}
        assert names == {"test1", "test2"}
