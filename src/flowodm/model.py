"""
FlowBaseModel - Pydantic v2 base class for Kafka message models.

Provides both synchronous and asynchronous methods for produce/consume operations.
"""

from __future__ import annotations

import io
import struct
from collections.abc import AsyncIterator, Iterator
from datetime import datetime
from enum import Enum
from typing import Any, TypeVar

import fastavro
from confluent_kafka import Consumer, Message, Producer
from pydantic import BaseModel, ConfigDict

from flowodm.connection import (
    get_async_consumer,
    get_async_producer,
    get_consumer,
    get_producer,
    get_schema_registry,
)
from flowodm.exceptions import (
    ConfigurationError,
    DeserializationError,
    ProducerError,
    SerializationError,
    SettingsError,
)
from flowodm.settings import BaseSettings

T = TypeVar("T", bound="FlowBaseModel")

# Cache for schema IDs keyed by class name, stored at module level
# to avoid Pydantic treating it as a model attribute
_schema_id_cache: dict[str, int] = {}

# Confluent wire format constants
# Confluent Kafka messages use a wire format with:
# - Byte 0: Magic byte (0x00)
# - Bytes 1-4: Schema ID (big-endian 4-byte integer)
# - Bytes 5+: Actual Avro serialized data
CONFLUENT_MAGIC_BYTE = 0x00
CONFLUENT_HEADER_SIZE = 5  # 1 byte magic + 4 bytes schema ID


class FlowBaseModel(BaseModel):
    """
    Base class for Kafka message models with ODM functionality.

    Provides both synchronous and asynchronous methods for produce/consume.
    Maps Pydantic models to Avro schemas automatically.

    Subclasses must define an inner Settings class:

    Example:
        class UserEvent(FlowBaseModel):
            class Settings:
                topic = "user-events"
                schema_subject = "user-events-value"  # Optional
                consumer_group = "my-service"  # Optional

            user_id: str
            action: str
            timestamp: datetime
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    class Settings:
        """
        Settings for the Kafka model.

        Configuration class for defining Kafka topic, schema, and consumer settings.
        The type annotations provide the documentation for each setting.
        """

        topic: str | None = None
        """Kafka topic name (required)"""
        schema_subject: str | None = None
        """Schema Registry subject (defaults to {topic}-value)"""
        schema_path: str | None = None
        """Path to local .avsc file (optional)"""
        consumer_group: str | None = None
        """Consumer group ID (optional)"""
        key_field: str | None = None
        """Field name to use as message key (optional)"""
        key_serializer: str = "string"
        """Key serialization format: "string", "avro", "json" """
        value_serializer: str = "avro"
        """Value serialization format: "avro", "json" """
        confluent_wire_format: bool = True
        """Prepend Confluent wire format header (magic byte + schema ID) when serializing"""

    # ==================== Class Methods for Configuration ====================

    @classmethod
    def _get_topic(cls) -> str:
        """Get topic name from Settings."""
        settings = getattr(cls, "Settings", None)
        if settings is None:
            raise SettingsError(f"{cls.__name__} must define an inner Settings class")

        topic: str | None = getattr(settings, "topic", None)
        if not topic:
            raise SettingsError(f"{cls.__name__}.Settings must define 'topic'")

        return topic

    @classmethod
    def _get_schema_subject(cls) -> str:
        """Get schema subject (defaults to {topic}-value)."""
        settings = getattr(cls, "Settings", None)
        subject: str | None = getattr(settings, "schema_subject", None) if settings else None
        if subject:
            return subject
        return f"{cls._get_topic()}-value"

    @classmethod
    def _get_consumer_group(cls) -> str | None:
        """Get consumer group from Settings."""
        settings = getattr(cls, "Settings", None)
        return getattr(settings, "consumer_group", None) if settings else None

    @classmethod
    def _get_key_field(cls) -> str | None:
        """Get key field from Settings."""
        settings = getattr(cls, "Settings", None)
        return getattr(settings, "key_field", None) if settings else None

    @classmethod
    def _get_schema_path(cls) -> str | None:
        """Get schema path from Settings."""
        settings = getattr(cls, "Settings", None)
        return getattr(settings, "schema_path", None) if settings else None

    @classmethod
    def _get_confluent_wire_format(cls) -> bool:
        """Get confluent_wire_format setting (defaults to True)."""
        settings = getattr(cls, "Settings", None)
        return getattr(settings, "confluent_wire_format", True) if settings else True

    @classmethod
    def _get_or_register_schema_id(cls) -> int:
        """
        Get or register the schema ID for this model.

        Uses a module-level cache to avoid repeated registry calls.

        Returns:
            Schema ID from registry

        Raises:
            ConfigurationError: If no Schema Registry is configured
        """
        cache_key = f"{cls.__module__}.{cls.__qualname__}"
        if cache_key not in _schema_id_cache:
            _schema_id_cache[cache_key] = cls.register_schema()
        return _schema_id_cache[cache_key]

    @classmethod
    def _get_avro_schema(cls) -> dict[str, Any]:
        """
        Get Avro schema for this model.

        Priority:
        1. Load from schema_path if specified
        2. Load from Schema Registry if schema_subject specified
        3. Auto-generate from Pydantic model
        """
        # Try loading from file
        schema_path = cls._get_schema_path()
        if schema_path:
            with open(schema_path) as f:
                import json

                schema_from_file: dict[str, Any] = json.load(f)
                return schema_from_file

        # Try loading from Schema Registry
        try:
            registry = get_schema_registry()
            subject = cls._get_schema_subject()
            schema = registry.get_latest_version(subject)
            import json

            schema_str = schema.schema.schema_str
            if not schema_str:
                raise ValueError("Schema string is empty")
            schema_from_registry: dict[str, Any] = json.loads(schema_str)
            return schema_from_registry
        except Exception:
            pass

        # Auto-generate from Pydantic model
        return cls._generate_avro_schema()

    @classmethod
    def _generate_avro_schema(cls) -> dict[str, Any]:
        """Generate Avro schema from Pydantic model fields."""
        fields = []
        for field_name, field_info in cls.model_fields.items():
            avro_type = cls._python_type_to_avro(field_info.annotation)

            # Handle optional fields
            field_type: str | dict[str, Any] | list[str | dict[str, Any]]
            if field_info.is_required():
                field_type = avro_type
            else:
                field_type = ["null", avro_type]

            fields.append({"name": field_name, "type": field_type})

        return {
            "type": "record",
            "name": cls.__name__,
            "namespace": cls.__module__,
            "fields": fields,
        }

    @classmethod
    def _python_type_to_avro(cls, python_type: Any) -> str | dict[str, Any]:
        """Convert Python type annotation to Avro type."""
        # Handle None type
        if python_type is type(None):
            return "null"

        # Handle basic types
        type_mapping = {
            str: "string",
            int: "long",
            float: "double",
            bool: "boolean",
            bytes: "bytes",
        }

        # Get origin type for generic types
        origin = getattr(python_type, "__origin__", None)

        if origin is None:
            # Simple type
            if python_type in type_mapping:
                return type_mapping[python_type]
            if python_type == datetime:
                return {"type": "long", "logicalType": "timestamp-millis"}
            return "string"  # Default fallback

        # Handle Optional (Union with None)
        if origin is type(None):
            return "null"

        return "string"  # Default fallback

    # ==================== Serialization ====================

    def _to_avro_dict(self) -> dict[str, Any]:
        """Convert model to Avro-compatible dictionary."""
        data = self.model_dump(mode="python")

        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = int(value.timestamp() * 1000)
            elif isinstance(value, Enum):
                data[key] = value.value

        return data

    @classmethod
    def _from_avro_dict(cls: type[T], data: dict[str, Any]) -> T:
        """Create model instance from Avro dictionary."""
        # Convert timestamp milliseconds back to datetime
        for field_name, field_info in cls.model_fields.items():
            if field_name in data and field_info.annotation == datetime:
                if isinstance(data[field_name], int):
                    data[field_name] = datetime.fromtimestamp(data[field_name] / 1000)

        return cls.model_validate(data)

    def _serialize_avro(self) -> bytes:
        """Serialize model to Avro bytes.

        When confluent_wire_format is enabled (default) and a Schema Registry
        is configured, prepends the 5-byte Confluent wire format header
        (magic byte 0x00 + 4-byte big-endian schema ID) before the Avro data.
        """
        schema = self._get_avro_schema()
        data = self._to_avro_dict()

        output = io.BytesIO()

        # Prepend Confluent wire format header if enabled and registry available
        if self._get_confluent_wire_format():
            try:
                schema_id = self._get_or_register_schema_id()
                output.write(struct.pack(">bI", CONFLUENT_MAGIC_BYTE, schema_id))
            except ConfigurationError:
                pass  # No Schema Registry configured → raw Avro

        try:
            fastavro.schemaless_writer(output, schema, data)
            return output.getvalue()
        except Exception as e:
            raise SerializationError(f"Failed to serialize to Avro: {e}") from e

    @classmethod
    def _strip_confluent_header(cls, data: bytes) -> bytes:
        """
        Strip Confluent wire format header if present.

        Confluent Kafka messages use a wire format with:
        - Byte 0: Magic byte (0x00)
        - Bytes 1-4: Schema ID (big-endian 4-byte integer)
        - Bytes 5+: Actual Avro serialized data

        This method detects and removes this header, returning pure Avro bytes.

        Args:
            data: Raw message bytes (may include Confluent header)

        Returns:
            Pure Avro bytes without the Confluent header
        """
        if len(data) >= CONFLUENT_HEADER_SIZE and data[0] == CONFLUENT_MAGIC_BYTE:
            return data[CONFLUENT_HEADER_SIZE:]
        return data

    @classmethod
    def _deserialize_avro(cls: type[T], data: bytes) -> T:
        """
        Deserialize Avro bytes to model instance.

        Automatically handles both pure Avro format and Confluent wire format
        (which includes a 5-byte header with magic byte and schema ID).

        Args:
            data: Avro bytes (with or without Confluent header)

        Returns:
            Model instance
        """
        schema = cls._get_avro_schema()
        # Strip Confluent wire format header if present
        avro_data = cls._strip_confluent_header(data)
        input_stream = io.BytesIO(avro_data)

        try:
            record: dict[str, Any] = fastavro.schemaless_reader(input_stream, schema)  # type: ignore[assignment,call-arg]

            # Validate all bytes were consumed (catches wire format mismatches)
            bytes_read = input_stream.tell()
            if bytes_read != len(avro_data):
                raise DeserializationError(
                    f"Incomplete deserialization: read {bytes_read} of {len(avro_data)} bytes. "
                    "This may indicate a wire format mismatch or schema incompatibility."
                )

            return cls._from_avro_dict(record)
        except DeserializationError:
            raise
        except Exception as e:
            raise DeserializationError(f"Failed to deserialize from Avro: {e}") from e

    def _get_message_key(self) -> bytes | None:
        """Get message key based on key_field setting."""
        key_field = self._get_key_field()
        if not key_field:
            return None

        key_value = getattr(self, key_field, None)
        if key_value is None:
            return None

        return str(key_value).encode("utf-8")

    # ==================== Producer/Consumer Access ====================

    @classmethod
    def get_producer(cls) -> Producer:
        """Get sync Kafka producer. Override for custom connection logic."""
        return get_producer()

    @classmethod
    async def get_async_producer(cls) -> Any:
        """Get async Kafka producer. Override for custom connection logic."""
        return await get_async_producer()

    @classmethod
    def get_consumer(
        cls, group_id: str | None = None, settings: BaseSettings | None = None
    ) -> Consumer:
        """Get sync Kafka consumer. Override for custom connection logic."""
        group = group_id or cls._get_consumer_group()
        if not group:
            raise SettingsError(
                f"{cls.__name__} requires consumer_group in Settings or group_id parameter"
            )
        return get_consumer(group, [cls._get_topic()], settings)

    @classmethod
    async def get_async_consumer(
        cls, group_id: str | None = None, settings: BaseSettings | None = None
    ) -> Any:
        """Get async Kafka consumer. Override for custom connection logic."""
        group = group_id or cls._get_consumer_group()
        if not group:
            raise SettingsError(
                f"{cls.__name__} requires consumer_group in Settings or group_id parameter"
            )
        return await get_async_consumer(group, [cls._get_topic()], settings)

    # ==================== Produce Operations (Sync) ====================

    def produce_nowait(self, callback: Any | None = None) -> None:
        """
        Produce message to Kafka (non-blocking, fire-and-forget).

        Args:
            callback: Optional delivery callback function(err, msg)
        """
        producer = self.get_producer()
        topic = self._get_topic()
        value = self._serialize_avro()
        key = self._get_message_key()

        try:
            producer.produce(
                topic=topic,
                value=value,
                key=key,
                callback=callback,
            )
            producer.poll(0)  # Trigger delivery reports
        except Exception as e:
            raise ProducerError(f"Failed to produce message: {e}") from e

    def produce(self, timeout: float = 10.0) -> None:
        """
        Produce message and wait for delivery confirmation (blocking).

        Args:
            timeout: Maximum time to wait for delivery (seconds)
        """
        delivery_error: Exception | None = None

        def on_delivery(err: Any, msg: Message) -> None:
            nonlocal delivery_error
            if err:
                delivery_error = ProducerError(f"Delivery failed: {err}")

        self.produce_nowait(callback=on_delivery)

        producer = self.get_producer()
        remaining = producer.flush(timeout=timeout)

        if remaining > 0:
            raise ProducerError(f"Timed out waiting for message delivery ({remaining} pending)")

        if delivery_error:
            raise delivery_error

    @classmethod
    def produce_many(cls, messages: list[FlowBaseModel], flush: bool = True) -> int:
        """
        Produce multiple messages (batch).

        Args:
            messages: List of model instances to produce
            flush: Whether to wait for all deliveries

        Returns:
            Number of messages produced
        """
        producer = cls.get_producer() if messages else None
        count = 0

        for msg in messages:
            msg.produce_nowait()
            count += 1

        if flush and producer:
            producer.flush()

        return count

    # ==================== Produce Operations (Async) ====================

    async def aproduce(self) -> None:
        """Produce message to Kafka (asynchronous)."""
        producer = await self.get_async_producer()
        topic = self._get_topic()
        value = self._serialize_avro()
        key = self._get_message_key()

        try:
            # Check if it's a real AIOProducer or fallback sync
            if hasattr(producer, "produce_async"):
                await producer.produce_async(topic=topic, value=value, key=key)
            else:
                # Fallback to sync producer
                producer.produce(topic=topic, value=value, key=key)
                producer.poll(0)
        except Exception as e:
            raise ProducerError(f"Failed to produce message: {e}") from e

    @classmethod
    async def aproduce_many(cls, messages: list[FlowBaseModel]) -> int:
        """Produce multiple messages asynchronously."""
        count = 0
        for msg in messages:
            await msg.aproduce()
            count += 1
        return count

    # ==================== Consume Operations (Sync) ====================

    @classmethod
    def consume_one(
        cls: type[T],
        timeout: float = 1.0,
        group_id: str | None = None,
        settings: BaseSettings | None = None,
    ) -> T | None:
        """
        Consume single message (synchronous).

        Args:
            timeout: Poll timeout in seconds
            group_id: Consumer group ID (uses Settings.consumer_group if not specified)
            settings: Optional settings profile

        Returns:
            Model instance or None if no message available
        """
        consumer = cls.get_consumer(group_id, settings)
        msg = consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            return None

        try:
            value = msg.value()
            if value is None:
                return None
            instance = cls._deserialize_avro(value)
            consumer.commit(msg)
            return instance
        except Exception:
            return None

    @classmethod
    def consume_iter(
        cls: type[T],
        timeout: float = 1.0,
        group_id: str | None = None,
        settings: BaseSettings | None = None,
    ) -> Iterator[T]:
        """
        Iterate over messages (synchronous generator).

        Args:
            timeout: Poll timeout in seconds
            group_id: Consumer group ID
            settings: Optional settings profile

        Yields:
            Model instances
        """
        consumer = cls.get_consumer(group_id, settings)

        while True:
            msg = consumer.poll(timeout)

            if msg is None:
                continue

            if msg.error():
                continue

            try:
                value = msg.value()
                if value is None:
                    continue
                instance = cls._deserialize_avro(value)
                yield instance
                consumer.commit(msg)
            except Exception:
                continue

    @classmethod
    def consume_batch(
        cls: type[T],
        max_messages: int,
        timeout: float = 1.0,
        group_id: str | None = None,
        settings: BaseSettings | None = None,
    ) -> list[T]:
        """
        Consume batch of messages.

        Args:
            max_messages: Maximum number of messages to consume
            timeout: Poll timeout in seconds
            group_id: Consumer group ID
            settings: Optional settings profile

        Returns:
            List of model instances
        """
        results: list[T] = []
        consumer = cls.get_consumer(group_id, settings)

        while len(results) < max_messages:
            msg = consumer.poll(timeout)

            if msg is None:
                break

            if msg.error():
                continue

            try:
                value = msg.value()
                if value is None:
                    continue
                instance = cls._deserialize_avro(value)
                results.append(instance)
                consumer.commit(msg)
            except Exception:
                continue

        return results

    # ==================== Consume Operations (Async) ====================

    @classmethod
    async def aconsume_one(
        cls: type[T],
        timeout: float = 1.0,
        group_id: str | None = None,
        settings: BaseSettings | None = None,
    ) -> T | None:
        """Consume single message (asynchronous)."""
        consumer = await cls.get_async_consumer(group_id, settings)

        # Check if it's a real AIOConsumer or fallback sync
        if hasattr(consumer, "poll_async"):
            msg = await consumer.poll_async(timeout)
        else:
            msg = consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            return None

        try:
            value = msg.value()
            if value is None:
                return None
            instance = cls._deserialize_avro(value)
            consumer.commit(msg)
            return instance
        except Exception:
            return None

    @classmethod
    async def aconsume_iter(
        cls: type[T],
        timeout: float = 1.0,
        group_id: str | None = None,
        settings: BaseSettings | None = None,
    ) -> AsyncIterator[T]:
        """Iterate over messages (async generator)."""
        consumer = await cls.get_async_consumer(group_id, settings)

        while True:
            # Check if it's a real AIOConsumer or fallback sync
            if hasattr(consumer, "poll_async"):
                msg = await consumer.poll_async(timeout)
            else:
                msg = consumer.poll(timeout)

            if msg is None:
                continue

            if msg.error():
                continue

            try:
                value = msg.value()
                if value is None:
                    continue
                instance = cls._deserialize_avro(value)
                yield instance
                consumer.commit(msg)
            except Exception:
                continue

    @classmethod
    async def aconsume_batch(
        cls: type[T],
        max_messages: int,
        timeout: float = 1.0,
        group_id: str | None = None,
        settings: BaseSettings | None = None,
    ) -> list[T]:
        """Consume batch of messages asynchronously."""
        results: list[T] = []
        consumer = await cls.get_async_consumer(group_id, settings)

        while len(results) < max_messages:
            if hasattr(consumer, "poll_async"):
                msg = await consumer.poll_async(timeout)
            else:
                msg = consumer.poll(timeout)

            if msg is None:
                break

            if msg.error():
                continue

            try:
                value = msg.value()
                if value is None:
                    continue
                instance = cls._deserialize_avro(value)
                results.append(instance)
                consumer.commit(msg)
            except Exception:
                continue

        return results

    # ==================== Schema Operations ====================

    @classmethod
    def register_schema(cls) -> int:
        """
        Register Avro schema with Schema Registry.

        Returns:
            Schema ID from registry
        """
        from confluent_kafka.schema_registry import Schema

        registry = get_schema_registry()
        subject = cls._get_schema_subject()
        schema = cls._generate_avro_schema()

        import json

        avro_schema = Schema(json.dumps(schema), "AVRO")
        return registry.register_schema(subject, avro_schema)
