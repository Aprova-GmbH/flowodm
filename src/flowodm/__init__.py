"""
FlowODM - A lightweight ODM for Apache Kafka with Avro schema support.

FlowODM provides a Pydantic-based interface for working with Kafka messages
and Avro schemas. It supports both synchronous and asynchronous operations.

Example:
    >>> from flowodm import FlowBaseModel, connect
    >>> from datetime import datetime
    >>>
    >>> # Connect to Kafka
    >>> connect(
    ...     bootstrap_servers="localhost:9092",
    ...     schema_registry_url="http://localhost:8081"
    ... )
    >>>
    >>> # Define a model
    >>> class UserEvent(FlowBaseModel):
    ...     class Settings:
    ...         topic = "user-events"
    ...         consumer_group = "my-service"
    ...
    ...     user_id: str
    ...     action: str
    ...     timestamp: datetime
    >>>
    >>> # Produce messages
    >>> event = UserEvent(user_id="123", action="login", timestamp=datetime.now())
    >>> event.produce()
    >>>
    >>> # Consume messages
    >>> for event in UserEvent.consume_iter():
    ...     print(f"User {event.user_id} performed {event.action}")

Apache Kafka is a registered trademark of the Apache Software Foundation.
"""

__version__ = "0.2.2"

# Connection management
from flowodm.connection import (
    KafkaConnection,
    connect,
    get_async_consumer,
    get_async_producer,
    get_consumer,
    get_kafka_connection,
    get_producer,
    get_schema_registry,
)

# Consumer loop patterns
from flowodm.consumer import (
    AsyncConsumerLoop,
    ConsumerLoop,
    async_consumer_loop,
    consumer_loop,
)

# Exceptions
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

# Core model
from flowodm.model import FlowBaseModel, generate_message_id

# Schema utilities
from flowodm.schema import (
    CompatibilityResult,
    ValidationResult,
    check_compatibility,
    delete_subject,
    generate_model_from_registry,
    generate_model_from_schema,
    get_schema_versions,
    list_subjects,
    load_schema_from_file,
    load_schema_from_registry,
    upload_schema,
    validate_against_file,
    validate_against_registry,
)

# Settings profiles
from flowodm.settings import (
    BaseSettings,
    BatchSettings,
    HighThroughputSettings,
    LongRunningSettings,
    RealTimeSettings,
    ReliableSettings,
)

__all__ = [
    # Version
    "__version__",
    # Connection
    "KafkaConnection",
    "connect",
    "get_kafka_connection",
    "get_producer",
    "get_consumer",
    "get_async_producer",
    "get_async_consumer",
    "get_schema_registry",
    # Model
    "FlowBaseModel",
    "generate_message_id",
    # Consumer loops
    "ConsumerLoop",
    "AsyncConsumerLoop",
    "consumer_loop",
    "async_consumer_loop",
    # Settings
    "BaseSettings",
    "LongRunningSettings",
    "BatchSettings",
    "RealTimeSettings",
    "HighThroughputSettings",
    "ReliableSettings",
    # Schema
    "ValidationResult",
    "CompatibilityResult",
    "load_schema_from_file",
    "load_schema_from_registry",
    "generate_model_from_schema",
    "generate_model_from_registry",
    "validate_against_file",
    "validate_against_registry",
    "check_compatibility",
    "upload_schema",
    "list_subjects",
    "get_schema_versions",
    "delete_subject",
    # Exceptions
    "FlowODMError",
    "ConnectionError",
    "ConfigurationError",
    "SchemaError",
    "SchemaValidationError",
    "SchemaRegistryError",
    "SchemaCompatibilityError",
    "SerializationError",
    "DeserializationError",
    "ProducerError",
    "ConsumerError",
    "TopicError",
    "SettingsError",
]
