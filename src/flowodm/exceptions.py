"""Custom exceptions for FlowODM."""


class FlowODMError(Exception):
    """Base exception for all FlowODM errors."""

    pass


class ConnectionError(FlowODMError):
    """Raised when connection to Kafka or Schema Registry fails."""

    pass


class ConfigurationError(FlowODMError):
    """Raised when configuration is invalid or missing."""

    pass


class SchemaError(FlowODMError):
    """Base exception for schema-related errors."""

    pass


class SchemaValidationError(SchemaError):
    """Raised when schema validation fails."""

    def __init__(self, message: str, errors: list[str] | None = None):
        super().__init__(message)
        self.errors = errors or []


class SchemaRegistryError(SchemaError):
    """Raised when Schema Registry operations fail."""

    pass


class SchemaCompatibilityError(SchemaError):
    """Raised when schema compatibility check fails."""

    def __init__(self, message: str, compatibility_level: str | None = None):
        super().__init__(message)
        self.compatibility_level = compatibility_level


class SerializationError(FlowODMError):
    """Raised when message serialization fails."""

    pass


class DeserializationError(FlowODMError):
    """Raised when message deserialization fails."""

    pass


class ProducerError(FlowODMError):
    """Raised when message production fails."""

    pass


class ConsumerError(FlowODMError):
    """Raised when message consumption fails."""

    pass


class TopicError(FlowODMError):
    """Raised when topic-related operations fail."""

    pass


class SettingsError(FlowODMError):
    """Raised when model Settings class is invalid or missing."""

    pass
