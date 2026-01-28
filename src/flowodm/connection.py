"""
Thread-safe singleton for Kafka and Schema Registry connection management.

Provides both synchronous (confluent-kafka) and asynchronous client access.
"""

from __future__ import annotations

import atexit
import os
import threading
from typing import Any

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient

from flowodm.exceptions import ConfigurationError, ConnectionError
from flowodm.settings import BaseSettings

# Module-level connection instance
_kafka_conn: KafkaConnection | None = None


class KafkaConnection:
    """
    Thread-safe singleton managing Kafka producers, consumers, and Schema Registry.

    Configuration via environment variables:
        - KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (required)
        - KAFKA_SECURITY_PROTOCOL: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
        - KAFKA_SASL_MECHANISM: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
        - KAFKA_SASL_USERNAME: SASL username
        - KAFKA_SASL_PASSWORD: SASL password
        - SCHEMA_REGISTRY_URL: Schema Registry URL
        - SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO: Basic auth in format "key:secret" (alternative to separate key/secret)
        - SCHEMA_REGISTRY_API_KEY: Confluent Cloud API key
        - SCHEMA_REGISTRY_API_SECRET: Confluent Cloud API secret

    Example:
        >>> from flowodm.connection import connect, get_producer
        >>> connect(bootstrap_servers="localhost:9092")
        >>> producer = get_producer()
    """

    _instance: KafkaConnection | None = None
    _lock = threading.Lock()

    # Connection state
    _bootstrap_servers: str | None = None
    _security_protocol: str = "PLAINTEXT"
    _sasl_mechanism: str | None = None
    _sasl_username: str | None = None
    _sasl_password: str | None = None
    _schema_registry_url: str | None = None
    _schema_registry_api_key: str | None = None
    _schema_registry_api_secret: str | None = None
    _schema_registry_basic_auth_user_info: str | None = None

    # Sync clients
    _producer: Producer | None = None
    _consumers: dict[str, Consumer]

    # Schema Registry
    _schema_registry: SchemaRegistryClient | None = None

    # Async clients (lazy initialization)
    # Note: AIOProducer/AIOConsumer available in confluent-kafka >= 2.13.0
    _async_producer: Any | None = None
    _async_consumers: dict[str, Any]

    def __new__(cls) -> KafkaConnection:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._consumers = {}
                    instance._async_consumers = {}
                    cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        # Only initialize once
        if self._bootstrap_servers is None:
            self._load_config_from_env()

    def _load_config_from_env(self) -> None:
        """Load configuration from environment variables."""
        self._bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        self._security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        self._sasl_mechanism = os.environ.get("KAFKA_SASL_MECHANISM")
        self._sasl_username = os.environ.get("KAFKA_SASL_USERNAME")
        self._sasl_password = os.environ.get("KAFKA_SASL_PASSWORD")
        self._schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL")
        self._schema_registry_api_key = os.environ.get("SCHEMA_REGISTRY_API_KEY")
        self._schema_registry_api_secret = os.environ.get("SCHEMA_REGISTRY_API_SECRET")
        self._schema_registry_basic_auth_user_info = os.environ.get("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")

    def configure(
        self,
        bootstrap_servers: str | None = None,
        security_protocol: str | None = None,
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        schema_registry_url: str | None = None,
        schema_registry_api_key: str | None = None,
        schema_registry_api_secret: str | None = None,
        schema_registry_basic_auth_user_info: str | None = None,
    ) -> None:
        """
        Configure the connection with explicit parameters.

        Parameters override environment variables.
        """
        if bootstrap_servers:
            self._bootstrap_servers = bootstrap_servers
        if security_protocol:
            self._security_protocol = security_protocol
        if sasl_mechanism:
            self._sasl_mechanism = sasl_mechanism
        if sasl_username:
            self._sasl_username = sasl_username
        if sasl_password:
            self._sasl_password = sasl_password
        if schema_registry_url:
            self._schema_registry_url = schema_registry_url
        if schema_registry_api_key:
            self._schema_registry_api_key = schema_registry_api_key
        if schema_registry_api_secret:
            self._schema_registry_api_secret = schema_registry_api_secret
        if schema_registry_basic_auth_user_info:
            self._schema_registry_basic_auth_user_info = schema_registry_basic_auth_user_info

        # Register cleanup handler
        atexit.register(self.close_connection)

    def _get_base_kafka_config(self) -> dict[str, Any]:
        """Get base Kafka configuration dict."""
        if not self._bootstrap_servers:
            raise ConfigurationError(
                "KAFKA_BOOTSTRAP_SERVERS not configured. "
                "Set environment variable or call connect() with bootstrap_servers."
            )

        config: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap_servers,
            "security.protocol": self._security_protocol,
        }

        if self._sasl_mechanism:
            config["sasl.mechanism"] = self._sasl_mechanism
            if self._sasl_username:
                config["sasl.username"] = self._sasl_username
            if self._sasl_password:
                config["sasl.password"] = self._sasl_password

        return config

    @property
    def producer(self) -> Producer:
        """
        Get synchronous Kafka producer.

        Creates producer on first access (lazy initialization).
        """
        if self._producer is None:
            config = self._get_base_kafka_config()
            config.update(
                {
                    "acks": "all",
                    "retries": 3,
                    "retry.backoff.ms": 100,
                }
            )
            try:
                self._producer = Producer(config)
            except Exception as e:
                raise ConnectionError(f"Failed to create Kafka producer: {e}") from e
        return self._producer

    def get_consumer(
        self,
        group_id: str,
        topics: list[str],
        settings: BaseSettings | None = None,
    ) -> Consumer:
        """
        Get or create a Kafka consumer for the given group and topics.

        Args:
            group_id: Consumer group ID
            topics: List of topics to subscribe to
            settings: Optional settings profile for consumer configuration

        Returns:
            Configured Consumer instance
        """
        # Create unique key for this consumer configuration
        consumer_key = f"{group_id}:{','.join(sorted(topics))}"

        if consumer_key not in self._consumers:
            config = self._get_base_kafka_config()
            config["group.id"] = group_id

            # Apply settings if provided
            if settings:
                config.update(settings.to_consumer_config())
            else:
                # Default settings
                config.update(
                    {
                        "auto.offset.reset": "earliest",
                        "enable.auto.commit": False,
                    }
                )

            try:
                consumer = Consumer(config)
                consumer.subscribe(topics)
                self._consumers[consumer_key] = consumer
            except Exception as e:
                raise ConnectionError(f"Failed to create Kafka consumer: {e}") from e

        return self._consumers[consumer_key]

    @property
    def schema_registry(self) -> SchemaRegistryClient:
        """
        Get Schema Registry client.

        Creates client on first access (lazy initialization).
        """
        if self._schema_registry is None:
            if not self._schema_registry_url:
                raise ConfigurationError(
                    "SCHEMA_REGISTRY_URL not configured. "
                    "Set environment variable or call connect() with schema_registry_url."
                )

            config: dict[str, Any] = {"url": self._schema_registry_url}

            # Add authentication if provided
            if self._schema_registry_basic_auth_user_info:
                # Use combined format directly (highest priority)
                config["basic.auth.user.info"] = self._schema_registry_basic_auth_user_info
            elif self._schema_registry_api_key and self._schema_registry_api_secret:
                # Fall back to separate key/secret format
                config["basic.auth.user.info"] = (
                    f"{self._schema_registry_api_key}:{self._schema_registry_api_secret}"
                )

            try:
                self._schema_registry = SchemaRegistryClient(config)
            except Exception as e:
                raise ConnectionError(f"Failed to create Schema Registry client: {e}") from e

        return self._schema_registry

    async def get_async_producer(self) -> Any:
        """
        Get asynchronous Kafka producer.

        Note: Requires confluent-kafka >= 2.13.0 for AIOProducer support.
        Falls back to sync producer wrapped in asyncio if not available.
        """
        if self._async_producer is None:
            try:
                from confluent_kafka import AIOProducer  # type: ignore[attr-defined]

                config = self._get_base_kafka_config()
                config.update(
                    {
                        "acks": "all",
                        "retries": 3,
                        "retry.backoff.ms": 100,
                    }
                )
                self._async_producer = AIOProducer(config)
            except ImportError:
                # Fall back to sync producer for older versions
                self._async_producer = self.producer
        return self._async_producer

    async def get_async_consumer(
        self,
        group_id: str,
        topics: list[str],
        settings: BaseSettings | None = None,
    ) -> Any:
        """
        Get asynchronous Kafka consumer.

        Note: Requires confluent-kafka >= 2.13.0 for AIOConsumer support.
        Falls back to sync consumer for older versions.
        """
        consumer_key = f"async:{group_id}:{','.join(sorted(topics))}"

        if consumer_key not in self._async_consumers:
            try:
                from confluent_kafka import AIOConsumer  # type: ignore[attr-defined]

                config = self._get_base_kafka_config()
                config["group.id"] = group_id

                if settings:
                    config.update(settings.to_consumer_config())
                else:
                    config.update(
                        {
                            "auto.offset.reset": "earliest",
                            "enable.auto.commit": False,
                        }
                    )

                consumer = AIOConsumer(config)
                consumer.subscribe(topics)
                self._async_consumers[consumer_key] = consumer
            except ImportError:
                # Fall back to sync consumer for older versions
                self._async_consumers[consumer_key] = self.get_consumer(group_id, topics, settings)

        return self._async_consumers[consumer_key]

    def close_connection(self) -> None:
        """Close all Kafka clients and cleanup resources."""
        # Close sync producer
        if self._producer is not None:
            self._producer.flush(timeout=10)
            self._producer = None

        # Close sync consumers
        for consumer in self._consumers.values():
            consumer.close()
        self._consumers.clear()

        # Close async producer if it's a real AIOProducer
        if self._async_producer is not None and self._async_producer is not self._producer:
            try:
                self._async_producer.flush(timeout=10)
            except Exception:
                pass
            self._async_producer = None

        # Close async consumers
        for consumer in self._async_consumers.values():
            try:
                consumer.close()
            except Exception:
                pass
        self._async_consumers.clear()

        # Schema Registry client doesn't need explicit closing
        self._schema_registry = None


# Module-level helper functions


def get_kafka_connection() -> KafkaConnection:
    """Get the global KafkaConnection singleton instance."""
    global _kafka_conn
    if _kafka_conn is None:
        _kafka_conn = KafkaConnection()
    return _kafka_conn


def connect(
    bootstrap_servers: str | None = None,
    security_protocol: str | None = None,
    sasl_mechanism: str | None = None,
    sasl_username: str | None = None,
    sasl_password: str | None = None,
    schema_registry_url: str | None = None,
    schema_registry_api_key: str | None = None,
    schema_registry_api_secret: str | None = None,
    schema_registry_basic_auth_user_info: str | None = None,
) -> KafkaConnection:
    """
    Configure and return the global Kafka connection.

    Args:
        bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        security_protocol: Security protocol (PLAINTEXT, SSL, SASL_SSL)
        sasl_mechanism: SASL mechanism (PLAIN, SCRAM-SHA-256, OAUTHBEARER)
        sasl_username: SASL username
        sasl_password: SASL password
        schema_registry_url: Schema Registry URL
        schema_registry_api_key: Schema Registry API key (Confluent Cloud)
        schema_registry_api_secret: Schema Registry API secret (Confluent Cloud)
        schema_registry_basic_auth_user_info: Schema Registry basic auth (format: "key:secret")

    Returns:
        Configured KafkaConnection instance

    Example:
        >>> connect(
        ...     bootstrap_servers="localhost:9092",
        ...     schema_registry_url="http://localhost:8081"
        ... )
    """
    conn = get_kafka_connection()
    conn.configure(
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        schema_registry_url=schema_registry_url,
        schema_registry_api_key=schema_registry_api_key,
        schema_registry_api_secret=schema_registry_api_secret,
        schema_registry_basic_auth_user_info=schema_registry_basic_auth_user_info,
    )
    return conn


def get_producer() -> Producer:
    """Get the global Kafka producer."""
    return get_kafka_connection().producer


def get_consumer(
    group_id: str,
    topics: list[str],
    settings: BaseSettings | None = None,
) -> Consumer:
    """Get or create a Kafka consumer."""
    return get_kafka_connection().get_consumer(group_id, topics, settings)


def get_schema_registry() -> SchemaRegistryClient:
    """Get the Schema Registry client."""
    return get_kafka_connection().schema_registry


async def get_async_producer() -> Any:
    """Get the async Kafka producer."""
    return await get_kafka_connection().get_async_producer()


async def get_async_consumer(
    group_id: str,
    topics: list[str],
    settings: BaseSettings | None = None,
) -> Any:
    """Get or create an async Kafka consumer."""
    return await get_kafka_connection().get_async_consumer(group_id, topics, settings)
