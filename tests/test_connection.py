"""Unit tests for connection management."""

from unittest.mock import MagicMock, patch

import pytest

from flowodm.connection import (
    KafkaConnection,
    connect,
    get_consumer,
    get_producer,
    get_schema_registry,
)
from flowodm.exceptions import ConfigurationError
from flowodm.settings import BaseSettings


@pytest.mark.unit
class TestKafkaConnectionSingleton:
    """Tests for KafkaConnection singleton behavior."""

    def test_singleton_same_instance(self):
        """Test that KafkaConnection returns the same instance."""
        conn1 = KafkaConnection()
        conn2 = KafkaConnection()

        assert conn1 is conn2

    def test_load_config_from_env(self, monkeypatch):
        """Test loading configuration from environment variables."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "test-user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "test-pass")
        monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

        conn = KafkaConnection()
        conn._load_config_from_env()

        assert conn._bootstrap_servers == "localhost:9092"
        assert conn._security_protocol == "SASL_SSL"
        assert conn._sasl_mechanism == "PLAIN"
        assert conn._sasl_username == "test-user"
        assert conn._sasl_password == "test-pass"
        assert conn._schema_registry_url == "http://localhost:8081"

    def test_configure_explicit_parameters(self):
        """Test configuring connection with explicit parameters."""
        conn = KafkaConnection()
        conn.configure(
            bootstrap_servers="test-server:9092",
            security_protocol="SSL",
            schema_registry_url="http://test:8081",
        )

        assert conn._bootstrap_servers == "test-server:9092"
        assert conn._security_protocol == "SSL"
        assert conn._schema_registry_url == "http://test:8081"

    def test_configure_with_sasl(self):
        """Test configuring with SASL authentication."""
        conn = KafkaConnection()
        conn.configure(
            bootstrap_servers="test-server:9092",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="user",
            sasl_password="pass",
        )

        assert conn._sasl_mechanism == "SCRAM-SHA-256"
        assert conn._sasl_username == "user"
        assert conn._sasl_password == "pass"

    def test_configure_with_schema_registry_auth(self):
        """Test configuring with Schema Registry authentication."""
        conn = KafkaConnection()
        conn.configure(
            bootstrap_servers="test-server:9092",
            schema_registry_url="http://test:8081",
            schema_registry_api_key="key123",
            schema_registry_api_secret="secret456",
        )

        assert conn._schema_registry_api_key == "key123"
        assert conn._schema_registry_api_secret == "secret456"

    def test_configure_with_basic_auth_user_info(self):
        """Test configuring with basic auth user info."""
        conn = KafkaConnection()
        conn.configure(
            bootstrap_servers="test-server:9092",
            schema_registry_basic_auth_user_info="key:secret",
        )

        assert conn._schema_registry_basic_auth_user_info == "key:secret"


@pytest.mark.unit
class TestKafkaConfiguration:
    """Tests for Kafka configuration building."""

    def test_get_base_kafka_config(self):
        """Test getting base Kafka configuration."""
        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")

        config = conn._get_base_kafka_config()

        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["security.protocol"] == "PLAINTEXT"

    def test_get_base_kafka_config_no_bootstrap_raises_error(self):
        """Test error when bootstrap servers not configured."""
        conn = KafkaConnection()
        conn._bootstrap_servers = None

        with pytest.raises(ConfigurationError):
            conn._get_base_kafka_config()

    def test_get_base_kafka_config_with_sasl(self):
        """Test Kafka config with SASL authentication."""
        conn = KafkaConnection()
        conn.configure(
            bootstrap_servers="localhost:9092",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",
        )

        config = conn._get_base_kafka_config()

        assert config["sasl.mechanism"] == "PLAIN"
        assert config["sasl.username"] == "user"
        assert config["sasl.password"] == "pass"


@pytest.mark.unit
class TestProducerCreation:
    """Tests for producer creation."""

    @patch("flowodm.connection.Producer")
    def test_producer_property(self, mock_producer_class):
        """Test getting producer via property."""
        mock_instance = MagicMock()
        mock_producer_class.return_value = mock_instance

        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")

        producer = conn.producer

        assert producer is mock_instance
        mock_producer_class.assert_called_once()

    @patch("flowodm.connection.Producer")
    def test_producer_reuses_instance(self, mock_producer_class):
        """Test that producer property reuses the same instance."""
        mock_instance = MagicMock()
        mock_producer_class.return_value = mock_instance

        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")

        producer1 = conn.producer
        producer2 = conn.producer

        assert producer1 is producer2
        # Should only be called once due to caching
        assert mock_producer_class.call_count == 1


@pytest.mark.unit
class TestConsumerCreation:
    """Tests for consumer creation."""

    @patch("flowodm.connection.Consumer")
    def test_get_consumer(self, mock_consumer_class):
        """Test getting consumer."""
        mock_instance = MagicMock()
        mock_consumer_class.return_value = mock_instance

        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")

        consumer = conn.get_consumer("test-group", ["test-topic"])

        assert consumer is mock_instance
        mock_consumer_class.assert_called_once()
        # Verify consumer was subscribed to topic
        mock_instance.subscribe.assert_called_once_with(["test-topic"])

    @patch("flowodm.connection.Consumer")
    def test_get_consumer_with_settings(self, mock_consumer_class):
        """Test getting consumer with custom settings."""
        mock_instance = MagicMock()
        mock_consumer_class.return_value = mock_instance

        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")

        settings = BaseSettings(max_poll_records=100)
        consumer = conn.get_consumer("test-group", ["test-topic"], settings)

        assert consumer is mock_instance
        # Verify settings were applied
        call_args = mock_consumer_class.call_args
        assert "max.poll.interval.ms" in call_args[0][0]

    @patch("flowodm.connection.Consumer")
    def test_get_consumer_reuses_for_same_group(self, mock_consumer_class):
        """Test that get_consumer reuses instance for same group."""
        mock_instance = MagicMock()
        mock_consumer_class.return_value = mock_instance

        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")

        consumer1 = conn.get_consumer("test-group", ["test-topic"])
        consumer2 = conn.get_consumer("test-group", ["test-topic"])

        assert consumer1 is consumer2
        assert mock_consumer_class.call_count == 1


@pytest.mark.unit
class TestSchemaRegistry:
    """Tests for Schema Registry client."""

    @patch("flowodm.connection.SchemaRegistryClient")
    def test_schema_registry_property(self, mock_sr_class):
        """Test getting Schema Registry client via property."""
        mock_instance = MagicMock()
        mock_sr_class.return_value = mock_instance

        conn = KafkaConnection()
        conn.configure(
            bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
        )

        registry = conn.schema_registry

        assert registry is mock_instance
        mock_sr_class.assert_called_once()

    @patch("flowodm.connection.SchemaRegistryClient")
    def test_schema_registry_with_api_key(self, mock_sr_class):
        """Test Schema Registry with API key authentication."""
        mock_instance = MagicMock()
        mock_sr_class.return_value = mock_instance

        conn = KafkaConnection()
        conn.configure(
            bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
            schema_registry_api_key="key",
            schema_registry_api_secret="secret",
        )

        registry = conn.schema_registry

        assert registry is mock_instance
        # Verify auth config was included
        call_args = mock_sr_class.call_args[0][0]
        assert "basic.auth.user.info" in call_args
        assert call_args["basic.auth.user.info"] == "key:secret"

    @patch("flowodm.connection.SchemaRegistryClient")
    def test_schema_registry_with_basic_auth(self, mock_sr_class):
        """Test Schema Registry with basic auth user info."""
        mock_instance = MagicMock()
        mock_sr_class.return_value = mock_instance

        conn = KafkaConnection()
        conn.configure(
            bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
            schema_registry_basic_auth_user_info="key:secret",
        )

        registry = conn.schema_registry

        assert registry is mock_instance
        call_args = mock_sr_class.call_args[0][0]
        assert call_args["basic.auth.user.info"] == "key:secret"

    def test_schema_registry_no_url_raises_error(self):
        """Test error when Schema Registry URL not configured."""
        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")
        conn._schema_registry_url = None

        with pytest.raises(ConfigurationError):
            _ = conn.schema_registry


@pytest.mark.unit
class TestHelperFunctions:
    """Tests for module-level helper functions."""

    def test_connect_function(self):
        """Test connect() helper function."""
        connect(bootstrap_servers="test-server:9092")

        conn = KafkaConnection()
        assert conn._bootstrap_servers == "test-server:9092"

    @patch("flowodm.connection.Producer")
    def test_get_producer_function(self, mock_producer_class):
        """Test get_producer() helper function."""
        mock_instance = MagicMock()
        mock_producer_class.return_value = mock_instance

        connect(bootstrap_servers="localhost:9092")
        producer = get_producer()

        assert producer is mock_instance

    @patch("flowodm.connection.Consumer")
    def test_get_consumer_function(self, mock_consumer_class):
        """Test get_consumer() helper function."""
        mock_instance = MagicMock()
        mock_consumer_class.return_value = mock_instance

        connect(bootstrap_servers="localhost:9092")
        consumer = get_consumer("test-group", ["test-topic"])

        assert consumer is mock_instance

    @patch("flowodm.connection.SchemaRegistryClient")
    def test_get_schema_registry_function(self, mock_sr_class):
        """Test get_schema_registry() helper function."""
        mock_instance = MagicMock()
        mock_sr_class.return_value = mock_instance

        connect(
            bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
        )
        registry = get_schema_registry()

        assert registry is mock_instance


@pytest.mark.unit
class TestConnectionCleanup:
    """Tests for connection cleanup."""

    @patch("flowodm.connection.Producer")
    def test_close_connection(self, mock_producer_class):
        """Test close_connection cleans up resources."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")
        _ = conn.producer

        conn.close_connection()

        # Verify producer was flushed
        mock_producer.flush.assert_called_once()

    @patch("flowodm.connection.Consumer")
    def test_close_connection_with_consumers(self, mock_consumer_class):
        """Test close_connection closes all consumers."""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        conn = KafkaConnection()
        conn.configure(bootstrap_servers="localhost:9092")
        conn.get_consumer("test-group", ["test-topic"])

        conn.close_connection()

        # Verify consumer was closed
        mock_consumer.close.assert_called_once()
