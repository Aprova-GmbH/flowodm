"""Unit tests for settings profiles."""

import pytest

from flowodm.settings import (
    BaseSettings,
    BatchSettings,
    HighThroughputSettings,
    LongRunningSettings,
    RealTimeSettings,
    ReliableSettings,
)


@pytest.mark.unit
class TestBaseSettings:
    """Tests for BaseSettings class."""

    def test_default_values(self):
        """Test BaseSettings default values."""
        settings = BaseSettings()

        assert settings.session_timeout_ms == 45000
        assert settings.heartbeat_interval_ms == 15000
        assert settings.max_poll_interval_ms == 300000
        assert settings.max_poll_records == 500
        assert settings.auto_offset_reset == "earliest"
        assert settings.enable_auto_commit is False
        assert settings.extra_config == {}

    def test_to_consumer_config(self):
        """Test converting to consumer config."""
        settings = BaseSettings()
        config = settings.to_consumer_config()

        assert config["session.timeout.ms"] == 45000
        assert config["heartbeat.interval.ms"] == 15000
        assert config["max.poll.interval.ms"] == 300000
        assert config["auto.offset.reset"] == "earliest"
        assert config["enable.auto.commit"] is False

    def test_to_consumer_config_with_extra(self):
        """Test consumer config with extra configuration."""
        settings = BaseSettings(extra_config={"fetch.min.bytes": 1024})
        config = settings.to_consumer_config()

        assert config["fetch.min.bytes"] == 1024
        assert config["session.timeout.ms"] == 45000

    def test_to_producer_config(self):
        """Test converting to producer config."""
        settings = BaseSettings()
        config = settings.to_producer_config()

        assert config["acks"] == "all"
        assert config["retries"] == 3
        assert config["retry.backoff.ms"] == 100

    def test_to_producer_config_with_extra(self):
        """Test producer config with extra configuration."""
        settings = BaseSettings(extra_config={"compression.type": "gzip"})
        config = settings.to_producer_config()

        assert config["compression.type"] == "gzip"
        assert config["acks"] == "all"


@pytest.mark.unit
class TestLongRunningSettings:
    """Tests for LongRunningSettings profile."""

    def test_default_values(self):
        """Test LongRunningSettings default values."""
        settings = LongRunningSettings()

        assert settings.session_timeout_ms == 300000  # 5 minutes
        assert settings.heartbeat_interval_ms == 60000  # 1 minute
        assert settings.max_poll_interval_ms == 600000  # 10 minutes
        assert settings.max_poll_records == 100
        assert settings.auto_offset_reset == "earliest"
        assert settings.enable_auto_commit is False

    def test_to_consumer_config(self):
        """Test LongRunningSettings consumer config."""
        settings = LongRunningSettings()
        config = settings.to_consumer_config()

        assert config["session.timeout.ms"] == 300000
        assert config["heartbeat.interval.ms"] == 60000
        assert config["max.poll.interval.ms"] == 600000

    def test_to_producer_config(self):
        """Test LongRunningSettings producer config."""
        settings = LongRunningSettings()
        config = settings.to_producer_config()

        assert config["acks"] == "all"
        assert config["retries"] == 3


@pytest.mark.unit
class TestBatchSettings:
    """Tests for BatchSettings profile."""

    def test_default_values(self):
        """Test BatchSettings default values."""
        settings = BatchSettings()

        assert settings.session_timeout_ms == 45000
        assert settings.heartbeat_interval_ms == 15000
        assert settings.max_poll_interval_ms == 300000
        assert settings.max_poll_records == 500  # Large batch size
        assert settings.auto_offset_reset == "earliest"
        assert settings.enable_auto_commit is False

    def test_to_consumer_config(self):
        """Test BatchSettings consumer config."""
        settings = BatchSettings()
        config = settings.to_consumer_config()

        assert config["session.timeout.ms"] == 45000
        assert config["enable.auto.commit"] is False


@pytest.mark.unit
class TestRealTimeSettings:
    """Tests for RealTimeSettings profile."""

    def test_default_values(self):
        """Test RealTimeSettings default values."""
        settings = RealTimeSettings()

        assert settings.session_timeout_ms == 10000  # Short timeout
        assert settings.heartbeat_interval_ms == 3000  # Frequent heartbeat
        assert settings.max_poll_interval_ms == 30000  # Short interval
        assert settings.max_poll_records == 10  # Small batch
        assert settings.auto_offset_reset == "latest"  # Real-time
        assert settings.enable_auto_commit is True

    def test_to_consumer_config(self):
        """Test RealTimeSettings consumer config."""
        settings = RealTimeSettings()
        config = settings.to_consumer_config()

        assert config["session.timeout.ms"] == 10000
        assert config["heartbeat.interval.ms"] == 3000
        assert config["max.poll.interval.ms"] == 30000
        assert config["auto.offset.reset"] == "latest"
        assert config["enable.auto.commit"] is True


@pytest.mark.unit
class TestHighThroughputSettings:
    """Tests for HighThroughputSettings profile."""

    def test_default_values(self):
        """Test HighThroughputSettings default values."""
        settings = HighThroughputSettings()

        assert settings.session_timeout_ms == 45000
        assert settings.heartbeat_interval_ms == 15000
        assert settings.max_poll_interval_ms == 300000
        assert settings.max_poll_records == 1000  # Maximum batch size
        assert settings.auto_offset_reset == "latest"
        assert settings.enable_auto_commit is True

    def test_to_consumer_config(self):
        """Test HighThroughputSettings consumer config."""
        settings = HighThroughputSettings()
        config = settings.to_consumer_config()

        assert config["session.timeout.ms"] == 45000
        assert config["enable.auto.commit"] is True

    def test_to_producer_config(self):
        """Test HighThroughputSettings producer config optimized for throughput."""
        settings = HighThroughputSettings()
        config = settings.to_producer_config()

        assert config["acks"] == "1"  # Only leader ack
        assert config["retries"] == 0  # No retries
        assert config["batch.size"] == 65536  # 64KB
        assert config["linger.ms"] == 10
        assert config["compression.type"] == "lz4"


@pytest.mark.unit
class TestReliableSettings:
    """Tests for ReliableSettings profile."""

    def test_default_values(self):
        """Test ReliableSettings default values."""
        settings = ReliableSettings()

        assert settings.session_timeout_ms == 60000
        assert settings.heartbeat_interval_ms == 20000
        assert settings.max_poll_interval_ms == 300000
        assert settings.max_poll_records == 100
        assert settings.auto_offset_reset == "earliest"
        assert settings.enable_auto_commit is False

    def test_to_consumer_config(self):
        """Test ReliableSettings consumer config."""
        settings = ReliableSettings()
        config = settings.to_consumer_config()

        assert config["session.timeout.ms"] == 60000
        assert config["heartbeat.interval.ms"] == 20000
        assert config["enable.auto.commit"] is False

    def test_to_producer_config(self):
        """Test ReliableSettings producer config optimized for reliability."""
        settings = ReliableSettings()
        config = settings.to_producer_config()

        assert config["acks"] == "all"  # All replicas
        assert config["retries"] == 10  # Many retries
        assert config["retry.backoff.ms"] == 500
        assert config["max.in.flight.requests.per.connection"] == 1  # Preserve order
        assert config["enable.idempotence"] is True  # Exactly-once


@pytest.mark.unit
class TestSettingsCustomization:
    """Tests for customizing settings."""

    def test_custom_values(self):
        """Test creating settings with custom values."""
        settings = BaseSettings(
            session_timeout_ms=30000,
            max_poll_records=200,
            auto_offset_reset="latest",
        )

        assert settings.session_timeout_ms == 30000
        assert settings.max_poll_records == 200
        assert settings.auto_offset_reset == "latest"

    def test_extra_config_override(self):
        """Test that extra_config can override default values."""
        settings = BaseSettings(extra_config={"acks": "1"})
        producer_config = settings.to_producer_config()

        # Extra config should override
        assert producer_config["acks"] == "1"

    def test_chaining_settings(self):
        """Test that settings can be modified and converted."""
        settings = RealTimeSettings(extra_config={"fetch.max.wait.ms": 100})

        consumer_config = settings.to_consumer_config()
        producer_config = settings.to_producer_config()

        assert consumer_config["fetch.max.wait.ms"] == 100
        assert consumer_config["auto.offset.reset"] == "latest"
        assert "acks" in producer_config
