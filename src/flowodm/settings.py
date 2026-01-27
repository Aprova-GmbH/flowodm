"""Predefined Kafka settings profiles for different use cases."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BaseSettings:
    """Base settings class with common Kafka configuration options."""

    # Session management
    session_timeout_ms: int = 45000
    heartbeat_interval_ms: int = 15000
    max_poll_interval_ms: int = 300000

    # Polling
    max_poll_records: int = 500

    # Offset management
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False

    # Additional config overrides
    extra_config: dict[str, Any] = field(default_factory=dict)

    def to_consumer_config(self) -> dict[str, Any]:
        """Convert settings to confluent-kafka consumer configuration dict."""
        config = {
            "session.timeout.ms": self.session_timeout_ms,
            "heartbeat.interval.ms": self.heartbeat_interval_ms,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
        }
        config.update(self.extra_config)
        return config

    def to_producer_config(self) -> dict[str, Any]:
        """Convert settings to confluent-kafka producer configuration dict."""
        config = {
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
        }
        config.update(self.extra_config)
        return config


@dataclass
class LongRunningSettings(BaseSettings):
    """
    Settings optimized for long-running processing tasks.

    Use when processing a single message may take several minutes,
    such as ML inference, complex calculations, or external API calls.

    Timeouts are extended to prevent unnecessary consumer rebalances
    during long processing.
    """

    # 5 minutes session timeout
    session_timeout_ms: int = 300000

    # 1 minute heartbeat (should be ~1/3 of session timeout)
    heartbeat_interval_ms: int = 60000

    # 10 minutes max poll interval - allows very long processing
    max_poll_interval_ms: int = 600000

    # Smaller batch size for long-running tasks
    max_poll_records: int = 100

    # Start from earliest to not miss messages
    auto_offset_reset: str = "earliest"

    # Manual commit for at-least-once delivery
    enable_auto_commit: bool = False


@dataclass
class BatchSettings(BaseSettings):
    """
    Settings optimized for batch processing.

    Use for ETL jobs, data aggregation, or bulk operations
    where throughput is more important than latency.
    """

    # Standard session timeout
    session_timeout_ms: int = 45000

    # Standard heartbeat
    heartbeat_interval_ms: int = 15000

    # 5 minutes max poll interval
    max_poll_interval_ms: int = 300000

    # Large batch size for throughput
    max_poll_records: int = 500

    # Start from earliest to process all data
    auto_offset_reset: str = "earliest"

    # Manual commit for at-least-once delivery
    enable_auto_commit: bool = False


@dataclass
class RealTimeSettings(BaseSettings):
    """
    Settings optimized for real-time processing.

    Use for event-driven microservices, notifications, or any
    scenario where low latency is critical.
    """

    # Short session timeout for quick rebalancing
    session_timeout_ms: int = 10000

    # Frequent heartbeat
    heartbeat_interval_ms: int = 3000

    # Short max poll interval
    max_poll_interval_ms: int = 30000

    # Small batch size for low latency
    max_poll_records: int = 10

    # Start from latest for real-time processing
    auto_offset_reset: str = "latest"

    # Auto commit for simplicity (at-most-once)
    enable_auto_commit: bool = True


@dataclass
class HighThroughputSettings(BaseSettings):
    """
    Settings optimized for maximum throughput.

    Use for high-volume data ingestion where some message loss
    is acceptable in exchange for performance.
    """

    # Standard session timeout
    session_timeout_ms: int = 45000

    # Standard heartbeat
    heartbeat_interval_ms: int = 15000

    # Extended max poll interval
    max_poll_interval_ms: int = 300000

    # Maximum batch size
    max_poll_records: int = 1000

    # Start from latest
    auto_offset_reset: str = "latest"

    # Auto commit for performance
    enable_auto_commit: bool = True

    def to_producer_config(self) -> dict[str, Any]:
        """Optimized producer config for throughput."""
        config = {
            "acks": "1",  # Only wait for leader acknowledgment
            "retries": 0,  # No retries for max throughput
            "batch.size": 65536,  # 64KB batch size
            "linger.ms": 10,  # Wait up to 10ms to batch messages
            "compression.type": "lz4",  # Fast compression
        }
        config.update(self.extra_config)
        return config


@dataclass
class ReliableSettings(BaseSettings):
    """
    Settings optimized for maximum reliability.

    Use when message delivery guarantees are critical,
    such as financial transactions or audit logs.
    """

    # Extended session timeout
    session_timeout_ms: int = 60000

    # Regular heartbeat
    heartbeat_interval_ms: int = 20000

    # Extended max poll interval
    max_poll_interval_ms: int = 300000

    # Moderate batch size
    max_poll_records: int = 100

    # Start from earliest to not miss messages
    auto_offset_reset: str = "earliest"

    # Manual commit for exactly-once processing
    enable_auto_commit: bool = False

    def to_producer_config(self) -> dict[str, Any]:
        """Optimized producer config for reliability."""
        config = {
            "acks": "all",  # Wait for all replicas
            "retries": 10,  # Retry on failure
            "retry.backoff.ms": 500,
            "max.in.flight.requests.per.connection": 1,  # Preserve order
            "enable.idempotence": True,  # Exactly-once semantics
        }
        config.update(self.extra_config)
        return config
