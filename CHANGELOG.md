# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- `_to_avro_dict()` now converts `Enum` values to their plain `.value` before passing to fastavro

### Removed
- **BREAKING**: Removed auto-generated `message_id` field from `FlowBaseModel`. Users who need an ID field should add one manually to their model with `default_factory`.
- Removed `generate_message_id()` function from public API

### Migration Guide
- If you relied on the automatic `message_id` field, add it to your model explicitly:
  ```python
  import uuid
  class MyModel(FlowBaseModel):
      id: str = Field(default_factory=lambda: str(uuid.uuid4()))
  ```

### Added
- Confluent wire format header (magic byte `0x00` + 4-byte schema ID) is now prepended to serialized Avro messages when a Schema Registry is configured ([#7](https://github.com/Aprova-GmbH/flowodm/issues/7), [#8](https://github.com/Aprova-GmbH/flowodm/pull/8))
- New `confluent_wire_format` setting in the `Settings` class (default `True`) to control wire format header behavior
- Schema ID caching per model class to avoid repeated Schema Registry calls during serialization

### Fixed
- Schema ID cache key now uses fully qualified class name (`module.qualname`) to prevent collisions between same-named classes in different modules

## [0.2.2] - 2026-02-05

### Changed
- **BREAKING**: Default `max_retries` changed from `3` to `0` in both `ConsumerLoop` and `AsyncConsumerLoop`
- Retry logic is now opt-in rather than opt-out
- Retry-related log messages are suppressed when `max_retries=0`
- Updated documentation to clarify opt-in retry configuration

### Migration Guide
- If you relied on the default retry behavior (3 retries), explicitly set `max_retries=3` in your consumer loop
- Old default: `ConsumerLoop(model=MyModel, handler=handler)` would retry 3 times
- New default: `ConsumerLoop(model=MyModel, handler=handler)` does not retry
- To restore old behavior: `ConsumerLoop(model=MyModel, handler=handler, max_retries=3)`

## [0.2.1] - 2026-02-04

### Added
- Deserialized message instance as third parameter to error handlers in both `ConsumerLoop` and `AsyncConsumerLoop` ([#5](https://github.com/Aprova-GmbH/flowodm/pull/5))
- Error handlers can now distinguish between deserialization failures (`deserialized=None`) and handler failures (`deserialized=FlowBaseModel`)
- Enhanced error handling examples in documentation and example code

### Changed
- **BREAKING**: Error handler signature changed from `(Exception, Any)` to `(Exception, Any, FlowBaseModel | None)`
- Updated `examples/microservice.py` to demonstrate new error handler pattern
- Enhanced documentation in `docs/consumer_loops.rst` with error handling scenarios

### Migration Guide
- Update error handler functions to accept a third parameter for the deserialized instance
- Old signature: `def error_handler(error: Exception, raw_message: Any) -> None`
- New signature: `def error_handler(error: Exception, raw_message: Any, deserialized: FlowBaseModel | None) -> None`

## [0.2.0] - 2026-02-03

### Added
- New `commit_strategy="before_processing"` to prevent duplicate processing in parallel pod deployments
- New `commit_strategy="after_processing"` as explicit name for traditional commit-after-processing behavior
- Commit retry logic with exponential backoff for transient failures
- Comprehensive documentation on commit strategies and delivery semantics in docs/consumer_loops.rst
- Strategy validation that raises ValueError for invalid commit_strategy values
- Windows compatibility for signal handlers in both `ConsumerLoop` and `AsyncConsumerLoop` ([#2](https://github.com/Aprova-GmbH/flowodm/pull/2))
- Platform detection for SIGTERM availability (Unix-like vs Windows)
- Fallback to `signal.signal()` for SIGINT on Windows when `add_signal_handler()` is not available
- Comprehensive unit tests for Windows and Unix signal handling compatibility

### Changed
- **BREAKING**: Removed `commit_strategy="per_message"` option (replaced by explicit `"before_processing"` and `"after_processing"`)
- **BREAKING**: Default `commit_strategy` is now `"before_processing"` to prevent duplicates in parallel deployments
- Updated examples and README to show recommended `before_processing` strategy for parallel deployments

### Fixed
- Signal handler setup now works correctly on Windows (uses only SIGINT when SIGTERM is unavailable)
- AsyncConsumerLoop gracefully falls back to `signal.signal()` when asyncio signal handlers are not supported

### Migration Guide
- **Default behavior changed**: New default is `commit_strategy="before_processing"` (prevents duplicates in parallel deployments)
- **Removed option**: `commit_strategy="per_message"` is no longer supported - use `"after_processing"` instead
- To restore old behavior, explicitly set `commit_strategy="after_processing"`
- For guaranteed delivery (no message loss), use `commit_strategy="after_processing"`
- Update handlers to be idempotent when using `before_processing` strategy (now the default)

## [0.1.1] - 2026-01-28

### Added
- Unit tests for connection management, exceptions, and settings modules
- Project logo to README and documentation

### Changed
- Updated repository URLs across project files
- Improved Kafka and Schema Registry configurations for multi-listener support in test workflow

## [0.1.0] - 2026-01-27

### Added
- Initial release of FlowODM
- `FlowBaseModel` base class with full produce/consume operations
- Support for both synchronous and asynchronous operations
- Thread-safe singleton connection manager (`KafkaConnection`)
- Automatic cleanup with atexit handlers
- Full type hints and py.typed marker
- Pydantic v2 integration with Avro serialization
- Comprehensive produce/consume methods:
  - Sync: `produce_sync`, `consume_one`, `consume_iter`
  - Async: `aproduce`, `aconsume_one`, `aconsume_iter`
- Consumer loop patterns for microservices:
  - `ConsumerLoop` for synchronous processing
  - `AsyncConsumerLoop` for concurrent async processing
- Schema Registry integration:
  - Schema validation against files and registry
  - Model generation from Avro schemas
  - Compatibility checking
  - Schema upload and management
- Settings class pattern for topic and consumer configuration
- Predefined settings profiles:
  - `LongRunningSettings` - For ML inference, complex processing
  - `BatchSettings` - For ETL jobs, data aggregation
  - `RealTimeSettings` - For event-driven, low-latency processing
  - `HighThroughputSettings` - For high-volume processing
  - `ReliableSettings` - For at-least-once delivery
- UUID-based message ID generation with `generate_message_id()` function (removed in later release)
- CLI tools for schema validation and Schema Registry operations
- Comprehensive exception hierarchy for error handling
- Graceful shutdown handling with signal support

### Features
- Zero dependencies beyond Pydantic, confluent-kafka, and fastavro
- Minimal, schema-first approach to Kafka messaging
- Full async/await support
- Connection pooling with configurable parameters
- Automatic field validation via Pydantic v2
- Type-safe generic methods
- Message key handling via `key_field` in Settings
- Support for extra fields with Pydantic configuration
- Comprehensive unit and integration test suite
- Sphinx documentation with ReadTheDocs integration
