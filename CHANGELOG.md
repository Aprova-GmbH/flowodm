# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- UUID-based message ID generation with `generate_message_id()` function
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
