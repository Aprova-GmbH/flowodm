Changelog
=========

All notable changes to FlowODM will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[0.1.0] - 2025-01-27
--------------------

Initial release.

Added
^^^^^

* **FlowBaseModel**: Pydantic v2 base class for Kafka messages
* **Connection Management**: Thread-safe singleton for Kafka and Schema Registry
* **Sync/Async Support**: Both synchronous and asynchronous APIs
* **Schema Registry Integration**: Automatic schema registration and validation
* **Consumer Loop Patterns**: ConsumerLoop and AsyncConsumerLoop for microservices
* **Settings Profiles**: LongRunning, Batch, RealTime, HighThroughput, Reliable
* **CLI Tools**: validate, check-compatibility, upload-schema, list-subjects
* **Schema Utilities**: Generate models from Avro schemas, validate against registry
