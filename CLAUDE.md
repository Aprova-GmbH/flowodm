# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FlowODM is a lightweight ODM (Object-Document Mapper) for Apache Kafka with Avro schema support. Built on Pydantic v2, confluent-kafka, and fastavro, it provides a minimal, schema-first approach to Kafka messaging with full sync/async support. The entire codebase is ~2000 lines of production code across 8 core modules.

## Development Commands

### Environment Setup
```bash
# Install uv (recommended package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies with development tools
uv pip install -e ".[dev]"

# Or using pip
pip install -e ".[dev]"
```

### Testing
```bash
# Run fast unit tests only (no Kafka needed) - DEFAULT for local dev
uv run pytest -m unit

# Run integration tests (REQUIRES Kafka via Docker - see setup below)
uv run pytest -m integration

# Run all tests (unit + integration - requires Kafka)
uv run pytest

# Run with coverage
uv run pytest --cov=flowodm --cov-report=term-missing

# Generate HTML coverage report
uv run pytest --cov=flowodm --cov-report=html
```

### Code Quality
```bash
# Format code (line length: 100)
uv run black src tests

# Lint code
uv run ruff check src tests

# Auto-fix linting issues
uv run ruff check --fix src tests

# Type checking
uv run mypy src
```

### Kafka Setup for Integration Testing
```bash
# Start Kafka using Docker Compose
docker-compose -f docker-compose.test.yml up -d

# Set environment variables for integration tests
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SCHEMA_REGISTRY_URL="http://localhost:8081"

# Run integration tests
uv run pytest -m integration

# Cleanup when done
docker-compose -f docker-compose.test.yml down
```

**Important:**
- **Unit tests** (`pytest -m unit`) use mocks and NEVER require Kafka (34 tests)
- **Integration tests** (`pytest -m integration`) REQUIRE real Kafka and will FAIL without it (14 tests)
- For local development, run unit tests only (no Kafka needed)
- Integration tests cover: sync/async produce/consume, consumer loops, Schema Registry, error handling

## Architecture

### Core Components

**1. Connection Management (src/flowodm/connection.py)**
- `KafkaConnection`: Thread-safe singleton managing both sync and async Kafka clients
- Schema Registry client initialization
- Configuration via environment variables

**2. ODM Model (src/flowodm/model.py)**
- `FlowBaseModel`: Pydantic v2 base class providing produce/consume operations
- Dual API: All methods have sync and async versions (async prefixed with `a`)
- Settings inner class for topic, schema, and consumer configuration

**3. Schema Utilities (src/flowodm/schema.py)**
- Schema validation against files or Schema Registry
- Model generation from Avro schemas
- Compatibility checking

**4. Consumer Loops (src/flowodm/consumer.py)**
- `ConsumerLoop`: Sync consumer loop for microservices
- `AsyncConsumerLoop`: Async consumer loop with concurrent processing
- Graceful shutdown handling

**5. Settings Profiles (src/flowodm/settings.py)**
- `LongRunningSettings`: For ML inference, complex processing
- `BatchSettings`: For ETL jobs, data aggregation
- `RealTimeSettings`: For event-driven, low-latency processing

**6. CLI (src/flowodm/cli.py)**
- Schema validation commands
- Schema Registry operations

### Key Design Patterns

**Settings Inner Class Pattern**
```python
class UserEvent(FlowBaseModel):
    class Settings:
        topic = "user-events"           # Required
        schema_subject = "user-events-value"  # Optional
        consumer_group = "my-service"   # For consuming
        key_field = "user_id"           # Field to use as message key

    user_id: str
    action: str
```

**Dual Sync/Async API**
- Sync: `produce()`, `consume_one()`, `consume_iter()`
- Async: `aproduce()`, `aconsume_one()`, `aconsume_iter()`

## Testing Strategy

**Strict Separation: Unit vs Integration**
- **Unit Tests** (`@pytest.mark.unit`): Use mock producers/consumers, test logic only (34 tests)
- **Integration Tests** (`@pytest.mark.integration`): Require real Kafka, test end-to-end (14 tests)

**Test Files**
- `conftest.py`: Mock fixtures (MockProducer, MockConsumer, MockSchemaRegistry) + connection reset
- `test_model.py`: Unit tests for FlowBaseModel (model creation, settings, serialization, schema gen)
- `test_schema.py`: Unit tests for schema utilities (loading, validation, type conversion)
- `test_integration.py`: Integration tests with real Kafka (produce/consume, loops, Schema Registry)

**Integration Test Coverage**
- Sync and async produce/consume operations
- Multiple message batch operations
- Message key handling
- Consumer loops (sync and async)
- Schema Registry registration and compatibility
- Error handling and timeouts
- Connection lifecycle management

## Design Philosophy

1. **Minimalistic**: Keep codebase small (~2000 lines)
2. **Schema-First**: Avro schemas as source of truth
3. **Dual Support**: Both sync and async APIs equally supported
4. **Pydantic Native**: Leverage Pydantic v2 fully
5. **Zero Abstraction Penalty**: Minimal overhead over raw confluent-kafka

## Code Style
- Black formatter (line length: 100)
- Ruff linter
- Type hints for all public APIs
- Google-style docstrings
