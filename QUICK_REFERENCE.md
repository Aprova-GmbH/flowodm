# Quick Reference Guide

## Publishing a New Release

```bash
# 1. Update version in 3 files:
#    - pyproject.toml (line 7)
#    - src/flowodm/__init__.py (__version__)
#    - docs/conf.py (release)

# 2. Update CHANGELOG.md with changes

# 3. Run tests (unit tests only - no Kafka needed!)
cd /Users/vykhand/DEV/flowodm
uv run pytest -m unit --cov=flowodm --cov-report=term-missing
uv run black --check src tests
uv run ruff check src tests

# 4. Commit version bump
git add pyproject.toml src/flowodm/__init__.py docs/conf.py CHANGELOG.md
git commit -m "Bump version to X.Y.Z"
git push origin main

# 5. Create and push tag (this triggers automatic publishing)
git tag vX.Y.Z
git push origin vX.Y.Z

# Done! GitHub Actions will:
# - Build package
# - Publish to PyPI
# - Create GitHub release
# - ReadTheDocs will rebuild docs automatically
```

---

## Verify Release

```bash
# Check GitHub Actions
open https://github.com/vykhand/flowodm/actions

# Check PyPI
open https://pypi.org/project/flowodm/

# Check Docs
open https://flowodm.readthedocs.io

# Test installation
pip install --upgrade flowodm
python -c "from flowodm import FlowBaseModel; print('OK')"
```

---

## Common Commands

```bash
# Run unit tests (fast - no Kafka needed!)
uv run pytest -m unit -v

# Run all tests (same as unit for now)
uv run pytest -v

# Run tests with coverage
uv run pytest -m unit --cov=flowodm --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_model.py -v

# Run specific test
uv run pytest tests/test_model.py::TestFlowBaseModel::test_produce_sync -v

# Format code
uv run black src tests

# Lint code
uv run ruff check src tests

# Fix linting issues
uv run ruff check --fix src tests

# Build package locally
uv run python -m build

# Check package metadata
uv run twine check dist/*

# Build documentation locally
cd docs
uv run sphinx-build -b html . _build/html
open _build/html/index.html

# Install package in editable mode
uv pip install -e .

# Install with dev dependencies
uv pip install -e ".[dev]"
```

---

## Development Setup

```bash
# Install uv (recommended package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies with development tools
uv pip install -e ".[dev]"

# Or using pip
pip install -e ".[dev]"
```

---

## Project Structure

```
flowodm/
├── src/flowodm/              # Source code
│   ├── __init__.py          # Public API exports
│   ├── connection.py        # KafkaConnection singleton
│   ├── model.py             # FlowBaseModel base class
│   ├── schema.py            # Schema validation utilities
│   ├── consumer.py          # ConsumerLoop patterns
│   ├── settings.py          # Predefined Kafka settings
│   ├── exceptions.py        # Custom exceptions
│   └── cli.py               # CLI tools
├── tests/                    # Test suite (34 unit tests)
│   ├── conftest.py          # Pytest fixtures with mocks
│   ├── test_model.py        # FlowBaseModel tests
│   └── test_schema.py       # Schema validation tests
├── docs/                     # Sphinx documentation
├── examples/                 # Usage examples
├── pyproject.toml           # Project configuration
└── CLAUDE_PLAN.md           # Implementation plan
```

---

## Core Architecture

**Connection Management** (`src/flowodm/connection.py`)
- `KafkaConnection`: Thread-safe singleton
- Manages Producer, Consumer, and SchemaRegistryClient
- Environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, etc.

**ODM Model** (`src/flowodm/model.py`)
- `FlowBaseModel`: Pydantic v2 base class
- Dual API: sync methods + async methods (prefixed with `a`)
- Auto-generated `message_id` field
- Avro serialization/deserialization

**Schema Utilities** (`src/flowodm/schema.py`)
- Schema validation against files and registry
- Model generation from Avro schemas
- Compatibility checking

**Consumer Loops** (`src/flowodm/consumer.py`)
- `ConsumerLoop` for sync microservices
- `AsyncConsumerLoop` for async with concurrency
- Graceful shutdown, error handling, retries

---

## Development Workflow

```bash
# Create feature branch
git checkout -b feature/my-feature

# Make changes and test (unit tests are fast!)
uv run pytest -m unit

# Format and lint
uv run black src tests
uv run ruff check --fix src tests

# Commit using Conventional Commits
git commit -m "feat: add new feature"

# Push and create PR
git push origin feature/my-feature
```

---

## Semantic Versioning Quick Guide

- **Patch** (0.1.0 → 0.1.1): Bug fixes only
- **Minor** (0.1.0 → 0.2.0): New features, backward compatible
- **Major** (0.9.0 → 1.0.0): Breaking changes

---

## Emergency: Manual Publishing

If GitHub Actions fails:

```bash
cd /Users/vykhand/DEV/flowodm
uv run python -m build
uv run twine upload dist/*
# Enter: __token__ (username)
# Enter: pypi-AgE... (your PyPI token)
```

---

## Testing Strategy

### Unit Tests (All 34 tests - FAST!)

```bash
# Run all unit tests (uses mocks - no Kafka needed!)
uv run pytest -m unit -v

# With coverage
uv run pytest -m unit --cov=flowodm --cov-report=term-missing
```

**Key Point:** All FlowODM tests use mocks and do NOT require:
- Kafka broker
- Schema Registry
- Docker containers

This makes local development and CI/CD extremely fast!

### Test Markers

- **`-m unit`**: All 34 tests using mocks (default)
- Future: Integration tests could be added for real Kafka testing

---

## Useful Git Commands

```bash
# View recent commits
git log --oneline -10

# View uncommitted changes
git status
git diff

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Delete local branch
git branch -d feature/old-feature

# Update from main
git checkout main
git pull origin main
git checkout feature/my-feature
git rebase main

# View all tags
git tag -l

# Delete tag (local and remote)
git tag -d v0.1.0
git push origin :refs/tags/v0.1.0
```

---

## Environment Setup

```bash
# Set Kafka connection for runtime (optional)
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SCHEMA_REGISTRY_URL="http://localhost:8081"

# Or create .env file
cat > .env <<EOF
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
EOF
```

**Note:** Environment variables are NOT needed for running tests!

---

## Usage Examples

### Define a Model

```python
from flowodm import FlowBaseModel
from datetime import datetime

class OrderEvent(FlowBaseModel):
    class Settings:
        topic = "orders"
        consumer_group = "order-processor"
        key_field = "order_id"

    order_id: str
    customer_id: str
    total: float
    created_at: datetime
```

### Sync Operations

```python
from flowodm import connect

# Connect to Kafka
connect(bootstrap_servers="localhost:9092")

# Produce
order = OrderEvent(
    order_id="ORD-123",
    customer_id="CUST-001",
    total=99.99,
    created_at=datetime.now()
)
order.produce_sync()

# Consume
for order in OrderEvent.consume_iter(max_messages=10):
    print(f"Processing order: {order.order_id}")
```

### Async Operations

```python
import asyncio
from flowodm import connect

connect(bootstrap_servers="localhost:9092")

async def main():
    # Produce
    order = OrderEvent(
        order_id="ORD-456",
        customer_id="CUST-002",
        total=149.99,
        created_at=datetime.now()
    )
    await order.aproduce()

    # Consume
    async for order in OrderEvent.aconsume_iter(max_messages=10):
        print(f"Processing order: {order.order_id}")

asyncio.run(main())
```

### Consumer Loop (Microservice Pattern)

```python
from flowodm import ConsumerLoop, LongRunningSettings

def process_order(order: OrderEvent) -> None:
    print(f"Processing {order.order_id}")
    # Your business logic here

loop = ConsumerLoop(
    model=OrderEvent,
    handler=process_order,
    settings=LongRunningSettings(),
)

loop.run()  # Blocks until SIGTERM/SIGINT
```

---

## Key Design Patterns

**Settings Class Pattern**
```python
class MyEvent(FlowBaseModel):
    class Settings:
        topic = "my-topic"
        consumer_group = "my-service"
        key_field = "event_id"
```

**Method Pairs (Sync/Async)**
- `produce()` / `aproduce()`
- `produce_sync()` / `aproduce()`
- `consume_one()` / `aconsume_one()`
- `consume_iter()` / `aconsume_iter()`

**Predefined Settings Profiles**
- `LongRunningSettings` - ML inference, complex processing
- `BatchSettings` - ETL, aggregation
- `RealTimeSettings` - Notifications, event-driven
- `HighThroughputSettings` - High-volume processing
- `ReliableSettings` - At-least-once delivery

---

## Testing Best Practices

- **All tests use mocks** - No Kafka/Schema Registry required
- Use `pytest -m unit` for fast local testing
- Mock fixtures in `tests/conftest.py`
- Target >95% code coverage
- Test both sync and async code paths
- Reset connection singleton between tests

---

## Documentation

```bash
# Build and view docs locally
cd docs && \
uv run sphinx-build -b html . _build/html && \
open _build/html/index.html

# Docs auto-deploy to ReadTheDocs on push to main
# URL: https://flowodm.readthedocs.io
```

---

## Code Style

- **Formatter**: Black (line length: 100)
- **Linter**: Ruff
- **Type Checking**: mypy
- **Docstrings**: Google style
- **Commits**: Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`)

---

## Important Links

- **Repository**: https://github.com/vykhand/flowodm
- **PyPI**: https://pypi.org/project/flowodm/
- **Documentation**: https://flowodm.readthedocs.io
- **Issues**: https://github.com/vykhand/flowodm/issues

---

## Clean Up

```bash
# Remove build artifacts
rm -rf dist/ build/ *.egg-info/

# Remove pytest cache
rm -rf .pytest_cache/ __pycache__/

# Remove coverage reports
rm -rf htmlcov/ .coverage

# Remove docs build
rm -rf docs/_build/

# All at once
rm -rf dist/ build/ *.egg-info/ .pytest_cache/ __pycache__/ htmlcov/ .coverage docs/_build/
```

---

## CLI Tools

```bash
# Validate models against schemas
flowodm validate --models myapp.events --schemas-dir schemas/

# Check compatibility
flowodm check-compatibility --models myapp.events --level BACKWARD

# Upload schema to registry
flowodm upload-schema --avro schemas/event.avsc --subject my-event-value

# List Schema Registry subjects
flowodm list-subjects

# Get schema from registry
flowodm get-schema --subject my-event-value
```
