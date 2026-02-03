# Quick Start Guide

This guide will help you get started with FlowODM in minutes.

## Installation

### From PyPI (once published)

```bash
pip install flowodm
```

### For Development

```bash
# Clone the repository
git clone https://github.com/Aprova-GmbH/flowodm.git
cd flowodm

# Install in development mode
pip install -e .

# Or with dev dependencies
pip install -e ".[dev]"
```

## Basic Setup

### 1. Configure Kafka Connection

Set environment variables for your Kafka cluster:

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SCHEMA_REGISTRY_URL="http://localhost:8081"
```

Or create a `.env` file:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
```

**For Confluent Cloud:**

```bash
export KAFKA_BOOTSTRAP_SERVERS="pkc-xxx.us-east-2.aws.confluent.cloud:9092"
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_SASL_MECHANISM="PLAIN"
export KAFKA_SASL_USERNAME="YOUR_API_KEY"
export KAFKA_SASL_PASSWORD="YOUR_API_SECRET"
export SCHEMA_REGISTRY_URL="https://psrc-xxx.us-east-2.aws.confluent.cloud"
# Option 1: Combined format (recommended)
export SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="SR_API_KEY:SR_API_SECRET"
# Option 2: Separate key/secret
# export SCHEMA_REGISTRY_API_KEY="SR_API_KEY"
# export SCHEMA_REGISTRY_API_SECRET="SR_API_SECRET"
```

### 2. Define Your First Model

Create a file `models.py`:

```python
from flowodm import FlowBaseModel
from datetime import datetime

class UserEvent(FlowBaseModel):
    class Settings:
        topic = "user-events"
        consumer_group = "my-service"

    user_id: str
    action: str
    timestamp: datetime
```

### 3. Use Your Model

#### Synchronous Usage

```python
from models import UserEvent
from flowodm import connect
from datetime import datetime

# Connect to Kafka
connect(bootstrap_servers="localhost:9092")

# Produce a message
event = UserEvent(
    user_id="user123",
    action="login",
    timestamp=datetime.now()
)
event.produce()
print(f"Produced event with ID: {event.message_id}")

# Consume messages
for event in UserEvent.consume_iter(max_messages=10):
    print(f"User {event.user_id} performed {event.action}")
```

#### Asynchronous Usage

```python
import asyncio
from models import UserEvent
from flowodm import connect
from datetime import datetime

connect(bootstrap_servers="localhost:9092")

async def main():
    # Produce a message
    event = UserEvent(
        user_id="user456",
        action="logout",
        timestamp=datetime.now()
    )
    await event.aproduce()
    print(f"Produced event with ID: {event.message_id}")

    # Consume messages
    async for event in UserEvent.aconsume_iter(max_messages=10):
        print(f"User {event.user_id} performed {event.action}")

asyncio.run(main())
```

## Complete Example

Here's a complete working example:

```python
import asyncio
from datetime import datetime
from flowodm import FlowBaseModel, connect

# Define your model
class OrderEvent(FlowBaseModel):
    class Settings:
        topic = "orders"
        consumer_group = "order-processor"
        key_field = "order_id"

    order_id: str
    customer_id: str
    product_name: str
    quantity: int
    total_price: float
    created_at: datetime


async def demo():
    # Connect to Kafka
    connect(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081"
    )

    # Create and produce orders
    orders = [
        OrderEvent(
            order_id="ORD-001",
            customer_id="CUST-001",
            product_name="Laptop",
            quantity=1,
            total_price=999.99,
            created_at=datetime.now()
        ),
        OrderEvent(
            order_id="ORD-002",
            customer_id="CUST-002",
            product_name="Mouse",
            quantity=2,
            total_price=49.98,
            created_at=datetime.now()
        ),
        OrderEvent(
            order_id="ORD-003",
            customer_id="CUST-003",
            product_name="Keyboard",
            quantity=1,
            total_price=79.99,
            created_at=datetime.now()
        ),
    ]

    # Produce orders
    print("Producing orders...")
    for order in orders:
        await order.aproduce()
        print(f"  Produced: {order.order_id} - {order.product_name}")

    # Consume orders
    print("\nConsuming orders...")
    order_count = 0
    total_revenue = 0.0

    async for order in OrderEvent.aconsume_iter(max_messages=3, timeout=5.0):
        print(f"  Received: {order.order_id}")
        print(f"    Customer: {order.customer_id}")
        print(f"    Product: {order.product_name}")
        print(f"    Total: ${order.total_price:.2f}")
        order_count += 1
        total_revenue += order.total_price

    print(f"\nProcessed {order_count} orders")
    print(f"Total revenue: ${total_revenue:.2f}")


if __name__ == "__main__":
    asyncio.run(demo())
```

## Building a Microservice

Use `ConsumerLoop` for production-ready microservices:

```python
import logging
from datetime import datetime
from flowodm import (
    FlowBaseModel,
    ConsumerLoop,
    LongRunningSettings,
    connect,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrderEvent(FlowBaseModel):
    class Settings:
        topic = "orders"
        consumer_group = "order-processor"

    order_id: str
    customer_id: str
    total_price: float
    created_at: datetime


def process_order(order: OrderEvent) -> None:
    """Process an order (your business logic here)."""
    logger.info(f"Processing order {order.order_id}")
    logger.info(f"  Customer: {order.customer_id}")
    logger.info(f"  Total: ${order.total_price:.2f}")
    # Your business logic here
    # - Update inventory
    # - Send confirmation email
    # - Update analytics
    # etc.


def handle_error(error: Exception, raw_message) -> None:
    """Handle processing errors."""
    logger.error(f"Failed to process message: {error}")
    # Send to dead letter queue, alert ops, etc.


def on_startup() -> None:
    """Called when the service starts."""
    logger.info("=== Order Processor Starting ===")


def on_shutdown() -> None:
    """Called when the service stops."""
    logger.info("=== Order Processor Shutting Down ===")


def main():
    # Connect to Kafka
    connect(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )

    # Create and configure consumer loop
    loop = ConsumerLoop(
        model=OrderEvent,
        handler=process_order,
        settings=LongRunningSettings(),  # Optimized for long tasks
        error_handler=handle_error,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        max_retries=3,
        retry_delay=1.0,
    )

    # Run forever (until SIGTERM/SIGINT)
    logger.info("Starting consumer loop...")
    loop.run()


if __name__ == "__main__":
    main()
```

Run the microservice:

```bash
python order_processor.py

# Gracefully stop with Ctrl+C or:
kill -SIGTERM <pid>
```

## Next Steps

- Read the [full documentation](https://flowodm.readthedocs.io)
- Check out [examples/](examples/) for more use cases
- Learn about [Schema Registry integration](docs/schema_registry.rst)
- Explore [Settings Profiles](docs/settings_profiles.rst) for different workloads

## Common Patterns

### Message Keys for Partitioning

```python
class OrderEvent(FlowBaseModel):
    class Settings:
        topic = "orders"
        key_field = "customer_id"  # Partition by customer

    customer_id: str
    order_id: str
    total: float

# Messages with same customer_id go to same partition
order.produce()
```

### Non-Blocking Production

```python
from flowodm import get_producer

# Fire and forget
order.produce()

# Flush all pending messages later
get_producer().flush()
```

### Async Consumer Loop

```python
from flowodm import AsyncConsumerLoop

async def process_order_async(order: OrderEvent) -> None:
    await do_async_work(order)

loop = AsyncConsumerLoop(
    model=OrderEvent,
    handler=process_order_async,
    max_concurrent=20,  # Process 20 messages concurrently
)

await loop.run()
```

### Schema Validation

```python
from flowodm.schema import validate_against_registry

# Validate your model against Schema Registry
result = validate_against_registry(
    model_class=OrderEvent,
    subject="orders-value",
)

if result.is_valid:
    print("Model is valid!")
else:
    for error in result.errors:
        print(f"  - {error}")
```

### Settings Profiles

```python
from flowodm import (
    LongRunningSettings,    # ML inference (10 min timeout)
    BatchSettings,          # ETL (5 min, 500 records)
    RealTimeSettings,       # Notifications (30 sec, 10 records)
    HighThroughputSettings, # Logs (1000 records)
    ReliableSettings,       # Payments (manual commit)
)

loop = ConsumerLoop(
    model=OrderEvent,
    handler=process_order,
    settings=RealTimeSettings(),  # For low-latency processing
)
```

## Troubleshooting

### Connection Issues

If you get connection errors, verify:
1. Kafka broker is running and accessible
2. Environment variables are set correctly
3. Network/firewall allows connection
4. Security credentials are valid (for Confluent Cloud)

```bash
# Test Kafka connection
kafka-console-producer --bootstrap-server localhost:9092 --topic test

# Test Schema Registry
curl http://localhost:8081/subjects
```

### Import Errors

If imports fail, ensure dependencies are installed:

```bash
pip install pydantic confluent-kafka fastavro
```

### Model Not Found

Make sure your model has a `Settings` class with a `topic`:

```python
class MyEvent(FlowBaseModel):
    class Settings:
        topic = "my-topic"  # Required!

    field: str
```

### Consumer Not Receiving Messages

Check:
1. Topic exists in Kafka
2. Messages are being produced to the correct topic
3. Consumer group ID doesn't have committed offsets past the messages
4. `auto.offset.reset` setting (defaults to `latest`)

```python
# Start from beginning of topic
connect(
    bootstrap_servers="localhost:9092",
    consumer_config={"auto.offset.reset": "earliest"}
)
```

## Running Kafka Locally

For development, run Kafka with Docker:

```bash
# Simple setup (no authentication)
docker run -d \
  --name kafka \
  -p 9092:9092 \
  -p 9101:9101 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:latest

# With Schema Registry
docker run -d \
  --name schema-registry \
  --link kafka \
  -p 8081:8081 \
  -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9092 \
  confluentinc/cp-schema-registry:latest
```

## Getting Help

- **Documentation**: https://flowodm.readthedocs.io
- **GitHub Issues**: https://github.com/Aprova-GmbH/flowodm/issues
- **Email**: andy-v-github@outlook.com
