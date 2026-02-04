#!/usr/bin/env python
"""
Microservice example using FlowODM consumer loops.

This example demonstrates how to build a Kafka consumer microservice with:
1. Graceful shutdown handling
2. Error handling and retries
3. Lifecycle hooks (startup/shutdown)
4. Long-running task settings
"""

import logging
from datetime import datetime

from flowodm import (
    FlowBaseModel,
    ConsumerLoop,
    LongRunningSettings,
    connect,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Define the message model
class OrderEvent(FlowBaseModel):
    """Order event to process."""

    class Settings:
        topic = "orders"
        consumer_group = "order-processor"

    order_id: str
    customer_id: str
    product_name: str
    quantity: int
    total_price: float
    created_at: datetime


# Database simulation
processed_orders: list[str] = []


def process_order(order: OrderEvent) -> None:
    """
    Process an order event.

    This is where your business logic goes. It could:
    - Update a database
    - Call external APIs
    - Trigger downstream processes
    """
    logger.info(f"Processing order {order.order_id} for customer {order.customer_id}")
    logger.info(f"  Product: {order.product_name} x {order.quantity}")
    logger.info(f"  Total: ${order.total_price:.2f}")

    # Simulate processing (e.g., database update)
    processed_orders.append(order.order_id)

    logger.info(f"Order {order.order_id} processed successfully")


def handle_error(
    error: Exception, raw_message, deserialized: OrderEvent | None
) -> None:
    """
    Handle processing errors.

    Args:
        error: The exception that occurred
        raw_message: The raw Kafka message
        deserialized: The deserialized OrderEvent if deserialization succeeded,
            None if deserialization failed

    Options for error handling:
    - Log and continue (current behavior)
    - Send to dead letter queue
    - Alert operations team
    - Retry with backoff
    """
    if deserialized is not None:
        # Handler failed but we have the deserialized message
        logger.error(f"Failed to process order {deserialized.order_id}: {error}")
    else:
        # Deserialization failed
        logger.error(f"Failed to deserialize message: {error}")
    # In production, you might:
    # - Send to a dead letter topic
    # - Store in a failed messages database
    # - Send alert to ops team


def on_startup() -> None:
    """Called before the consumer loop starts."""
    logger.info("=== Order Processor Starting ===")
    logger.info("Initializing database connections...")
    # Initialize resources, connections, caches, etc.


def on_shutdown() -> None:
    """Called after the consumer loop stops."""
    logger.info("=== Order Processor Shutting Down ===")
    logger.info(f"Processed {len(processed_orders)} orders in this session")
    # Cleanup resources, flush buffers, close connections, etc.


def main():
    """Run the order processor microservice."""
    # Connect to Kafka
    connect(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )

    # Create and configure the consumer loop
    loop = ConsumerLoop(
        model=OrderEvent,
        handler=process_order,
        settings=LongRunningSettings(),  # Tolerates up to 10 min processing
        commit_strategy="before_processing",  # Prevents duplicate processing in parallel pods
        error_handler=handle_error,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        max_retries=3,
        retry_delay=1.0,
    )

    # Run the loop (blocking)
    # Will handle SIGTERM and SIGINT for graceful shutdown
    logger.info("Starting consumer loop...")
    logger.info("Press Ctrl+C to stop")
    loop.run()


if __name__ == "__main__":
    main()
