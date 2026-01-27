#!/usr/bin/env python
"""
Basic producer example for FlowODM.

This example demonstrates how to:
1. Define a Pydantic model for Kafka messages
2. Connect to Kafka
3. Produce messages synchronously and asynchronously
"""

import asyncio
from datetime import datetime

from flowodm import FlowBaseModel, connect


# Define your message model
class OrderEvent(FlowBaseModel):
    """Order event model."""

    class Settings:
        topic = "orders"
        key_field = "order_id"  # Use order_id as message key

    order_id: str
    customer_id: str
    product_name: str
    quantity: int
    total_price: float
    created_at: datetime


def main_sync():
    """Synchronous producer example."""
    # Connect to Kafka (uses environment variables if not specified)
    connect(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )

    # Create an order event
    order = OrderEvent(
        order_id="ORD-001",
        customer_id="CUST-123",
        product_name="Widget Pro",
        quantity=2,
        total_price=49.99,
        created_at=datetime.now(),
    )

    # Produce single message (non-blocking)
    order.produce()
    print(f"Produced order {order.order_id} (non-blocking)")

    # Produce with confirmation (blocking)
    order2 = OrderEvent(
        order_id="ORD-002",
        customer_id="CUST-456",
        product_name="Gadget Plus",
        quantity=1,
        total_price=99.99,
        created_at=datetime.now(),
    )
    order2.produce_sync()
    print(f"Produced order {order2.order_id} (with confirmation)")

    # Batch produce
    orders = [
        OrderEvent(
            order_id=f"ORD-{100 + i}",
            customer_id=f"CUST-{i}",
            product_name=f"Product {i}",
            quantity=i + 1,
            total_price=float(i * 10),
            created_at=datetime.now(),
        )
        for i in range(10)
    ]

    count = OrderEvent.produce_many(orders)
    print(f"Produced {count} orders in batch")


async def main_async():
    """Asynchronous producer example."""
    connect(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )

    # Async produce
    order = OrderEvent(
        order_id="ORD-ASYNC-001",
        customer_id="CUST-789",
        product_name="Async Widget",
        quantity=5,
        total_price=149.99,
        created_at=datetime.now(),
    )

    await order.aproduce()
    print(f"Async produced order {order.order_id}")

    # Async batch produce
    orders = [
        OrderEvent(
            order_id=f"ORD-ASYNC-{i}",
            customer_id=f"CUST-ASYNC-{i}",
            product_name=f"Async Product {i}",
            quantity=i,
            total_price=float(i * 15),
            created_at=datetime.now(),
        )
        for i in range(5)
    ]

    count = await OrderEvent.aproduce_many(orders)
    print(f"Async produced {count} orders")


if __name__ == "__main__":
    print("=== Sync Producer Example ===")
    main_sync()

    print("\n=== Async Producer Example ===")
    asyncio.run(main_async())
