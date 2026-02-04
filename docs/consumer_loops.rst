Consumer Loops
==============

Consumer loops provide a robust pattern for building Kafka microservices with
graceful shutdown, error handling, and lifecycle management.

Basic Consumer Loop
-------------------

**Synchronous**

.. code-block:: python

   from flowodm import FlowBaseModel, ConsumerLoop, connect

   connect(bootstrap_servers="localhost:9092")

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"
           consumer_group = "order-processor"

       order_id: str
       amount: float

   def process_order(order: OrderEvent) -> None:
       print(f"Processing order {order.order_id}: ${order.amount:.2f}")
       # Your business logic here

   loop = ConsumerLoop(
       model=OrderEvent,
       handler=process_order,
   )

   loop.run()  # Blocks until shutdown signal

**Asynchronous**

.. code-block:: python

   import asyncio
   from flowodm import AsyncConsumerLoop

   async def process_order_async(order: OrderEvent) -> None:
       await do_async_processing(order)

   loop = AsyncConsumerLoop(
       model=OrderEvent,
       handler=process_order_async,
   )

   asyncio.run(loop.run())

Error Handling
--------------

Provide a custom error handler. The handler receives three arguments:

- ``error``: The exception that occurred
- ``raw_message``: The raw Kafka message
- ``deserialized``: The deserialized ``FlowBaseModel`` instance if deserialization
  succeeded (but the handler failed), or ``None`` if deserialization itself failed

.. code-block:: python

   def handle_error(
       error: Exception, raw_message, deserialized: OrderEvent | None
   ) -> None:
       if deserialized is not None:
           # Handler failed but we have the deserialized message
           print(f"Failed to process order {deserialized.order_id}: {error}")
       else:
           # Deserialization failed
           print(f"Failed to deserialize message: {error}")
       # Options:
       # - Log and continue
       # - Send to dead letter queue
       # - Alert operations team

   loop = ConsumerLoop(
       model=OrderEvent,
       handler=process_order,
       error_handler=handle_error,
   )

Retries
-------

Configure automatic retries:

.. code-block:: python

   loop = ConsumerLoop(
       model=OrderEvent,
       handler=process_order,
       max_retries=3,         # Retry up to 3 times
       retry_delay=1.0,       # Wait 1 second between retries
       error_handler=handle_error,  # Called after all retries exhausted
   )

Commit Strategies
-----------------

Control when message offsets are committed to prevent duplicates or ensure reliability:

**before_processing** (Recommended for parallel deployments):
Commits immediately after receiving, before handler execution. Prevents duplicate processing
in parallel pod deployments but may lose messages if processing fails (at-most-once delivery).

**after_processing** (Recommended for single consumer):
Commits after successful processing. Guarantees at-least-once delivery but may cause
duplicates in parallel deployments.

Example:

.. code-block:: python

   # Prevent duplicates in Kubernetes/parallel deployments
   loop = ConsumerLoop(
       model=OrderEvent,
       handler=process_order,
       commit_strategy="before_processing",
   )

   # Guarantee no message loss (may process duplicates)
   loop = ConsumerLoop(
       model=OrderEvent,
       handler=process_order,
       commit_strategy="after_processing",
   )

**Best Practices:**

- Use ``before_processing`` when running multiple consumer instances in parallel
- Make handlers idempotent when possible
- Use ``error_handler`` to capture failed messages for dead letter queue
- Monitor failed commits in logs/metrics

Lifecycle Hooks
---------------

Execute code on startup and shutdown:

.. code-block:: python

   db_connection = None

   def on_startup() -> None:
       global db_connection
       print("Starting up...")
       db_connection = connect_to_database()

   def on_shutdown() -> None:
       global db_connection
       print("Shutting down...")
       if db_connection:
           db_connection.close()

   loop = ConsumerLoop(
       model=OrderEvent,
       handler=process_order,
       on_startup=on_startup,
       on_shutdown=on_shutdown,
   )

Settings Profiles
-----------------

Use predefined settings for different scenarios:

.. code-block:: python

   from flowodm import LongRunningSettings, BatchSettings, RealTimeSettings

   # For long-running processing (ML inference, complex operations)
   loop = ConsumerLoop(
       model=OrderEvent,
       handler=process_order,
       settings=LongRunningSettings(),  # Tolerates up to 10 min processing
   )

   # For batch processing (ETL, aggregation)
   loop = ConsumerLoop(
       model=OrderEvent,
       handler=batch_process,
       settings=BatchSettings(),  # Larger batches, 5 min poll interval
   )

   # For real-time processing (notifications, event-driven)
   loop = ConsumerLoop(
       model=OrderEvent,
       handler=quick_process,
       settings=RealTimeSettings(),  # Small batches, fast polling
   )

Graceful Shutdown
-----------------

Consumer loops handle shutdown signals automatically:

.. code-block:: python

   loop = ConsumerLoop(
       model=OrderEvent,
       handler=process_order,
   )

   # Handles SIGTERM and SIGINT gracefully
   # - Completes current message processing
   # - Commits offsets
   # - Calls on_shutdown hook
   # - Closes connections
   loop.run()

You can also trigger shutdown programmatically:

.. code-block:: python

   # In another thread or async task
   loop.stop()

Concurrent Processing (Async)
-----------------------------

Process multiple messages concurrently with ``AsyncConsumerLoop``:

.. code-block:: python

   async def process_order_async(order: OrderEvent) -> None:
       await asyncio.sleep(1)  # Simulate async work
       print(f"Processed {order.order_id}")

   loop = AsyncConsumerLoop(
       model=OrderEvent,
       handler=process_order_async,
       max_concurrent=20,  # Process up to 20 messages concurrently
   )

   await loop.run()

Decorator Style
---------------

For simple use cases, use the decorator:

.. code-block:: python

   from flowodm import consumer_loop

   @consumer_loop(OrderEvent)
   def process_order(order: OrderEvent) -> None:
       print(f"Processing {order.order_id}")

   # Start the loop
   process_order()

Async version:

.. code-block:: python

   from flowodm import async_consumer_loop

   @async_consumer_loop(OrderEvent)
   async def process_order_async(order: OrderEvent) -> None:
       await handle_order(order)

   await process_order_async()

Complete Microservice Example
-----------------------------

.. code-block:: python

   #!/usr/bin/env python
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

   # State
   processed_count = 0

   def process_order(order: OrderEvent) -> None:
       global processed_count
       logger.info(f"Processing order {order.order_id}")
       # Business logic here
       processed_count += 1

   def handle_error(
       error: Exception, raw, deserialized: OrderEvent | None
   ) -> None:
       if deserialized is not None:
           logger.error(f"Failed to process order {deserialized.order_id}: {error}")
       else:
           logger.error(f"Failed to deserialize: {error}")
       # Send to dead letter queue, alert, etc.

   def on_startup() -> None:
       logger.info("=== Order Processor Starting ===")

   def on_shutdown() -> None:
       logger.info(f"=== Processed {processed_count} orders ===")

   def main():
       connect(
           bootstrap_servers="localhost:9092",
           schema_registry_url="http://localhost:8081",
       )

       loop = ConsumerLoop(
           model=OrderEvent,
           handler=process_order,
           settings=LongRunningSettings(),
           error_handler=handle_error,
           on_startup=on_startup,
           on_shutdown=on_shutdown,
           max_retries=3,
           retry_delay=1.0,
       )

       logger.info("Starting consumer loop...")
       loop.run()

   if __name__ == "__main__":
       main()
