Consuming Messages
==================

FlowODM provides multiple ways to consume messages from Kafka topics.

Basic Consumption
-----------------

**Consume Single Message**

.. code-block:: python

   from flowodm import FlowBaseModel, connect

   connect(bootstrap_servers="localhost:9092")

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"
           consumer_group = "order-processor"

       order_id: str
       amount: float

   # Wait up to 5 seconds for a message
   event = OrderEvent.consume_one(timeout=5.0)
   if event:
       print(f"Received order: {event.order_id}")

**Iterate Over Messages**

.. code-block:: python

   # Process up to 100 messages
   for event in OrderEvent.consume_iter(max_messages=100):
       print(f"Processing order: {event.order_id}")

   # Process until timeout
   for event in OrderEvent.consume_iter(timeout=10.0):
       process_order(event)

Asynchronous Consumption
------------------------

For async applications:

.. code-block:: python

   import asyncio

   async def main():
       # Single message
       event = await OrderEvent.aconsume_one(timeout=5.0)
       if event:
           print(f"Received: {event.order_id}")

       # Iterate asynchronously
       async for event in OrderEvent.aconsume_iter(max_messages=100):
           await process_order_async(event)

   asyncio.run(main())

Consumer Groups
---------------

Consumer groups enable parallel processing:

.. code-block:: python

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"
           consumer_group = "order-processor"  # Group ID

       order_id: str
       amount: float

Multiple instances with the same ``consumer_group`` will share the workload.

Offset Management
-----------------

**Auto-Commit (Default)**

By default, offsets are committed automatically:

.. code-block:: python

   for event in OrderEvent.consume_iter():
       process_order(event)
       # Offset committed automatically after processing

**Manual Commit**

For more control:

.. code-block:: python

   connect(
       bootstrap_servers="localhost:9092",
       consumer_config={"enable.auto.commit": False}
   )

   from flowodm import get_consumer

   consumer = get_consumer("order-processor")

   for event in OrderEvent.consume_iter():
       try:
           process_order(event)
           consumer.commit()  # Commit after successful processing
       except Exception as e:
           # Don't commit on error - message will be reprocessed
           log_error(e)

Starting Position
-----------------

Control where consumption starts:

.. code-block:: python

   connect(
       bootstrap_servers="localhost:9092",
       consumer_config={
           "auto.offset.reset": "earliest",  # or "latest"
       }
   )

* ``earliest``: Start from the beginning of the topic
* ``latest``: Start from the end (new messages only)

Deserialization
---------------

Messages are automatically deserialized from Avro:

.. code-block:: python

   for event in OrderEvent.consume_iter():
       # event is a fully typed OrderEvent instance
       print(event.order_id)      # str
       print(event.amount)        # float
       print(event.message_id)    # Auto-generated ID

Handling Errors
---------------

.. code-block:: python

   from flowodm.exceptions import ConsumerError, DeserializationError

   try:
       for event in OrderEvent.consume_iter():
           process_order(event)
   except DeserializationError as e:
       print(f"Failed to deserialize message: {e}")
   except ConsumerError as e:
       print(f"Consumer error: {e}")

Filtering
---------

Filter messages during consumption:

.. code-block:: python

   # Filter in application code
   for event in OrderEvent.consume_iter():
       if event.amount > 100:  # Only process high-value orders
           process_order(event)

Consumer Configuration
----------------------

Customize consumer settings:

.. code-block:: python

   connect(
       bootstrap_servers="localhost:9092",
       consumer_config={
           "session.timeout.ms": 30000,
           "max.poll.interval.ms": 300000,
           "max.poll.records": 500,
           "fetch.min.bytes": 1,
           "fetch.max.wait.ms": 500,
       }
   )

See :doc:`settings_profiles` for predefined configurations.
