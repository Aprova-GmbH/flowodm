Producing Messages
==================

FlowODM provides both synchronous and asynchronous methods for producing messages.

Synchronous Production
----------------------

**Blocking Production**

Use ``produce()`` to produce a message and wait for broker acknowledgment:

.. code-block:: python

   from datetime import datetime
   from flowodm import FlowBaseModel, connect

   connect(bootstrap_servers="localhost:9092")

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"
           key_field = "order_id"

       order_id: str
       amount: float

   order = OrderEvent(order_id="ORD-001", amount=99.99)
   order.produce()  # Blocks until acknowledged

**Non-Blocking Production**

Use ``produce_nowait()`` for non-blocking production (fire-and-forget):

.. code-block:: python

   order.produce_nowait()  # Returns immediately

   # Flush all pending messages
   from flowodm import get_producer
   get_producer().flush()

Asynchronous Production
-----------------------

For async applications, use ``aproduce()``:

.. code-block:: python

   import asyncio
   from flowodm import FlowBaseModel, connect

   connect(bootstrap_servers="localhost:9092")

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"

       order_id: str
       amount: float

   async def main():
       order = OrderEvent(order_id="ORD-001", amount=99.99)
       await order.aproduce()

   asyncio.run(main())

Message Keys
------------

Messages can be produced with a key for partitioning:

**Using key_field Setting**

.. code-block:: python

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"
           key_field = "order_id"  # Use this field as key

       order_id: str
       amount: float

   # order_id value is automatically used as the message key
   order = OrderEvent(order_id="ORD-001", amount=99.99)
   order.produce()

**Explicit Key**

.. code-block:: python

   order = OrderEvent(order_id="ORD-001", amount=99.99)
   order.produce(key="custom-key")

Delivery Callbacks
------------------

For non-blocking production, you can provide a callback:

.. code-block:: python

   def on_delivery(err, msg):
       if err is not None:
           print(f"Delivery failed: {err}")
       else:
           print(f"Message delivered to {msg.topic()}/{msg.partition()}")

   order.produce_nowait(callback=on_delivery)

Batch Production
----------------

For high-throughput scenarios, produce multiple messages efficiently:

.. code-block:: python

   from flowodm import get_producer

   orders = [
       OrderEvent(order_id=f"ORD-{i:03d}", amount=float(i * 10))
       for i in range(1000)
   ]

   for order in orders:
       order.produce()  # Non-blocking

   # Flush all pending messages
   get_producer().flush()

Producer Configuration
----------------------

Customize producer settings via ``connect()``:

.. code-block:: python

   connect(
       bootstrap_servers="localhost:9092",
       producer_config={
           "acks": "all",
           "retries": 3,
           "linger.ms": 10,
           "batch.size": 16384,
           "compression.type": "snappy",
       }
   )

Wire Format
-----------

When a Schema Registry is configured, produced messages include the standard
Confluent wire format header (magic byte + 4-byte schema ID). This ensures
compatibility with Confluent consumers, Java's ``KafkaAvroDeserializer``, and
Kafka UI tools (AKHQ, Kafdrop, etc.).

The schema ID is cached per model class, so only the first serialization
triggers a registry call.

To disable the wire format header, set ``confluent_wire_format = False`` in
your model's ``Settings`` class. See :doc:`models` for details.

Error Handling
--------------

Handle production errors appropriately:

.. code-block:: python

   from flowodm.exceptions import ProducerError

   try:
       order.produce()
   except ProducerError as e:
       print(f"Failed to produce message: {e}")
       # Handle error: retry, log, alert, etc.
