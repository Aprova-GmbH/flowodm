Migration Guide
===============

This guide helps you migrate from other Kafka libraries to FlowODM.

From aiokafka
-------------

**Before (aiokafka)**

.. code-block:: python

   from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
   import json

   async def produce():
       producer = AIOKafkaProducer(
           bootstrap_servers='localhost:9092',
           value_serializer=lambda v: json.dumps(v).encode()
       )
       await producer.start()
       try:
           await producer.send('orders', {'order_id': '123', 'amount': 99.99})
       finally:
           await producer.stop()

   async def consume():
       consumer = AIOKafkaConsumer(
           'orders',
           bootstrap_servers='localhost:9092',
           value_deserializer=lambda v: json.loads(v)
       )
       await consumer.start()
       try:
           async for msg in consumer:
               print(msg.value)
       finally:
           await consumer.stop()

**After (FlowODM)**

.. code-block:: python

   from flowodm import FlowBaseModel, connect

   connect(bootstrap_servers="localhost:9092")

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"
           consumer_group = "order-processor"

       order_id: str
       amount: float

   async def produce():
       order = OrderEvent(order_id="123", amount=99.99)
       await order.aproduce()

   async def consume():
       async for event in OrderEvent.aconsume_iter():
           print(event)

From confluent-kafka
--------------------

**Before (confluent-kafka)**

.. code-block:: python

   from confluent_kafka import Producer, Consumer
   from confluent_kafka.schema_registry import SchemaRegistryClient
   from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

   sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
   schema_str = '{"type": "record", "name": "Order", "fields": [...]}'

   avro_serializer = AvroSerializer(sr_client, schema_str)
   avro_deserializer = AvroDeserializer(sr_client, schema_str)

   producer = Producer({'bootstrap.servers': 'localhost:9092'})
   producer.produce('orders', value=avro_serializer({'order_id': '123'}))
   producer.flush()

   consumer = Consumer({
       'bootstrap.servers': 'localhost:9092',
       'group.id': 'order-processor',
       'auto.offset.reset': 'earliest'
   })
   consumer.subscribe(['orders'])

   while True:
       msg = consumer.poll(1.0)
       if msg is not None:
           order = avro_deserializer(msg.value())
           print(order)

**After (FlowODM)**

.. code-block:: python

   from flowodm import FlowBaseModel, connect

   connect(
       bootstrap_servers="localhost:9092",
       schema_registry_url="http://localhost:8081"
   )

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"
           consumer_group = "order-processor"

       order_id: str

   # Produce
   order = OrderEvent(order_id="123")
   order.produce()

   # Consume
   for event in OrderEvent.consume_iter():
       print(event)

From faust
----------

**Before (faust)**

.. code-block:: python

   import faust

   app = faust.App('myapp', broker='kafka://localhost:9092')

   class Order(faust.Record):
       order_id: str
       amount: float

   orders_topic = app.topic('orders', value_type=Order)

   @app.agent(orders_topic)
   async def process_orders(orders):
       async for order in orders:
           print(f'Processing {order.order_id}')

   if __name__ == '__main__':
       app.main()

**After (FlowODM)**

.. code-block:: python

   from flowodm import FlowBaseModel, AsyncConsumerLoop, connect

   connect(bootstrap_servers="localhost:9092")

   class Order(FlowBaseModel):
       class Settings:
           topic = "orders"
           consumer_group = "myapp"

       order_id: str
       amount: float

   async def process_order(order: Order) -> None:
       print(f'Processing {order.order_id}')

   loop = AsyncConsumerLoop(
       model=Order,
       handler=process_order,
   )

   if __name__ == '__main__':
       import asyncio
       asyncio.run(loop.run())

Key Differences
---------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Feature
     - Other Libraries
     - FlowODM
   * - Model Definition
     - Dataclasses, Records, or dicts
     - Pydantic models with Settings
   * - Schema Management
     - Manual setup
     - Automatic with Settings
   * - Serialization
     - Explicit serializers
     - Built-in Avro support
   * - Consumer Groups
     - Per-consumer config
     - Per-model Settings
   * - Error Handling
     - Manual try/catch
     - Built-in error handlers
   * - Lifecycle
     - Manual setup/teardown
     - ConsumerLoop hooks

Benefits of FlowODM
-------------------

1. **Type Safety**: Full Pydantic validation and type hints
2. **Less Boilerplate**: Model defines everything in one place
3. **Schema Registry**: Automatic integration with validation
4. **Consumer Loops**: Production-ready patterns out of the box
5. **Settings Profiles**: Optimized configurations for common scenarios
6. **CLI Tools**: Schema management in CI/CD pipelines
