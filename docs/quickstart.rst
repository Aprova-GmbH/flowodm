Quick Start
===========

This guide will help you get started with FlowODM in 5 minutes.

Installation
------------

.. code-block:: bash

   pip install flowodm

For schema generation from Avro files:

.. code-block:: bash

   pip install flowodm[schema-gen]

Prerequisites
-------------

FlowODM requires:

* Python 3.11 or later
* Apache Kafka broker
* Confluent Schema Registry (optional, but recommended)

Basic Setup
-----------

1. **Connect to Kafka**

   .. code-block:: python

      from flowodm import connect

      connect(
          bootstrap_servers="localhost:9092",
          schema_registry_url="http://localhost:8081"
      )

2. **Define a Model**

   .. code-block:: python

      from datetime import datetime
      from flowodm import FlowBaseModel

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

3. **Produce Messages**

   .. code-block:: python

      # Create and produce a message
      order = OrderEvent(
          order_id="ORD-12345",
          customer_id="CUST-001",
          product_name="Laptop",
          quantity=1,
          total_price=999.99,
          created_at=datetime.now()
      )

      # Synchronous produce (blocks until confirmed)
      order.produce_sync()

      # Non-blocking produce
      order.produce()

4. **Consume Messages**

   .. code-block:: python

      # Consume a single message
      event = OrderEvent.consume_one(timeout=5.0)
      if event:
          print(f"Received order: {event.order_id}")

      # Iterate over messages
      for event in OrderEvent.consume_iter(max_messages=100):
          print(f"Processing order: {event.order_id}")

Environment Variables
---------------------

FlowODM can be configured via environment variables:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Variable
     - Description
   * - ``KAFKA_BOOTSTRAP_SERVERS``
     - Kafka broker addresses (e.g., ``localhost:9092``)
   * - ``KAFKA_SECURITY_PROTOCOL``
     - Security protocol (``PLAINTEXT``, ``SSL``, ``SASL_SSL``)
   * - ``KAFKA_SASL_MECHANISM``
     - SASL mechanism (``PLAIN``, ``SCRAM-SHA-256``, ``OAUTHBEARER``)
   * - ``KAFKA_SASL_USERNAME``
     - SASL username
   * - ``KAFKA_SASL_PASSWORD``
     - SASL password
   * - ``SCHEMA_REGISTRY_URL``
     - Schema Registry URL
   * - ``SCHEMA_REGISTRY_API_KEY``
     - Schema Registry API key (for Confluent Cloud)
   * - ``SCHEMA_REGISTRY_API_SECRET``
     - Schema Registry API secret

Next Steps
----------

* :doc:`models` - Learn about model definition and configuration
* :doc:`producing` - Detailed guide to producing messages
* :doc:`consuming` - Detailed guide to consuming messages
* :doc:`consumer_loops` - Build microservices with consumer loops
