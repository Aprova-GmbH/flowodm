.. image:: logo-web.png
   :align: center
   :width: 500px
   :alt: FlowODM Logo

FlowODM Documentation
=====================

**FlowODM** is a lightweight ODM (Object-Document Mapper) for Apache Kafka with Avro schema support.
It provides a Pydantic-based interface for working with Kafka messages and Avro schemas.

.. note::

   Apache Kafka is a registered trademark of the Apache Software Foundation.

Features
--------

* **Pydantic v2 Integration**: Define message models with full type safety and validation
* **Sync and Async Support**: Both synchronous and asynchronous APIs for produce/consume
* **Schema Registry Integration**: Automatic schema registration and validation
* **Consumer Loop Patterns**: Ready-to-use patterns for building Kafka microservices
* **Predefined Settings Profiles**: Optimized configurations for different use cases

Quick Example
-------------

.. code-block:: python

   from datetime import datetime
   from flowodm import FlowBaseModel, connect

   # Connect to Kafka
   connect(
       bootstrap_servers="localhost:9092",
       schema_registry_url="http://localhost:8081"
   )

   # Define a model
   class UserEvent(FlowBaseModel):
       class Settings:
           topic = "user-events"
           consumer_group = "my-service"

       user_id: str
       action: str
       timestamp: datetime

   # Produce messages
   event = UserEvent(user_id="123", action="login", timestamp=datetime.now())
   event.produce()

   # Consume messages
   for event in UserEvent.consume_iter():
       print(f"User {event.user_id} performed {event.action}")

Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   quickstart
   models
   producing
   consuming
   consumer_loops
   schema_registry
   settings_profiles
   ci_cd

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api

.. toctree::
   :maxdepth: 1
   :caption: Project Info

   changelog
   migration


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
