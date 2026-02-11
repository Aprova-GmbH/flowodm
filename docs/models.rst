Model Definition
================

FlowODM models are defined by inheriting from ``FlowBaseModel``, which extends
Pydantic's ``BaseModel`` with Kafka-specific functionality.

Basic Model
-----------

.. code-block:: python

   from datetime import datetime
   from flowodm import FlowBaseModel

   class UserEvent(FlowBaseModel):
       class Settings:
           topic = "user-events"

       user_id: str
       action: str
       timestamp: datetime

Settings Class
--------------

The inner ``Settings`` class configures Kafka-specific behavior:

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Attribute
     - Required
     - Description
   * - ``topic``
     - Yes
     - Kafka topic name for produce/consume operations
   * - ``schema_subject``
     - No
     - Schema Registry subject name (defaults to ``{topic}-value``)
   * - ``schema_path``
     - No
     - Path to local Avro schema file
   * - ``consumer_group``
     - No
     - Consumer group ID for consumption
   * - ``key_field``
     - No
     - Field name to use as message key
   * - ``confluent_wire_format``
     - No
     - Prepend Confluent wire format header when serializing (default ``True``)

Example with all settings:

.. code-block:: python

   class OrderEvent(FlowBaseModel):
       class Settings:
           topic = "orders"
           schema_subject = "orders-value-v1"
           schema_path = "schemas/order.avsc"
           consumer_group = "order-processor"
           key_field = "order_id"

       order_id: str
       customer_id: str
       total: float

Confluent Wire Format
---------------------

By default, FlowODM prepends the `Confluent wire format
<https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>`_
header to serialized messages when a Schema Registry is configured. This header
consists of a magic byte (``0x00``) followed by a 4-byte big-endian schema ID,
making messages compatible with standard Confluent consumers, Java's
``KafkaAvroDeserializer``, and Kafka UI tools like AKHQ and Kafdrop.

When no Schema Registry is configured, messages are serialized as raw Avro bytes.

To disable the wire format header (e.g., for custom consumers that expect raw Avro):

.. code-block:: python

   class RawAvroEvent(FlowBaseModel):
       class Settings:
           topic = "raw-events"
           confluent_wire_format = False  # Produce raw Avro bytes

       event_id: str
       payload: str

Optional Fields
---------------

Use Python's optional type syntax:

.. code-block:: python

   class UserProfile(FlowBaseModel):
       class Settings:
           topic = "profiles"

       user_id: str
       name: str
       email: str | None = None  # Optional field
       age: int | None = None    # Optional field
       verified: bool = False    # Default value

Complex Types
-------------

FlowODM supports nested models and complex types:

.. code-block:: python

   from pydantic import BaseModel
   from typing import list

   class Address(BaseModel):
       street: str
       city: str
       country: str

   class Customer(FlowBaseModel):
       class Settings:
           topic = "customers"

       customer_id: str
       name: str
       addresses: list[Address]
       tags: list[str]
       metadata: dict[str, str]

Validation
----------

Pydantic validation works seamlessly:

.. code-block:: python

   from pydantic import Field, field_validator

   class Product(FlowBaseModel):
       class Settings:
           topic = "products"

       product_id: str
       name: str = Field(min_length=1, max_length=200)
       price: float = Field(gt=0)
       quantity: int = Field(ge=0)

       @field_validator("product_id")
       @classmethod
       def validate_product_id(cls, v: str) -> str:
           if not v.startswith("PROD-"):
               raise ValueError("Product ID must start with 'PROD-'")
           return v

Serialization
-------------

Models are serialized to Avro format when producing and deserialized when consuming.
The Avro schema is automatically generated from the Pydantic model or loaded from
Schema Registry.

.. code-block:: python

   event = UserEvent(user_id="123", action="login", timestamp=datetime.now())

   # Get the Avro-compatible dict
   avro_dict = event.to_avro_dict()

   # Get the JSON representation
   json_str = event.model_dump_json()
