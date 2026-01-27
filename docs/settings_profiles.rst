Settings Profiles
=================

FlowODM provides predefined settings profiles optimized for common use cases.

Available Profiles
------------------

.. list-table::
   :header-rows: 1
   :widths: 20 15 15 15 35

   * - Profile
     - Session Timeout
     - Max Poll Interval
     - Max Poll Records
     - Use Case
   * - ``LongRunningSettings``
     - 5 min
     - 10 min
     - 100
     - ML inference, complex processing
   * - ``BatchSettings``
     - 45 sec
     - 5 min
     - 500
     - ETL, data aggregation
   * - ``RealTimeSettings``
     - 10 sec
     - 30 sec
     - 10
     - Event-driven, notifications
   * - ``HighThroughputSettings``
     - 30 sec
     - 5 min
     - 1000
     - High-volume batch processing
   * - ``ReliableSettings``
     - 45 sec
     - 5 min
     - 100
     - At-least-once with manual commit

LongRunningSettings
-------------------

For tasks that take significant time to process:

.. code-block:: python

   from flowodm import ConsumerLoop, LongRunningSettings

   loop = ConsumerLoop(
       model=MLPredictionRequest,
       handler=run_ml_inference,
       settings=LongRunningSettings(),
   )

Configuration:

* ``session.timeout.ms``: 300000 (5 minutes)
* ``max.poll.interval.ms``: 600000 (10 minutes)
* ``max.poll.records``: 100

Best for:

* Machine learning inference
* Complex data transformations
* External API calls with rate limits
* Database migrations

BatchSettings
-------------

For processing large batches efficiently:

.. code-block:: python

   from flowodm import ConsumerLoop, BatchSettings

   loop = ConsumerLoop(
       model=DataRecord,
       handler=process_batch,
       settings=BatchSettings(),
   )

Configuration:

* ``session.timeout.ms``: 45000 (45 seconds)
* ``max.poll.interval.ms``: 300000 (5 minutes)
* ``max.poll.records``: 500

Best for:

* ETL pipelines
* Data aggregation
* Bulk database operations
* Report generation

RealTimeSettings
----------------

For low-latency, real-time processing:

.. code-block:: python

   from flowodm import ConsumerLoop, RealTimeSettings

   loop = ConsumerLoop(
       model=NotificationEvent,
       handler=send_notification,
       settings=RealTimeSettings(),
   )

Configuration:

* ``session.timeout.ms``: 10000 (10 seconds)
* ``max.poll.interval.ms``: 30000 (30 seconds)
* ``max.poll.records``: 10

Best for:

* Push notifications
* Real-time alerts
* Event-driven workflows
* Low-latency responses

HighThroughputSettings
----------------------

For maximum throughput with large batches:

.. code-block:: python

   from flowodm import ConsumerLoop, HighThroughputSettings

   loop = ConsumerLoop(
       model=LogEvent,
       handler=process_logs,
       settings=HighThroughputSettings(),
   )

Configuration:

* ``session.timeout.ms``: 30000 (30 seconds)
* ``max.poll.interval.ms``: 300000 (5 minutes)
* ``max.poll.records``: 1000

Best for:

* Log processing
* Metrics aggregation
* High-volume data ingestion

ReliableSettings
----------------

For at-least-once processing with manual commit:

.. code-block:: python

   from flowodm import ConsumerLoop, ReliableSettings

   loop = ConsumerLoop(
       model=PaymentEvent,
       handler=process_payment,
       settings=ReliableSettings(),
   )

Configuration:

* ``session.timeout.ms``: 45000 (45 seconds)
* ``max.poll.interval.ms``: 300000 (5 minutes)
* ``max.poll.records``: 100
* ``enable.auto.commit``: False

Best for:

* Financial transactions
* Critical data pipelines
* Audit logs
* Order processing

Custom Settings
---------------

Create custom settings by extending ``BaseSettings``:

.. code-block:: python

   from flowodm.settings import BaseSettings

   class MyCustomSettings(BaseSettings):
       session_timeout_ms: int = 60000
       max_poll_interval_ms: int = 120000
       max_poll_records: int = 50

       # Add custom configuration
       enable_auto_commit: bool = False

       def to_consumer_config(self) -> dict:
           config = super().to_consumer_config()
           config["enable.auto.commit"] = self.enable_auto_commit
           return config

   loop = ConsumerLoop(
       model=MyEvent,
       handler=my_handler,
       settings=MyCustomSettings(),
   )

Using with connect()
--------------------

Settings can also be applied globally:

.. code-block:: python

   from flowodm import connect
   from flowodm.settings import LongRunningSettings

   settings = LongRunningSettings()

   connect(
       bootstrap_servers="localhost:9092",
       consumer_config=settings.to_consumer_config(),
   )

Understanding the Settings
--------------------------

**session.timeout.ms**

How long the broker waits for a heartbeat before considering the consumer dead.
If your processing takes longer than this, the consumer will be removed from the group.

**max.poll.interval.ms**

Maximum time between ``poll()`` calls. If processing a batch takes longer than this,
the consumer will be removed from the group.

**max.poll.records**

Maximum number of records returned in a single ``poll()`` call. Larger values
increase throughput but also increase memory usage and processing time per batch.

Choosing the Right Profile
--------------------------

.. code-block:: text

   Is latency critical? (< 1 second response)
   └── Yes → RealTimeSettings
   └── No
       └── Is processing > 1 minute per message?
           └── Yes → LongRunningSettings
           └── No
               └── Is throughput the priority?
                   └── Yes → HighThroughputSettings or BatchSettings
                   └── No
                       └── Is reliability critical?
                           └── Yes → ReliableSettings
                           └── No → BatchSettings (default)
