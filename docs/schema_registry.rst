Schema Registry
===============

FlowODM integrates with Confluent Schema Registry for schema management,
validation, and evolution.

Connection
----------

**Local Schema Registry**

.. code-block:: python

   from flowodm import connect

   connect(
       bootstrap_servers="localhost:9092",
       schema_registry_url="http://localhost:8081"
   )

**Confluent Cloud**

.. code-block:: python

   connect(
       bootstrap_servers="pkc-xxx.us-east-2.aws.confluent.cloud:9092",
       schema_registry_url="https://psrc-xxx.us-east-2.aws.confluent.cloud",
       security_protocol="SASL_SSL",
       sasl_mechanism="PLAIN",
       sasl_username="YOUR_API_KEY",
       sasl_password="YOUR_API_SECRET",
       schema_registry_config={
           "basic.auth.credentials.source": "USER_INFO",
           "basic.auth.user.info": "SR_API_KEY:SR_API_SECRET",
       }
   )

**Environment Variables**

.. code-block:: bash

   export KAFKA_BOOTSTRAP_SERVERS="pkc-xxx.us-east-2.aws.confluent.cloud:9092"
   export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
   export KAFKA_SASL_MECHANISM="PLAIN"
   export KAFKA_SASL_USERNAME="YOUR_API_KEY"
   export KAFKA_SASL_PASSWORD="YOUR_API_SECRET"
   export SCHEMA_REGISTRY_URL="https://psrc-xxx.us-east-2.aws.confluent.cloud"
   # Option 1: Combined format (recommended for native confluent-kafka compatibility)
   export SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="SR_API_KEY:SR_API_SECRET"
   # Option 2: Separate key/secret (FlowODM will combine them)
   # export SCHEMA_REGISTRY_API_KEY="SR_API_KEY"
   # export SCHEMA_REGISTRY_API_SECRET="SR_API_SECRET"

.. code-block:: python

   from flowodm import connect
   connect()  # Uses environment variables

Schema Validation
-----------------

**Validate Model Against Registry**

.. code-block:: python

   from flowodm.schema import validate_against_registry

   class UserEvent(FlowBaseModel):
       class Settings:
           topic = "user-events"

       user_id: str
       action: str

   result = validate_against_registry(
       model_class=UserEvent,
       subject="user-events-value",
       version="latest"  # or specific version number
   )

   if result.is_valid:
       print("Model matches schema!")
   else:
       print("Schema mismatch:")
       for error in result.errors:
           print(f"  - {error}")

**Validate Against Local File**

.. code-block:: python

   from flowodm.schema import validate_against_file

   result = validate_against_file(UserEvent, "schemas/user_event.avsc")

   if not result.is_valid:
       for error in result.errors:
           print(f"  - {error}")

Compatibility Checking
----------------------

Check schema compatibility before deploying changes:

.. code-block:: python

   from flowodm.schema import check_compatibility

   result = check_compatibility(
       model_class=UserEvent,
       subject="user-events-value",
       compatibility_level="BACKWARD"  # BACKWARD, FORWARD, FULL, NONE
   )

   if result.is_compatible:
       print("Schema change is backward compatible!")
   else:
       print(f"Incompatible: {result.message}")

Model Generation
----------------

**From Schema Registry**

.. code-block:: python

   from flowodm.schema import generate_model_from_registry

   # Generate a Pydantic model from a registered schema
   UserEvent = generate_model_from_registry(
       subject="user-events-value",
       topic="user-events",
       version="latest"
   )

   # Use the generated model
   for event in UserEvent.consume_iter():
       print(event)

**From Avro File**

.. code-block:: python

   from flowodm.schema import generate_model_from_schema

   # Generate from local .avsc file
   UserEvent = generate_model_from_schema(
       schema_path="schemas/user_event.avsc",
       topic="user-events"
   )

Schema Upload
-------------

Upload schemas to the registry:

.. code-block:: python

   from flowodm.schema import upload_schema

   # Upload from file
   upload_schema(
       schema_path="schemas/user_event.avsc",
       subject="user-events-value",
       compatibility="BACKWARD"
   )

Registry Operations
-------------------

**List Subjects**

.. code-block:: python

   from flowodm.schema import list_subjects

   subjects = list_subjects()
   for subject in subjects:
       print(subject)

**Get Schema Versions**

.. code-block:: python

   from flowodm.schema import get_schema_versions

   versions = get_schema_versions("user-events-value")
   print(f"Available versions: {versions}")

**Delete Subject**

.. code-block:: python

   from flowodm.schema import delete_subject

   # Soft delete (marks as deleted)
   delete_subject("user-events-value")

   # Permanent delete
   delete_subject("user-events-value", permanent=True)

CLI Tools
---------

FlowODM provides CLI commands for schema operations:

.. code-block:: bash

   # Validate models against Schema Registry
   flowodm validate --models myapp.events --registry

   # Validate against local files
   flowodm validate --models myapp.events --schemas-dir schemas/

   # Check compatibility
   flowodm check-compatibility \\
       --subject user-events-value \\
       --model myapp.events.UserEvent

   # Upload schema
   flowodm upload-schema \\
       --avro schemas/user_event.avsc \\
       --subject user-events-value

   # List subjects
   flowodm list-subjects

   # Get schema
   flowodm get-schema --subject user-events-value

See :doc:`ci_cd` for CI/CD integration examples.

Schema Subject Naming
---------------------

By default, FlowODM uses the topic name with ``-value`` suffix:

.. code-block:: python

   class UserEvent(FlowBaseModel):
       class Settings:
           topic = "user-events"
           # schema_subject defaults to "user-events-value"

Override with explicit subject:

.. code-block:: python

   class UserEvent(FlowBaseModel):
       class Settings:
           topic = "user-events"
           schema_subject = "my-custom-subject"

Schema Evolution
----------------

Follow Avro schema evolution rules:

**Backward Compatible Changes** (consumers can read old data):

* Add optional fields with defaults
* Remove fields

**Forward Compatible Changes** (producers can write new data):

* Remove optional fields
* Add fields

**Full Compatible Changes**:

* Add optional fields with defaults
* Remove optional fields with defaults

Example of a backward-compatible change:

.. code-block:: python

   # v1
   class UserEvent(FlowBaseModel):
       class Settings:
           topic = "user-events"

       user_id: str
       action: str

   # v2 - Added optional field (backward compatible)
   class UserEvent(FlowBaseModel):
       class Settings:
           topic = "user-events"

       user_id: str
       action: str
       metadata: dict[str, str] | None = None  # New optional field
