CI/CD Integration
=================

FlowODM provides CLI tools and utilities for integrating schema validation
into your CI/CD pipelines.

GitHub Actions Examples
-----------------------

Upload Schemas to Registry
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

   # .github/workflows/schema-upload.yml
   name: Upload Schemas to Registry

   on:
     push:
       branches: [main]
       paths:
         - 'schemas/**/*.avsc'
     workflow_dispatch:
       inputs:
         environment:
           description: 'Target environment'
           required: true
           default: 'staging'
           type: choice
           options:
             - staging
             - production

   jobs:
     upload-schemas:
       runs-on: ubuntu-latest
       environment: ${{ github.event.inputs.environment || 'staging' }}

       steps:
         - uses: actions/checkout@v4

         - name: Set up Python
           uses: actions/setup-python@v5
           with:
             python-version: '3.12'

         - name: Install FlowODM
           run: pip install flowodm

         - name: Upload all schemas
           env:
             SCHEMA_REGISTRY_URL: ${{ secrets.SCHEMA_REGISTRY_URL }}
             SCHEMA_REGISTRY_API_KEY: ${{ secrets.SCHEMA_REGISTRY_API_KEY }}
             SCHEMA_REGISTRY_API_SECRET: ${{ secrets.SCHEMA_REGISTRY_API_SECRET }}
           run: |
             for schema_file in schemas/*.avsc; do
               filename=$(basename "$schema_file" .avsc)
               subject="${filename//_/-}-value"
               echo "Uploading $schema_file as $subject"
               flowodm upload-schema \\
                 --avro "$schema_file" \\
                 --subject "$subject" \\
                 --compatibility BACKWARD
             done

Validate Models on PR
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

   # .github/workflows/schema-validation.yml
   name: Schema Validation

   on:
     push:
       branches: [main, develop]
     pull_request:
       branches: [main]
       paths:
         - 'src/**/*.py'
         - 'schemas/**/*.avsc'

   jobs:
     validate-schemas:
       runs-on: ubuntu-latest

       steps:
         - uses: actions/checkout@v4

         - name: Set up Python
           uses: actions/setup-python@v5
           with:
             python-version: '3.12'

         - name: Install dependencies
           run: |
             pip install flowodm
             pip install -e .

         # Validate against local schemas (no secrets needed)
         - name: Validate against local schemas
           run: |
             flowodm validate \\
               --models myapp.events \\
               --schemas-dir schemas/ \\
               --strict

         # For main branch: also validate against registry
         - name: Validate against Schema Registry
           if: github.ref == 'refs/heads/main'
           env:
             SCHEMA_REGISTRY_URL: ${{ secrets.SCHEMA_REGISTRY_URL }}
             SCHEMA_REGISTRY_API_KEY: ${{ secrets.SCHEMA_REGISTRY_API_KEY }}
             SCHEMA_REGISTRY_API_SECRET: ${{ secrets.SCHEMA_REGISTRY_API_SECRET }}
           run: |
             flowodm validate --models myapp.events --registry
             flowodm check-compatibility --models myapp.events --level BACKWARD

Complete CI Pipeline
^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

   # .github/workflows/ci.yml
   name: CI Pipeline

   on:
     push:
       branches: [main, develop]
     pull_request:
       branches: [main]

   jobs:
     lint:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.12'
         - run: pip install black ruff
         - run: black --check src tests
         - run: ruff check src tests

     test-unit:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.12'
         - run: pip install -e ".[dev]"
         - run: pytest -m unit --cov=myapp

     schema-validation:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.12'
         - run: pip install flowodm && pip install -e .
         - name: Validate schemas
           run: flowodm validate --models myapp.events --schemas-dir schemas/

     test-integration:
       needs: [lint, test-unit, schema-validation]
       runs-on: ubuntu-latest
       services:
         kafka:
           image: confluentinc/cp-kafka:7.5.0
           ports:
             - 9092:9092
         schema-registry:
           image: confluentinc/cp-schema-registry:7.5.0
           ports:
             - 8081:8081
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.12'
         - run: pip install -e ".[dev]"
         - name: Run integration tests
           env:
             KAFKA_BOOTSTRAP_SERVERS: localhost:9092
             SCHEMA_REGISTRY_URL: http://localhost:8081
           run: pytest -m integration

Python Validation Script
------------------------

For custom validation logic:

.. code-block:: python

   #!/usr/bin/env python
   """scripts/validate_schemas.py"""
   import sys
   import argparse
   from flowodm.schema import (
       validate_against_registry,
       validate_against_file,
       check_compatibility,
   )

   # Import your event models
   from myapp.events import (
       UserCreatedEvent,
       OrderPlacedEvent,
       PaymentProcessedEvent,
   )

   MODELS = {
       UserCreatedEvent: {
           "subject": "user-created-value",
           "file": "schemas/user_created.avsc",
       },
       OrderPlacedEvent: {
           "subject": "order-placed-value",
           "file": "schemas/order_placed.avsc",
       },
       PaymentProcessedEvent: {
           "subject": "payment-processed-value",
           "file": "schemas/payment_processed.avsc",
       },
   }

   def validate_all(use_registry: bool, schemas_dir: str = "schemas") -> bool:
       all_valid = True

       for model_class, config in MODELS.items():
           model_name = model_class.__name__

           if use_registry:
               result = validate_against_registry(
                   model_class=model_class,
                   subject=config["subject"],
               )
               source = f"registry '{config['subject']}'"
           else:
               schema_path = f"{schemas_dir}/{config['file'].split('/')[-1]}"
               result = validate_against_file(model_class, schema_path)
               source = f"file '{schema_path}'"

           if result.is_valid:
               print(f"OK {model_name}: Valid against {source}")
           else:
               print(f"FAIL {model_name}: Invalid against {source}")
               for error in result.errors:
                   print(f"   - {error}")
               all_valid = False

       return all_valid

   def main():
       parser = argparse.ArgumentParser()
       parser.add_argument("--registry", action="store_true")
       parser.add_argument("--schemas-dir", default="schemas")
       args = parser.parse_args()

       if validate_all(args.registry, args.schemas_dir):
           print("All validations passed!")
           sys.exit(0)
       else:
           print("Some validations failed!")
           sys.exit(1)

   if __name__ == "__main__":
       main()

Pre-commit Hook
---------------

Add schema validation as a pre-commit hook:

.. code-block:: yaml

   # .pre-commit-config.yaml
   repos:
     - repo: local
       hooks:
         - id: validate-schemas
           name: Validate Kafka schemas
           entry: python scripts/validate_schemas.py --schemas-dir schemas/
           language: system
           files: '(schemas/.*\\.avsc|src/.*events.*\\.py)$'
           pass_filenames: false

Required Secrets
----------------

For GitHub Actions with Confluent Cloud:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Secret
     - Description
   * - ``SCHEMA_REGISTRY_URL``
     - Schema Registry endpoint
   * - ``SCHEMA_REGISTRY_API_KEY``
     - API key for Confluent Cloud
   * - ``SCHEMA_REGISTRY_API_SECRET``
     - API secret for Confluent Cloud

For self-hosted Schema Registry without auth, only ``SCHEMA_REGISTRY_URL`` is needed.

CLI Reference
-------------

**validate**

.. code-block:: bash

   flowodm validate --models myapp.events --registry
   flowodm validate --models myapp.events --schemas-dir schemas/
   flowodm validate --models myapp.events.UserEvent --registry --strict

**check-compatibility**

.. code-block:: bash

   flowodm check-compatibility \\
       --models myapp.events \\
       --level BACKWARD

   flowodm check-compatibility \\
       --subject user-events-value \\
       --model myapp.events.UserEvent

**upload-schema**

.. code-block:: bash

   flowodm upload-schema \\
       --avro schemas/user_event.avsc \\
       --subject user-events-value \\
       --compatibility BACKWARD

**list-subjects**

.. code-block:: bash

   flowodm list-subjects

**get-schema**

.. code-block:: bash

   flowodm get-schema --subject user-events-value
   flowodm get-schema --subject user-events-value --version 2
