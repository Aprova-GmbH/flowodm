"""
Command-line interface for FlowODM schema operations.

Provides commands for:
- Validating models against schemas
- Uploading schemas to Schema Registry
- Checking schema compatibility
- Listing schema subjects
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path
from typing import Any

from flowodm.exceptions import FlowODMError


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="flowodm",
        description="FlowODM - CLI for Kafka schema operations",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Validate command
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate models against schemas",
    )
    validate_parser.add_argument(
        "--models",
        required=True,
        help="Python module containing FlowBaseModel classes (e.g., myapp.events)",
    )
    validate_parser.add_argument(
        "--registry",
        action="store_true",
        help="Validate against Schema Registry",
    )
    validate_parser.add_argument(
        "--schemas-dir",
        help="Directory containing .avsc files",
    )
    validate_parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with error on any validation failure",
    )

    # Upload schema command
    upload_parser = subparsers.add_parser(
        "upload-schema",
        help="Upload Avro schema to Schema Registry",
    )
    upload_parser.add_argument(
        "--avro",
        required=True,
        help="Path to .avsc file",
    )
    upload_parser.add_argument(
        "--subject",
        required=True,
        help="Schema Registry subject name",
    )
    upload_parser.add_argument(
        "--compatibility",
        choices=["BACKWARD", "FORWARD", "FULL", "NONE"],
        help="Set compatibility level",
    )

    # Check compatibility command
    compat_parser = subparsers.add_parser(
        "check-compatibility",
        help="Check schema compatibility",
    )
    compat_parser.add_argument(
        "--models",
        help="Python module containing FlowBaseModel classes",
    )
    compat_parser.add_argument(
        "--model",
        help="Specific model class (e.g., myapp.events.UserEvent)",
    )
    compat_parser.add_argument(
        "--subject",
        help="Schema Registry subject name",
    )
    compat_parser.add_argument(
        "--level",
        default="BACKWARD",
        choices=["BACKWARD", "FORWARD", "FULL", "NONE"],
        help="Compatibility level to check",
    )

    # List subjects command
    subparsers.add_parser(
        "list-subjects",
        help="List all Schema Registry subjects",
    )

    # Get schema command
    get_parser = subparsers.add_parser(
        "get-schema",
        help="Get schema from Schema Registry",
    )
    get_parser.add_argument(
        "--subject",
        required=True,
        help="Schema Registry subject name",
    )
    get_parser.add_argument(
        "--version",
        default="latest",
        help="Schema version (default: latest)",
    )
    get_parser.add_argument(
        "--output",
        help="Output file path (prints to stdout if not specified)",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    try:
        if args.command == "validate":
            cmd_validate(args)
        elif args.command == "upload-schema":
            cmd_upload_schema(args)
        elif args.command == "check-compatibility":
            cmd_check_compatibility(args)
        elif args.command == "list-subjects":
            cmd_list_subjects(args)
        elif args.command == "get-schema":
            cmd_get_schema(args)
        else:
            parser.print_help()
            sys.exit(1)
    except FlowODMError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_validate(args: argparse.Namespace) -> None:
    """Validate models against schemas."""
    from flowodm.schema import validate_against_file, validate_against_registry

    # Import the module
    models = _load_models_from_module(args.models)

    if not models:
        print(f"No FlowBaseModel classes found in {args.models}")
        sys.exit(1)

    print(f"Found {len(models)} model(s) to validate")
    print("=" * 60)

    all_valid = True

    for model_class in models:
        model_name = model_class.__name__
        subject = model_class._get_schema_subject()  # type: ignore[attr-defined]

        if args.registry:
            # Validate against Schema Registry
            print(f"\nValidating {model_name} against registry subject '{subject}'...")
            result = validate_against_registry(model_class, subject)
            source = f"registry:{subject}"
        elif args.schemas_dir:
            # Validate against local file
            schema_path = Path(args.schemas_dir) / f"{subject.replace('-value', '')}.avsc"
            if not schema_path.exists():
                # Try alternative naming
                schema_path = Path(args.schemas_dir) / f"{model_name.lower()}.avsc"

            if not schema_path.exists():
                print(f"  Schema file not found for {model_name}")
                all_valid = False
                continue

            print(f"\nValidating {model_name} against file '{schema_path}'...")
            result = validate_against_file(model_class, schema_path)
            source = str(schema_path)
        else:
            print("Error: Must specify --registry or --schemas-dir")
            sys.exit(1)

        if result.is_valid:
            print(f"  ✅ {model_name}: Valid against {source}")
        else:
            print(f"  ❌ {model_name}: INVALID")
            for error in result.errors:
                print(f"     - {error}")
            all_valid = False

        if result.warnings:
            for warning in result.warnings:
                print(f"     ⚠️  {warning}")

    print("\n" + "=" * 60)
    if all_valid:
        print("✅ All validations passed!")
    else:
        print("❌ Some validations failed!")
        if args.strict:
            sys.exit(1)


def cmd_upload_schema(args: argparse.Namespace) -> None:
    """Upload schema to Schema Registry."""
    from flowodm.schema import upload_schema

    print(f"Uploading {args.avro} as subject '{args.subject}'...")

    schema_id = upload_schema(
        schema_path=args.avro,
        subject=args.subject,
        compatibility_level=args.compatibility,
    )

    print(f"✅ Schema uploaded successfully (ID: {schema_id})")


def cmd_check_compatibility(args: argparse.Namespace) -> None:
    """Check schema compatibility."""
    from flowodm.schema import check_compatibility

    if args.model:
        # Single model
        model_class = _load_model_class(args.model)
        subject = args.subject or model_class._get_schema_subject()

        print(f"Checking {args.level} compatibility for {model_class.__name__}...")
        result = check_compatibility(model_class, subject, args.level)

        if result.is_compatible:
            print(f"✅ {model_class.__name__}: {args.level} compatible")
        else:
            print(f"❌ {model_class.__name__}: NOT {args.level} compatible")
            print(f"   {result.message}")
            sys.exit(1)

    elif args.models:
        # Multiple models
        models = _load_models_from_module(args.models)
        all_compatible = True

        print(f"Checking {args.level} compatibility for {len(models)} model(s)...")
        print("=" * 60)

        for model_class in models:
            subject = model_class._get_schema_subject()  # type: ignore[attr-defined]

            try:
                result = check_compatibility(model_class, subject, args.level)

                if result.is_compatible:
                    print(f"  ✅ {model_class.__name__}: {args.level} compatible")
                else:
                    print(f"  ❌ {model_class.__name__}: NOT {args.level} compatible")
                    print(f"     {result.message}")
                    all_compatible = False
            except Exception as e:
                print(f"  ⚠️  {model_class.__name__}: Could not check ({e})")

        print("=" * 60)
        if not all_compatible:
            sys.exit(1)
    else:
        print("Error: Must specify --model or --models")
        sys.exit(1)


def cmd_list_subjects(args: argparse.Namespace) -> None:
    """List Schema Registry subjects."""
    from flowodm.schema import list_subjects

    subjects = list_subjects()

    if not subjects:
        print("No subjects found in Schema Registry")
        return

    print(f"Found {len(subjects)} subject(s):")
    for subject in sorted(subjects):
        print(f"  - {subject}")


def cmd_get_schema(args: argparse.Namespace) -> None:
    """Get schema from Schema Registry."""
    import json

    from flowodm.schema import load_schema_from_registry

    schema = load_schema_from_registry(args.subject, args.version)
    schema_json = json.dumps(schema, indent=2)

    if args.output:
        with open(args.output, "w") as f:
            f.write(schema_json)
        print(f"Schema written to {args.output}")
    else:
        print(schema_json)


def _load_models_from_module(module_path: str) -> list[type]:
    """Load all FlowBaseModel subclasses from a module."""
    from flowodm.model import FlowBaseModel

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        print(f"Error: Could not import module '{module_path}': {e}")
        sys.exit(1)

    models = []
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and issubclass(obj, FlowBaseModel) and obj is not FlowBaseModel:
            # Check if it has a valid Settings class
            try:
                obj._get_topic()
                models.append(obj)
            except Exception:
                pass  # Skip models without valid Settings

    return models


def _load_model_class(class_path: str) -> Any:
    """Load a specific model class by full path."""
    from flowodm.model import FlowBaseModel

    parts = class_path.rsplit(".", 1)
    if len(parts) != 2:
        print(f"Error: Invalid class path '{class_path}'. Use format 'module.ClassName'")
        sys.exit(1)

    module_path, class_name = parts

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        print(f"Error: Could not import module '{module_path}': {e}")
        sys.exit(1)

    if not hasattr(module, class_name):
        print(f"Error: Class '{class_name}' not found in module '{module_path}'")
        sys.exit(1)

    model_class = getattr(module, class_name)

    if not issubclass(model_class, FlowBaseModel):
        print(f"Error: '{class_path}' is not a FlowBaseModel subclass")
        sys.exit(1)

    return model_class


if __name__ == "__main__":
    main()
