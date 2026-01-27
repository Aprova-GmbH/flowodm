"""
Schema utilities for Avro schema generation, validation, and registry operations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Type

from flowodm.connection import get_schema_registry
from flowodm.exceptions import (
    SchemaCompatibilityError,
    SchemaRegistryError,
    SchemaValidationError,
)

if TYPE_CHECKING:
    from flowodm.model import FlowBaseModel


@dataclass
class ValidationResult:
    """Result of schema validation."""

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class CompatibilityResult:
    """Result of schema compatibility check."""

    is_compatible: bool
    message: str = ""
    compatibility_level: str = ""


def load_schema_from_file(path: str | Path) -> dict[str, Any]:
    """
    Load Avro schema from .avsc file.

    Args:
        path: Path to .avsc file

    Returns:
        Parsed Avro schema as dict

    Raises:
        FileNotFoundError: If schema file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
    """
    path = Path(path)
    with open(path) as f:
        return json.load(f)


def load_schema_from_registry(
    subject: str,
    version: str | int = "latest",
) -> dict[str, Any]:
    """
    Load Avro schema from Schema Registry.

    Args:
        subject: Schema Registry subject name
        version: Schema version ("latest" or version number)

    Returns:
        Parsed Avro schema as dict

    Raises:
        SchemaRegistryError: If schema cannot be retrieved
    """
    try:
        registry = get_schema_registry()

        if version == "latest":
            schema_version = registry.get_latest_version(subject)
        else:
            schema_version = registry.get_version(subject, int(version))

        return json.loads(schema_version.schema.schema_str)

    except Exception as e:
        raise SchemaRegistryError(f"Failed to load schema from registry: {e}") from e


def generate_model_from_schema(
    schema_source: str | Path | dict[str, Any],
    topic: str,
    class_name: str | None = None,
    consumer_group: str | None = None,
) -> Type[FlowBaseModel]:
    """
    Generate a FlowBaseModel subclass from an Avro schema.

    Args:
        schema_source: Path to .avsc file, schema dict, or Schema Registry subject
        topic: Kafka topic name
        class_name: Optional class name (defaults to schema record name)
        consumer_group: Optional consumer group

    Returns:
        Dynamically generated FlowBaseModel subclass

    Example:
        >>> UserEvent = generate_model_from_schema("schemas/user.avsc", topic="users")
        >>> event = UserEvent(user_id="123", action="login")
    """
    from pydantic import create_model

    from flowodm.model import FlowBaseModel

    # Load schema
    if isinstance(schema_source, dict):
        schema = schema_source
    elif isinstance(schema_source, (str, Path)):
        path = Path(schema_source)
        if path.exists() and path.suffix == ".avsc":
            schema = load_schema_from_file(path)
        else:
            # Treat as Schema Registry subject
            schema = load_schema_from_registry(str(schema_source))
    else:
        raise ValueError(f"Invalid schema source: {schema_source}")

    # Extract class name
    model_name = class_name or schema.get("name", "GeneratedModel")

    # Convert Avro fields to Pydantic field definitions
    field_definitions: dict[str, Any] = {}

    for avro_field in schema.get("fields", []):
        field_name = avro_field["name"]
        avro_type = avro_field["type"]
        python_type = _avro_type_to_python(avro_type)
        default = avro_field.get("default", ...)

        if default is ...:
            field_definitions[field_name] = (python_type, ...)
        else:
            field_definitions[field_name] = (python_type, default)

    # Create Settings class
    class GeneratedSettings:
        pass

    GeneratedSettings.topic = topic
    GeneratedSettings.consumer_group = consumer_group

    # Create model class
    model = create_model(
        model_name,
        __base__=FlowBaseModel,
        **field_definitions,
    )

    # Attach Settings
    model.Settings = GeneratedSettings  # type: ignore

    return model


def generate_model_from_registry(
    subject: str,
    topic: str,
    version: str | int = "latest",
    class_name: str | None = None,
    consumer_group: str | None = None,
) -> Type[FlowBaseModel]:
    """
    Generate a FlowBaseModel subclass from Schema Registry.

    Args:
        subject: Schema Registry subject name
        topic: Kafka topic name
        version: Schema version ("latest" or version number)
        class_name: Optional class name
        consumer_group: Optional consumer group

    Returns:
        Dynamically generated FlowBaseModel subclass
    """
    schema = load_schema_from_registry(subject, version)
    return generate_model_from_schema(
        schema,
        topic=topic,
        class_name=class_name,
        consumer_group=consumer_group,
    )


def _avro_type_to_python(avro_type: Any) -> type:
    """Convert Avro type to Python type annotation."""
    # Handle union types (nullable)
    if isinstance(avro_type, list):
        # Find non-null type
        non_null_types = [t for t in avro_type if t != "null"]
        if not non_null_types:
            return type(None)
        if len(non_null_types) == 1:
            return _avro_type_to_python(non_null_types[0]) | None
        # Multiple types - use Any
        return Any

    # Handle complex types
    if isinstance(avro_type, dict):
        logical_type = avro_type.get("logicalType")
        base_type = avro_type.get("type")

        if logical_type == "timestamp-millis":
            from datetime import datetime

            return datetime
        if logical_type == "date":
            from datetime import date

            return date
        if logical_type == "decimal":
            from decimal import Decimal

            return Decimal
        if logical_type == "uuid":
            return str

        # Recurse for base type
        if base_type:
            return _avro_type_to_python(base_type)

        return Any

    # Handle primitive types
    type_mapping: dict[str, type] = {
        "null": type(None),
        "boolean": bool,
        "int": int,
        "long": int,
        "float": float,
        "double": float,
        "bytes": bytes,
        "string": str,
    }

    return type_mapping.get(avro_type, Any)


def validate_against_file(
    model_class: Type[FlowBaseModel],
    schema_path: str | Path,
) -> ValidationResult:
    """
    Validate that a Pydantic model matches a local Avro schema file.

    Args:
        model_class: FlowBaseModel subclass to validate
        schema_path: Path to .avsc file

    Returns:
        ValidationResult with is_valid flag and any errors
    """
    schema = load_schema_from_file(schema_path)
    return _validate_model_against_schema(model_class, schema)


def validate_against_registry(
    model_class: Type[FlowBaseModel],
    subject: str,
    version: str | int = "latest",
) -> ValidationResult:
    """
    Validate that a Pydantic model matches a Schema Registry schema.

    Args:
        model_class: FlowBaseModel subclass to validate
        subject: Schema Registry subject name
        version: Schema version ("latest" or version number)

    Returns:
        ValidationResult with is_valid flag and any errors
    """
    schema = load_schema_from_registry(subject, version)
    return _validate_model_against_schema(model_class, schema)


def _validate_model_against_schema(
    model_class: Type[FlowBaseModel],
    schema: dict[str, Any],
) -> ValidationResult:
    """
    Internal function to validate model against schema.

    Checks:
    1. All required schema fields exist in model
    2. All model fields exist in schema (no extra fields)
    3. Field types are compatible
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Get model fields
    model_fields = set(model_class.model_fields.keys())

    # Get schema fields
    schema_fields_list = schema.get("fields", [])
    schema_fields = {f["name"]: f for f in schema_fields_list}
    schema_field_names = set(schema_fields.keys())

    # Check for missing fields (in schema but not in model)
    missing_fields = schema_field_names - model_fields
    for field_name in missing_fields:
        field_info = schema_fields[field_name]
        is_nullable = _is_nullable_avro_type(field_info["type"])
        has_default = "default" in field_info

        if not is_nullable and not has_default:
            errors.append(f"Missing required field '{field_name}' from schema")
        else:
            warnings.append(f"Missing optional field '{field_name}' from schema")

    # Check for extra fields (in model but not in schema)
    # Exclude message_id which is FlowBaseModel's internal field
    extra_fields = model_fields - schema_field_names - {"message_id"}
    for field_name in extra_fields:
        errors.append(f"Extra field '{field_name}' not in schema")

    # Check type compatibility for common fields
    common_fields = model_fields & schema_field_names
    for field_name in common_fields:
        model_field = model_class.model_fields[field_name]
        schema_field = schema_fields[field_name]

        if not _types_compatible(model_field.annotation, schema_field["type"]):
            errors.append(
                f"Field '{field_name}' type mismatch: "
                f"model has {model_field.annotation}, schema expects {schema_field['type']}"
            )

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def _is_nullable_avro_type(avro_type: Any) -> bool:
    """Check if Avro type is nullable (union with null)."""
    if isinstance(avro_type, list):
        return "null" in avro_type
    return avro_type == "null"


def _types_compatible(python_type: Any, avro_type: Any) -> bool:
    """Check if Python type is compatible with Avro type."""
    # Handle optional types
    origin = getattr(python_type, "__origin__", None)
    if origin is type(None):
        return _is_nullable_avro_type(avro_type)

    # Handle Union types (Optional)
    if hasattr(python_type, "__args__"):
        args = python_type.__args__
        if type(None) in args:
            # It's Optional[X], check the non-None type
            non_none_types = [a for a in args if a is not type(None)]
            if non_none_types:
                return _types_compatible(non_none_types[0], avro_type)

    # Get expected Python type from Avro
    expected_python_type = _avro_type_to_python(avro_type)

    # Handle Optional expected types
    if hasattr(expected_python_type, "__origin__"):
        args = getattr(expected_python_type, "__args__", ())
        if type(None) in args:
            non_none_types = [a for a in args if a is not type(None)]
            if non_none_types:
                expected_python_type = non_none_types[0]

    # Simple type comparison
    if python_type == expected_python_type:
        return True

    # Check for compatible numeric types
    if python_type in (int, float) and expected_python_type in (int, float):
        return True

    # Fallback - be permissive
    return expected_python_type == Any


def check_compatibility(
    model_class: Type[FlowBaseModel],
    subject: str,
    compatibility_level: str = "BACKWARD",
) -> CompatibilityResult:
    """
    Check if model's schema is compatible with existing registry schemas.

    Args:
        model_class: FlowBaseModel subclass
        subject: Schema Registry subject name
        compatibility_level: BACKWARD, FORWARD, FULL, or NONE

    Returns:
        CompatibilityResult with is_compatible flag and message
    """
    try:
        from confluent_kafka.schema_registry import Schema

        registry = get_schema_registry()

        # Generate schema from model
        model_schema = model_class._generate_avro_schema()
        schema_str = json.dumps(model_schema)
        new_schema = Schema(schema_str, "AVRO")

        # Check compatibility
        is_compatible = registry.test_compatibility(subject, new_schema)

        return CompatibilityResult(
            is_compatible=is_compatible,
            message="Schema is compatible" if is_compatible else "Schema is not compatible",
            compatibility_level=compatibility_level,
        )

    except Exception as e:
        raise SchemaCompatibilityError(
            f"Compatibility check failed: {e}",
            compatibility_level=compatibility_level,
        ) from e


def upload_schema(
    schema_path: str | Path,
    subject: str,
    compatibility_level: str | None = None,
) -> int:
    """
    Upload an Avro schema file to Schema Registry.

    Args:
        schema_path: Path to .avsc file
        subject: Schema Registry subject name
        compatibility_level: Optional compatibility level to set

    Returns:
        Schema ID from registry

    Raises:
        SchemaRegistryError: If upload fails
    """
    try:
        from confluent_kafka.schema_registry import Schema

        registry = get_schema_registry()

        # Load schema from file
        schema_dict = load_schema_from_file(schema_path)
        schema_str = json.dumps(schema_dict)
        schema = Schema(schema_str, "AVRO")

        # Set compatibility level if specified
        if compatibility_level:
            try:
                registry.set_compatibility(subject, compatibility_level)
            except Exception:
                pass  # May fail if subject doesn't exist yet

        # Register schema
        schema_id = registry.register_schema(subject, schema)

        return schema_id

    except Exception as e:
        raise SchemaRegistryError(f"Failed to upload schema: {e}") from e


def list_subjects() -> list[str]:
    """
    List all subjects in Schema Registry.

    Returns:
        List of subject names
    """
    registry = get_schema_registry()
    return registry.get_subjects()


def get_schema_versions(subject: str) -> list[int]:
    """
    Get all versions of a schema subject.

    Args:
        subject: Schema Registry subject name

    Returns:
        List of version numbers
    """
    registry = get_schema_registry()
    return registry.get_versions(subject)


def delete_subject(subject: str, permanent: bool = False) -> list[int]:
    """
    Delete a schema subject from registry.

    Args:
        subject: Schema Registry subject name
        permanent: If True, permanently delete (cannot be recovered)

    Returns:
        List of deleted version numbers
    """
    registry = get_schema_registry()
    return registry.delete_subject(subject, permanent=permanent)
