"""Field definitions for database connection forms."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable


class FieldType(Enum):
    """Types of form fields."""

    TEXT = "text"
    PASSWORD = "password"
    SELECT = "select"
    FILE = "file"


@dataclass
class SelectOption:
    """An option in a select field."""

    value: str
    label: str


@dataclass
class FieldDefinition:
    """Definition of a form field for database connections."""

    name: str  # Maps to ConnectionConfig attribute
    label: str
    field_type: FieldType = FieldType.TEXT
    placeholder: str = ""
    required: bool = False
    default: str = ""
    options: list[SelectOption] = field(default_factory=list)  # For SELECT type
    # Function that takes current form values dict and returns True if field should be visible
    visible_when: Callable[[dict], bool] | None = None
    # Width hint: "full", "flex", or a number for fixed width
    width: str | int = "full"
    # Group fields on same row
    row_group: str | None = None
    # Whether the field should be hidden unless advanced mode is enabled
    advanced: bool = False


@dataclass
class FieldGroup:
    """A group of related fields with optional visibility condition."""

    name: str
    fields: list[FieldDefinition]
    # Function that takes current form values dict and returns True if group should be visible
    visible_when: Callable[[dict], bool] | None = None


def get_common_server_fields(default_port: str, server_placeholder: str = "localhost") -> list[FieldDefinition]:
    """Get common fields for server-based databases."""
    return [
        FieldDefinition(
            name="server",
            label="Server",
            placeholder=server_placeholder,
            required=True,
            row_group="server_port",
            width="flex",
        ),
        FieldDefinition(
            name="port",
            label="Port",
            placeholder=default_port,
            default=default_port,
            row_group="server_port",
            width=12,
        ),
        FieldDefinition(
            name="database",
            label="Database",
            placeholder="(empty = browse all)",
        ),
    ]


def get_credential_fields() -> list[FieldDefinition]:
    """Get username/password fields."""
    return [
        FieldDefinition(
            name="username",
            label="Username",
            placeholder="username",
            required=True,
        ),
        FieldDefinition(
            name="password",
            label="Password",
            field_type=FieldType.PASSWORD,
        ),
    ]
