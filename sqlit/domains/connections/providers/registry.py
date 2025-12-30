"""Provider registry and lazy loading for database adapters/schemas."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from importlib import import_module
from typing import TYPE_CHECKING, cast
import pkgutil

if TYPE_CHECKING:
    from sqlit.domains.connections.domain.config import ConnectionConfig
    from sqlit.domains.connections.providers.adapters.base import DatabaseAdapter
    from sqlit.domains.connections.providers.schema_catalog import ConnectionSchema


@dataclass(frozen=True)
class ProviderSpec:
    db_type: str
    display_name: str
    adapter_path: tuple[str, str]
    schema_path: tuple[str, str]
    supports_ssh: bool = True
    is_file_based: bool = False
    has_advanced_auth: bool = False
    default_port: str = ""
    requires_auth: bool = True


_PROVIDERS: dict[str, ProviderSpec] = {}
_DISCOVERED = False


def register_provider(spec: ProviderSpec) -> None:
    """Register a provider specification."""
    _PROVIDERS[spec.db_type] = spec


def _discover_providers() -> None:
    """Discover provider packages and import their registrations."""
    global _DISCOVERED
    if _DISCOVERED:
        return

    if __package__ is None:
        return
    package = import_module(__package__)
    for module_info in pkgutil.iter_modules(package.__path__):
        name = module_info.name
        if not module_info.ispkg:
            continue
        if name in {"adapters", "__pycache__"}:
            continue
        import_module(f"{__package__}.{name}.provider")

    _DISCOVERED = True


def _ensure_discovered() -> None:
    _discover_providers()


def get_supported_db_types() -> list[str]:
    _ensure_discovered()
    return list(_PROVIDERS.keys())


def iter_provider_schemas() -> Iterable[ConnectionSchema]:
    _ensure_discovered()
    return (get_connection_schema(spec.db_type) for spec in _PROVIDERS.values())


def get_provider_spec(db_type: str) -> ProviderSpec:
    _ensure_discovered()
    spec = _PROVIDERS.get(db_type)
    if spec is None:
        raise ValueError(f"Unknown database type: {db_type}")
    return spec


def _load_schema(module_name: str, attr_name: str) -> ConnectionSchema:
    module = import_module(module_name)
    schema = getattr(module, attr_name, None)
    if schema is None:
        raise ImportError(f"Schema '{attr_name}' not found in {module_name}")
    return cast("ConnectionSchema", schema)


def get_connection_schema(db_type: str) -> ConnectionSchema:
    spec = get_provider_spec(db_type)
    return _load_schema(*spec.schema_path)


def get_all_schemas() -> dict[str, ConnectionSchema]:
    _ensure_discovered()
    return {k: get_connection_schema(k) for k in _PROVIDERS.keys()}


@lru_cache(maxsize=1)
def _get_url_scheme_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for db_type in get_supported_db_types():
        adapter_class = get_adapter_class(db_type)
        for scheme in adapter_class.url_schemes():
            mapping[scheme] = db_type
    return mapping


def get_url_scheme_map() -> dict[str, str]:
    return dict(_get_url_scheme_map())


def get_db_type_for_scheme(scheme: str) -> str | None:
    return _get_url_scheme_map().get(scheme.lower())


def get_supported_url_schemes() -> set[str]:
    return set(_get_url_scheme_map().keys())


def _apply_schema_defaults(config: "ConnectionConfig", schema: ConnectionSchema) -> "ConnectionConfig":
    if (
        not config.port
        and schema.default_port
        and any(field.name == "port" for field in schema.fields)
    ):
        config.port = schema.default_port
    return config


def normalize_connection_config(config: "ConnectionConfig") -> "ConnectionConfig":
    """Normalize provider-specific defaults and run adapter validation."""
    try:
        schema = get_connection_schema(config.db_type)
    except ValueError:
        return config

    config = _apply_schema_defaults(config, schema)
    adapter = get_adapter_class(config.db_type)()
    config = adapter.normalize_config(config)
    adapter.validate_config(config)
    return config


def _check_mock_missing_driver(db_type: str, adapter: "DatabaseAdapter") -> None:
    """Check if driver should be mocked as missing (for testing)."""
    import os

    forced_missing = os.environ.get("SQLIT_MOCK_MISSING_DRIVERS", "").strip()
    if not forced_missing:
        return

    forced = {s.strip() for s in forced_missing.split(",") if s.strip()}
    if db_type in forced:
        from sqlit.domains.connections.providers.exceptions import MissingDriverError

        if not adapter.install_extra or not adapter.install_package:
            raise ImportError(f"Missing driver for {adapter.name}")
        raise MissingDriverError(adapter.name, adapter.install_extra, adapter.install_package)


def get_adapter(db_type: str) -> "DatabaseAdapter":
    adapter = get_adapter_class(db_type)()
    _check_mock_missing_driver(db_type, adapter)
    return adapter


def get_adapter_class(db_type: str) -> type["DatabaseAdapter"]:
    spec = get_provider_spec(db_type)
    module_name, class_name = spec.adapter_path
    return _load_adapter_class(module_name, class_name)


@lru_cache(maxsize=None)
def _load_adapter_class(module_name: str, class_name: str) -> type["DatabaseAdapter"]:
    module = import_module(module_name)
    adapter_class = getattr(module, class_name, None)
    if not isinstance(adapter_class, type):
        raise ImportError(f"Adapter class '{class_name}' not found in {module_name}")
    return adapter_class


def get_default_port(db_type: str) -> str:
    try:
        spec = get_provider_spec(db_type)
    except ValueError:
        return "1433"
    return spec.default_port or "1433"


def get_display_name(db_type: str) -> str:
    spec = _PROVIDERS.get(db_type)
    if spec is None:
        _ensure_discovered()
        spec = _PROVIDERS.get(db_type)
    return spec.display_name if spec else db_type


def get_badge_label(db_type: str) -> str:
    try:
        adapter_class = get_adapter_class(db_type)
    except ValueError:
        return db_type.upper() if db_type else "DB"
    return adapter_class.badge_label()


def get_connection_display_info(config: "ConnectionConfig") -> str:
    """Return a human-friendly label for a connection."""
    try:
        adapter_class = get_adapter_class(config.db_type)
    except ValueError:
        return config.name
    return adapter_class().get_display_info(config)


def supports_ssh(db_type: str) -> bool:
    try:
        return get_provider_spec(db_type).supports_ssh
    except ValueError:
        return False


def is_file_based(db_type: str) -> bool:
    try:
        return get_provider_spec(db_type).is_file_based
    except ValueError:
        return False


def has_advanced_auth(db_type: str) -> bool:
    try:
        return get_provider_spec(db_type).has_advanced_auth
    except ValueError:
        return False


def requires_auth(db_type: str) -> bool:
    """Check if this database type requires authentication."""
    try:
        return get_provider_spec(db_type).requires_auth
    except ValueError:
        return True


def requires_database_selection(db_type: str) -> bool:
    """Check if this database type requires a database to be specified."""
    try:
        adapter = get_adapter_class(db_type)()
        return not adapter.supports_cross_database_queries
    except (ValueError, ImportError):
        return False


def validate_database_required(db_type: str, database: str | None) -> None:
    """Validate that a database is specified when required."""
    if requires_database_selection(db_type) and not database:
        display_name = get_display_name(db_type)
        raise ValueError(
            f"{display_name} requires a database to be specified. "
            "Each database is isolated and cross-database queries are not supported."
        )
