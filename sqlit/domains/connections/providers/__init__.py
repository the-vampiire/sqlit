"""Database provider interfaces and catalog."""

from sqlit.domains.connections.providers.adapters.base import ColumnInfo, DatabaseAdapter, TableInfo
from sqlit.domains.connections.providers.catalog import (
    get_all_schemas,
    get_db_type_for_scheme,
    get_provider,
    get_provider_schema,
    get_provider_spec,
    get_supported_db_types,
    get_supported_url_schemes,
    get_url_scheme_map,
    iter_provider_schemas,
    register_provider,
)
from sqlit.domains.connections.providers.driver import (
    DriverDescriptor,
    ensure_driver_available,
    ensure_provider_driver_available,
    import_driver_module,
)
from sqlit.domains.connections.providers.docker import DockerCredentials, DockerDetector
from sqlit.domains.connections.providers.metadata import (
    get_badge_label,
    get_connection_display_info,
    get_default_port,
    get_display_name,
    has_advanced_auth,
    is_file_based,
    requires_auth,
    supports_ssh,
)
from sqlit.domains.connections.providers.model import ProviderSpec
from sqlit.domains.connections.providers.schema_helpers import ConnectionSchema, FieldType, SchemaField, SelectOption

__all__ = [
    "ColumnInfo",
    "DatabaseAdapter",
    "TableInfo",
    "DriverDescriptor",
    "ensure_driver_available",
    "ensure_provider_driver_available",
    "import_driver_module",
    "DockerCredentials",
    "DockerDetector",
    "ProviderSpec",
    "ConnectionSchema",
    "FieldType",
    "SchemaField",
    "SelectOption",
    "get_provider",
    "get_provider_spec",
    "get_provider_schema",
    "iter_provider_schemas",
    "register_provider",
    "get_all_schemas",
    "get_db_type_for_scheme",
    "get_default_port",
    "get_display_name",
    "get_badge_label",
    "get_connection_display_info",
    "get_supported_db_types",
    "get_supported_url_schemes",
    "get_url_scheme_map",
    "has_advanced_auth",
    "is_file_based",
    "requires_auth",
    "supports_ssh",
]
