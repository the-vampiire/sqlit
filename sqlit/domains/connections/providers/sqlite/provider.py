"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="sqlite",
    display_name="SQLite",
    adapter_path=("sqlit.domains.connections.providers.sqlite.adapter", "SQLiteAdapter"),
    schema_path=("sqlit.domains.connections.providers.sqlite.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=True,
    has_advanced_auth=False,
    default_port="",
    requires_auth=True,
)

register_provider(SPEC)
