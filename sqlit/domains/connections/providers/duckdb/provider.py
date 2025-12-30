"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="duckdb",
    display_name="DuckDB",
    adapter_path=("sqlit.domains.connections.providers.duckdb.adapter", "DuckDBAdapter"),
    schema_path=("sqlit.domains.connections.providers.duckdb.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=True,
    has_advanced_auth=False,
    default_port="",
    requires_auth=True,
)

register_provider(SPEC)
