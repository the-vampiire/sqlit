"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="mssql",
    display_name="SQL Server",
    adapter_path=("sqlit.domains.connections.providers.mssql.adapter", "SQLServerAdapter"),
    schema_path=("sqlit.domains.connections.providers.mssql.schema", "SCHEMA"),
    supports_ssh=True,
    is_file_based=False,
    has_advanced_auth=True,
    default_port="1433",
    requires_auth=True,
)

register_provider(SPEC)
