"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="postgresql",
    display_name="PostgreSQL",
    adapter_path=("sqlit.domains.connections.providers.postgresql.adapter", "PostgreSQLAdapter"),
    schema_path=("sqlit.domains.connections.providers.postgresql.schema", "SCHEMA"),
    supports_ssh=True,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="5432",
    requires_auth=True,
)

register_provider(SPEC)
