"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="turso",
    display_name="Turso",
    adapter_path=("sqlit.domains.connections.providers.turso.adapter", "TursoAdapter"),
    schema_path=("sqlit.domains.connections.providers.turso.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="8080",
    requires_auth=False,
)

register_provider(SPEC)
