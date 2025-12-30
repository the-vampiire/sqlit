"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="firebird",
    display_name="Firebird",
    adapter_path=("sqlit.domains.connections.providers.firebird.adapter", "FirebirdAdapter"),
    schema_path=("sqlit.domains.connections.providers.firebird.schema", "SCHEMA"),
    supports_ssh=True,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="3050",
    requires_auth=True,
)

register_provider(SPEC)
