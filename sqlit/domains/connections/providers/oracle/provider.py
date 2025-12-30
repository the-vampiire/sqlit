"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="oracle",
    display_name="Oracle",
    adapter_path=("sqlit.domains.connections.providers.oracle.adapter", "OracleAdapter"),
    schema_path=("sqlit.domains.connections.providers.oracle.schema", "SCHEMA"),
    supports_ssh=True,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="1521",
    requires_auth=True,
)

register_provider(SPEC)
