"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="d1",
    display_name="Cloudflare D1",
    adapter_path=("sqlit.domains.connections.providers.d1.adapter", "D1Adapter"),
    schema_path=("sqlit.domains.connections.providers.d1.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="",
    requires_auth=True,
)

register_provider(SPEC)
