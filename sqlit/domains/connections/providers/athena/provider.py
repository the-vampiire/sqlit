"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="athena",
    display_name="AWS Athena",
    adapter_path=("sqlit.domains.connections.providers.athena.adapter", "AthenaAdapter"),
    schema_path=("sqlit.domains.connections.providers.athena.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=False,
    has_advanced_auth=True,
    default_port="",
    requires_auth=True,
)

register_provider(SPEC)
