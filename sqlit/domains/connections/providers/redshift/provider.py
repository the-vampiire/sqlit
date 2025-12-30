"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="redshift",
    display_name="Amazon Redshift",
    adapter_path=("sqlit.domains.connections.providers.redshift.adapter", "RedshiftAdapter"),
    schema_path=("sqlit.domains.connections.providers.redshift.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=False,
    has_advanced_auth=True,
    default_port="5439",
    requires_auth=True,
)

register_provider(SPEC)
