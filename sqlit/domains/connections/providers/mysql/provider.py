"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="mysql",
    display_name="MySQL",
    adapter_path=("sqlit.domains.connections.providers.mysql.adapter", "MySQLAdapter"),
    schema_path=("sqlit.domains.connections.providers.mysql.schema", "SCHEMA"),
    supports_ssh=True,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="3306",
    requires_auth=True,
)

register_provider(SPEC)
