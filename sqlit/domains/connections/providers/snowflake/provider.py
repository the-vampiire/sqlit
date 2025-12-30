"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="snowflake",
    display_name="Snowflake",
    adapter_path=("sqlit.domains.connections.providers.snowflake.adapter", "SnowflakeAdapter"),
    schema_path=("sqlit.domains.connections.providers.snowflake.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="",
    requires_auth=True,
)

register_provider(SPEC)
