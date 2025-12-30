"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="clickhouse",
    display_name="ClickHouse",
    adapter_path=("sqlit.domains.connections.providers.clickhouse.adapter", "ClickHouseAdapter"),
    schema_path=("sqlit.domains.connections.providers.clickhouse.schema", "SCHEMA"),
    supports_ssh=True,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="8123",
    requires_auth=False,
)

register_provider(SPEC)
