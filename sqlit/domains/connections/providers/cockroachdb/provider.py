"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="cockroachdb",
    display_name="CockroachDB",
    adapter_path=("sqlit.domains.connections.providers.cockroachdb.adapter", "CockroachDBAdapter"),
    schema_path=("sqlit.domains.connections.providers.cockroachdb.schema", "SCHEMA"),
    supports_ssh=True,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="26257",
    requires_auth=False,
)

register_provider(SPEC)
