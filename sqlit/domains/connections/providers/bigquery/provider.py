"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="bigquery",
    display_name="Google BigQuery",
    adapter_path=("sqlit.domains.connections.providers.bigquery.adapter", "BigQueryAdapter"),
    schema_path=("sqlit.domains.connections.providers.bigquery.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=False,
    has_advanced_auth=True,
    default_port="",
    requires_auth=False,
)

register_provider(SPEC)
