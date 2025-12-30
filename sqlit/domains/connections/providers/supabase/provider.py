"""Provider registration."""

from sqlit.domains.connections.providers.registry import ProviderSpec, register_provider

SPEC = ProviderSpec(
    db_type="supabase",
    display_name="Supabase",
    adapter_path=("sqlit.domains.connections.providers.supabase.adapter", "SupabaseAdapter"),
    schema_path=("sqlit.domains.connections.providers.supabase.schema", "SCHEMA"),
    supports_ssh=False,
    is_file_based=False,
    has_advanced_auth=False,
    default_port="",
    requires_auth=True,
)

register_provider(SPEC)
