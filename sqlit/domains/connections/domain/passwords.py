"""Password prompt rules for connection configs."""

from __future__ import annotations

from sqlit.domains.connections.domain.config import ConnectionConfig
from sqlit.domains.connections.providers.registry import is_file_based


def needs_db_password(config: ConnectionConfig) -> bool:
    """Return True if the database password should be prompted."""
    if is_file_based(config.db_type):
        return False

    auth_type = config.get_option("auth_type")
    if auth_type in ("ad_default", "ad_integrated", "windows"):
        return False

    return config.password is None


def needs_ssh_password(config: ConnectionConfig) -> bool:
    """Return True if the SSH password should be prompted."""
    if not config.ssh_enabled:
        return False

    if config.ssh_auth_type != "password":
        return False

    return config.ssh_password is None
