"""CLI command handlers for sqlit."""

from __future__ import annotations

import sys
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from .helpers import build_connection_config_from_args
from sqlit.domains.connections.domain.config import (
    AUTH_TYPE_LABELS,
    AuthType,
    ConnectionConfig,
    DatabaseType,
    get_database_type_labels,
)
from sqlit.domains.connections.store.connections import load_connections, save_connections
from sqlit.domains.connections.providers.registry import get_adapter_class, get_connection_schema, has_advanced_auth, is_file_based

from sqlit.domains.connections.app.credentials import (
    ALLOW_PLAINTEXT_CREDENTIALS_SETTING,
    is_keyring_usable,
    reset_credentials_service,
)

if TYPE_CHECKING:
    pass


def _maybe_prompt_plaintext_credentials() -> bool:
    """Ensure plaintext credential storage preference is set when keyring isn't usable.

    Returns True if plaintext storage is allowed; False otherwise.
    """
    from sqlit.domains.shell.store.settings import load_settings, save_settings

    if is_keyring_usable():
        return False

    settings = load_settings()
    existing = settings.get(ALLOW_PLAINTEXT_CREDENTIALS_SETTING)
    if isinstance(existing, bool):
        if existing:
            reset_credentials_service()
        return bool(existing)

    if not sys.stdin.isatty():
        return False

    answer = input("Keyring isn't available. Save passwords as plaintext in ~/.sqlit/? [y/N]: ").strip().lower()
    allow = answer in {"y", "yes"}
    settings[ALLOW_PLAINTEXT_CREDENTIALS_SETTING] = allow
    save_settings(settings)
    if allow:
        reset_credentials_service()
    return allow


def _clear_passwords_if_not_persisted(config: ConnectionConfig) -> None:
    config.password = ""
    config.ssh_password = ""


def cmd_connection_list(args: Any) -> int:
    """List all saved connections."""
    connections = load_connections()
    if not connections:
        print("No saved connections.")
        return 0

    print(f"{'Name':<20} {'Type':<10} {'Connection Info':<40} {'Auth Type':<25}")
    print("-" * 95)
    labels = get_database_type_labels()
    for conn in connections:
        db_type_label = labels.get(conn.get_db_type(), conn.db_type)
        if is_file_based(conn.db_type):
            file_path = str(conn.get_option("file_path", ""))
            conn_info = file_path[:38] + ".." if len(file_path) > 40 else file_path
            auth_label = "N/A"
        elif has_advanced_auth(conn.db_type):
            conn_info = f"{conn.server}@{conn.database}" if conn.database else conn.server
            conn_info = conn_info[:38] + ".." if len(conn_info) > 40 else conn_info
            auth_value = str(conn.get_option("auth_type", ""))
            adapter_class = get_adapter_class(conn.db_type)
            auth_type = adapter_class().get_auth_type(conn)
            auth_label = AUTH_TYPE_LABELS.get(auth_type, auth_value) if auth_type else auth_value
        else:
            # Server-based databases with simple auth
            conn_info = f"{conn.server}@{conn.database}" if conn.database else conn.server
            conn_info = conn_info[:38] + ".." if len(conn_info) > 40 else conn_info
            auth_label = f"User: {conn.username}" if conn.username else "N/A"
        print(f"{conn.name:<20} {db_type_label:<10} {conn_info:<40} {auth_label:<25}")
    return 0


def cmd_connection_create(args: Any) -> int:
    """Create a new connection."""
    from sqlit.domains.connections.app.url_parser import is_connection_url, parse_connection_url

    connections = load_connections()

    # Handle URL-based connection creation
    url = getattr(args, "url", None)
    if url:
        if not is_connection_url(url):
            print(f"Error: Invalid connection URL: {url}")
            return 1

        url_name = getattr(args, "url_name", None)
        if not url_name:
            print("Error: --name is required when using --url")
            return 1

        if any(c.name == url_name for c in connections):
            print(f"Error: Connection '{url_name}' already exists. Use 'edit' to modify it.")
            return 1

        try:
            config = parse_connection_url(url, name=url_name)
        except ValueError as exc:
            print(f"Error: {exc}")
            return 1

        connections.append(config)
        if (config.password or config.ssh_password) and not is_keyring_usable():
            if not _maybe_prompt_plaintext_credentials():
                _clear_passwords_if_not_persisted(config)
        save_connections(connections)
        print(f"Connection '{url_name}' created successfully.")
        return 0

    # Handle provider-based connection creation (existing behavior)
    if not getattr(args, "provider", None):
        print("Error: provider or --url is required.")
        print("Examples:")
        print("  sqlit connections add postgresql --name MyDB --server localhost ...")
        print("  sqlit connections add --url postgresql://user:pass@host/db --name MyDB")
        return 1

    if any(c.name == args.name for c in connections):
        print(f"Error: Connection '{args.name}' already exists. Use 'edit' to modify it.")
        return 1

    db_type = getattr(args, "provider", None)
    try:
        DatabaseType(db_type)
    except ValueError:
        valid_types = ", ".join(t.value for t in DatabaseType)
        print(f"Error: Invalid database type '{db_type}'. Valid types: {valid_types}")
        return 1

    schema = get_connection_schema(db_type)
    try:
        config = build_connection_config_from_args(
            schema,
            args,
            name=args.name,
            default_name=None,
            strict=True,
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    connections.append(config)
    if (config.password or config.ssh_password) and not is_keyring_usable():
        if not _maybe_prompt_plaintext_credentials():
            _clear_passwords_if_not_persisted(config)
    save_connections(connections)
    print(f"Connection '{args.name}' created successfully.")
    return 0


def cmd_connection_edit(args: Any) -> int:
    """Edit an existing connection."""
    connections = load_connections()

    conn_idx = None
    for i, c in enumerate(connections):
        if c.name == args.connection_name:
            conn_idx = i
            break

    if conn_idx is None:
        print(f"Error: Connection '{args.connection_name}' not found.")
        return 1

    conn = connections[conn_idx]

    if args.name:
        if args.name != conn.name and any(c.name == args.name for c in connections):
            print(f"Error: Connection '{args.name}' already exists.")
            return 1
        conn.name = args.name

    server = getattr(args, "server", None) or getattr(args, "host", None)
    if server:
        conn.server = server
    if args.port:
        conn.port = args.port
    if args.database:
        conn.database = args.database
    if args.auth_type:
        try:
            auth_type = AuthType(args.auth_type)
            conn.set_option("auth_type", auth_type.value)
            conn.set_option("trusted_connection", auth_type == AuthType.WINDOWS)
        except ValueError:
            valid_types = ", ".join(t.value for t in AuthType)
            print(f"Error: Invalid auth type '{args.auth_type}'. Valid types: {valid_types}")
            return 1
    if args.username is not None:
        conn.username = args.username
    if args.password is not None:
        conn.password = args.password

    file_path = getattr(args, "file_path", None)
    if file_path is not None:
        conn.set_option("file_path", file_path)

    if (conn.password or conn.ssh_password) and not is_keyring_usable():
        if not _maybe_prompt_plaintext_credentials():
            _clear_passwords_if_not_persisted(conn)

    save_connections(connections)
    print(f"Connection '{conn.name}' updated successfully.")
    return 0


def cmd_connection_delete(args: Any) -> int:
    """Delete a connection."""
    connections = load_connections()

    conn_idx = None
    for i, c in enumerate(connections):
        if c.name == args.connection_name:
            conn_idx = i
            break

    if conn_idx is None:
        print(f"Error: Connection '{args.connection_name}' not found.")
        return 1

    deleted = connections.pop(conn_idx)
    save_connections(connections)
    print(f"Connection '{deleted.name}' deleted successfully.")
    return 0


def cmd_docker_list(args: Any) -> int:
    """List detected Docker database containers."""
    from sqlit.domains.connections.discovery.docker_detector import (
        ContainerStatus,
        DockerStatus,
        detect_database_containers,
    )

    status, containers = detect_database_containers()

    if status == DockerStatus.NOT_INSTALLED:
        print("Error: Docker Python library not installed.")
        print("Install it with: pip install docker")
        return 1
    elif status == DockerStatus.NOT_RUNNING:
        print("Error: Docker is not running.")
        return 1
    elif status == DockerStatus.NOT_ACCESSIBLE:
        print("Error: Docker is not accessible (permission denied).")
        print("Try adding your user to the docker group or running with sudo.")
        return 1

    if not containers:
        print("No database containers found.")
        return 0

    running = [c for c in containers if c.status == ContainerStatus.RUNNING]
    exited = [c for c in containers if c.status == ContainerStatus.EXITED]

    print(f"{'Container':<25} {'Type':<12} {'Port':<8} {'Database':<15} {'Status':<10}")
    print("-" * 75)

    for c in running:
        port_str = str(c.port) if c.port else "-"
        db_str = c.database[:13] + ".." if c.database and len(c.database) > 15 else (c.database or "-")
        name_str = c.container_name[:23] + ".." if len(c.container_name) > 25 else c.container_name
        print(f"{name_str:<25} {c.db_type:<12} {port_str:<8} {db_str:<15} {'running':<10}")

    for c in exited:
        port_str = "-"
        db_str = c.database[:13] + ".." if c.database and len(c.database) > 15 else (c.database or "-")
        name_str = c.container_name[:23] + ".." if len(c.container_name) > 25 else c.container_name
        print(f"{name_str:<25} {c.db_type:<12} {port_str:<8} {db_str:<15} {'exited':<10}")

    print(f"\nFound {len(running)} running, {len(exited)} exited database container(s).")
    return 0
