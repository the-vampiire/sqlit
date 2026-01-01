"""MySQL adapter using PyMySQL (pure Python)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlit.domains.connections.providers.mysql.base import MySQLBaseAdapter
from sqlit.domains.connections.providers.registry import get_default_port
from sqlit.domains.connections.providers.tls import (
    TLS_MODE_DEFAULT,
    TLS_MODE_DISABLE,
    get_tls_files,
    get_tls_mode,
    tls_mode_verifies_cert,
    tls_mode_verifies_hostname,
)

if TYPE_CHECKING:
    from sqlit.domains.connections.domain.config import ConnectionConfig


class MySQLAdapter(MySQLBaseAdapter):
    """Adapter for MySQL using PyMySQL."""

    @property
    def name(self) -> str:
        return "MySQL"

    @property
    def install_extra(self) -> str:
        return "mysql"

    @property
    def install_package(self) -> str:
        return "PyMySQL"

    @property
    def driver_import_names(self) -> tuple[str, ...]:
        return ("pymysql",)

    def connect(self, config: ConnectionConfig) -> Any:
        """Connect to MySQL database."""
        pymysql = self._import_driver_module(
            "pymysql",
            driver_name=self.name,
            extra_name=self.install_extra,
            package_name=self.install_package,
        )

        endpoint = config.tcp_endpoint
        if endpoint is None:
            raise ValueError("MySQL connections require a TCP-style endpoint.")
        port = int(endpoint.port or get_default_port("mysql"))
        connect_args: dict[str, Any] = {
            "host": endpoint.host,
            "port": port,
            "database": endpoint.database or None,
            "user": endpoint.username,
            "password": endpoint.password,
            "connect_timeout": 10,
            "autocommit": True,
            "charset": "utf8mb4",
        }

        tls_mode = get_tls_mode(config)
        tls_ca, tls_cert, tls_key, _ = get_tls_files(config)
        has_tls_files = any([tls_ca, tls_cert, tls_key])
        if tls_mode != TLS_MODE_DISABLE and (tls_mode != TLS_MODE_DEFAULT or has_tls_files):
            import ssl

            ssl_params: dict[str, Any] = {}
            if tls_ca:
                ssl_params["ca"] = tls_ca
            if tls_cert:
                ssl_params["cert"] = tls_cert
            if tls_key:
                ssl_params["key"] = tls_key

            if tls_mode_verifies_cert(tls_mode):
                ssl_params["cert_reqs"] = ssl.CERT_REQUIRED
            else:
                ssl_params["cert_reqs"] = ssl.CERT_NONE

            ssl_params["check_hostname"] = tls_mode_verifies_hostname(tls_mode)
            connect_args["ssl"] = ssl_params

        return pymysql.connect(**connect_args)
