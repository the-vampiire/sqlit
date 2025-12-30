"""Protocol definitions for mixin type safety."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, overload

if TYPE_CHECKING:
    from textual.screen import Screen
    from textual.timer import Timer
    from textual.widget import Widget
    from textual.widgets import DataTable, Static, TextArea, Tree
    from textual.worker import Worker
    from sqlit.domains.connections.app.session import ConnectionSession
    from sqlit.domains.connections.domain.config import ConnectionConfig
    from sqlit.domains.connections.providers.adapters.base import DatabaseAdapter
    from sqlit.domains.query.app.query_service import QueryService
    from sqlit.shared.ui.spinner import Spinner
    from sqlit.shared.ui.widgets import SqlitDataTable, VimMode

QueryType = TypeVar("QueryType", bound="Widget")


class AppProtocol(Protocol):
    """Protocol defining what mixins expect from the App class.

    This protocol captures the interface that mixin classes depend on,
    allowing proper type checking without creating inheritance conflicts.
    Mixins should use `self: AppProtocol` in method signatures.
    """

    # === Textual App methods ===

    def notify(
        self,
        message: str,
        *,
        title: str = "",
        severity: str = "information",
        timeout: float | None = None,
        markup: bool = True,
    ) -> None:
        """Show notification."""
        ...

    def push_screen(
        self,
        screen: Screen[Any] | str,
        callback: Callable[[Any], None] | Callable[[Any], Awaitable[None]] | None = None,
        wait_for_dismiss: bool = False,
    ) -> Any:
        """Push a screen onto the screen stack."""
        ...

    def pop_screen(self) -> Any:
        """Pop the current screen from the screen stack."""
        ...

    def run_worker(
        self,
        work: Any,
        name: str | None = "",
        group: str = "default",
        description: str = "",
        exit_on_error: bool = True,
        start: bool = True,
        exclusive: bool = False,
        thread: bool = False,
    ) -> Worker[Any]:
        """Run work in a worker thread/task."""
        ...

    def call_later(self, callback: Callable[..., Any], *args: Any, **kwargs: Any) -> bool:
        """Schedule a callback to run later on the main thread."""
        ...

    def call_from_thread(self, callback: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Call a function from a worker thread on the main thread."""
        ...

    def call_after_refresh(self, callback: Callable[[], Any]) -> None:
        """Call a function after the next screen refresh."""
        ...

    def set_timer(
        self,
        delay: float,
        callback: Callable[[], None] | None = None,
        *,
        name: str | None = None,
        pause: bool = False,
    ) -> Timer:
        """Set a timer to call a function after a delay."""
        ...

    def set_interval(
        self,
        interval: float,
        callback: Callable[[], None] | None = None,
        *,
        name: str | None = None,
        repeat: int = 0,
        pause: bool = False,
    ) -> Timer:
        """Set an interval timer."""
        ...

    @overload
    def query_one(self, selector: str) -> Widget: ...

    @overload
    def query_one(self, selector: type[QueryType]) -> QueryType: ...

    @overload
    def query_one(self, selector: str, expect_type: type[QueryType]) -> QueryType: ...

    def query_one(self, selector: Any, expect_type: Any = None) -> Any:
        """Query for a single widget."""
        ...

    def copy_to_clipboard(self, text: str) -> None:
        """Copy text to clipboard."""
        ...

    def exit(self, result: Any = None, return_code: int = 0, message: Any | None = None) -> None:
        """Exit the application."""
        ...

    # === Screen-related attributes ===

    @property
    def screen(self) -> Screen[Any]:
        """The current screen."""
        ...

    @property
    def screen_stack(self) -> list[Screen[Any]]:
        """The screen stack."""
        ...

    @property
    def focused(self) -> Any:
        """The currently focused widget."""
        ...

    @property
    def size(self) -> Any:
        """The size of the terminal."""
        ...

    @property
    def theme(self) -> str:
        """The current theme name."""
        ...

    @theme.setter
    def theme(self, value: str) -> None:
        """Set the theme."""
        ...

    def get_custom_theme_names(self) -> set[str]:
        """Return custom theme names."""
        ...

    def add_custom_theme(self, theme_name: str) -> str:
        """Add a custom theme by name."""
        ...

    def get_custom_theme_path(self, theme_name: str) -> Any:
        """Get the custom theme path for a theme name."""
        ...

    def open_custom_theme_in_editor(self, theme_name: str) -> None:
        """Open a custom theme in an external editor."""
        ...

    # === SSMSTUI widget properties ===

    @property
    def object_tree(self) -> Tree[Any]:
        """The explorer tree widget."""
        ...

    @property
    def query_input(self) -> TextArea:
        """The query input text area."""
        ...

    @property
    def results_table(self) -> SqlitDataTable:
        """The results table widget."""
        ...

    @property
    def status_bar(self) -> Static:
        """The status bar widget."""
        ...

    @property
    def autocomplete_dropdown(self) -> Any:
        """The autocomplete dropdown widget."""
        ...

    @property
    def tree_filter_input(self) -> Any:
        """The tree filter input widget."""
        ...

    @property
    def results_filter_input(self) -> Any:
        """The results filter input widget."""
        ...

    @property
    def results_area(self) -> Any:
        """The results container widget."""
        ...

    # === Metadata helpers ===

    def _get_effective_database(self) -> str | None:
        """Return the active database name for metadata lookups."""
        ...

    def _get_metadata_db_arg(self, database: str | None) -> str | None:
        """Normalize metadata database argument for queries."""
        ...

    # === Connection state ===

    connections: list[ConnectionConfig]
    current_connection: Any | None
    current_config: ConnectionConfig | None
    current_adapter: DatabaseAdapter | None
    current_ssh_tunnel: Any
    _direct_connection_config: ConnectionConfig | None
    _connecting_config: ConnectionConfig | None
    _connection_attempt_id: int
    _connect_spinner: Spinner | None
    _connect_spinner_index: int
    _connect_spinner_timer: Timer | None

    # === Session state ===

    _session: ConnectionSession | None
    _session_factory: Callable[[ConnectionConfig], ConnectionSession] | None
    _connection_failed: bool

    # === Vim mode state ===

    vim_mode: VimMode

    # === Tree state ===

    _expanded_paths: set[str]
    _loading_nodes: set[str]
    _tree_filter_visible: bool
    _tree_filter_text: str
    _tree_filter_query: str
    _tree_filter_fuzzy: bool
    _tree_filter_typing: bool
    _tree_filter_matches: list[Any]
    _tree_filter_match_index: int
    _tree_original_labels: dict[int, str]

    # === Query execution state ===

    _query_worker: Worker[Any] | None
    _query_executing: bool
    _query_start_time: float
    _spinner_index: int
    _spinner_timer: Timer | None
    _cancellable_query: Any | None
    _query_spinner: Spinner | None
    _query_cursor_cache: dict[str, tuple[int, int]] | None

    # === Schema cache state ===

    _schema_cache: dict[str, Any]
    _schema_indexing: bool
    _schema_worker: Worker[Any] | None
    _schema_spinner_index: int
    _schema_spinner_timer: Timer | None
    _table_metadata: dict[str, tuple[str, str, str | None]]
    _columns_loading: set[str]
    _schema_spinner: Spinner | None
    _schema_pending_dbs: list[str | None]
    _schema_total_jobs: int
    _schema_completed_jobs: int
    _schema_scheduler: Any
    _db_object_cache: dict[str, dict[str, list[Any]]]

    # === Autocomplete state ===

    _autocomplete_filter: str
    _autocomplete_just_applied: bool
    _autocomplete_visible: bool
    _suppress_autocomplete_on_newline: bool
    _autocomplete_debounce_timer: Timer | None
    _text_just_changed: bool

    # === Results state ===

    _last_result_columns: list[str]
    _last_result_rows: list[tuple[Any, ...]]
    _last_result_row_count: int
    _internal_clipboard: str
    _last_query_table: dict[str, Any] | None
    _results_table_counter: int
    _results_filter_visible: bool
    _results_filter_text: str
    _results_filter_matches: list[int]
    _results_filter_match_index: int
    _results_filter_original_rows: list[tuple[Any, ...]]
    _results_filter_matching_rows: list[tuple[Any, ...]]
    _results_filter_fuzzy: bool
    _results_filter_debounce_timer: Timer | None
    _results_filter_pending_update: bool
    _tooltip_cell_coord: tuple[int, int] | None
    _tooltip_showing: bool
    MAX_FILTER_MATCHES: int

    # === UI state ===

    _fullscreen_mode: str
    _last_notification: str
    _last_notification_severity: str
    _last_notification_time: str
    _notification_timer: Timer | None
    _notification_history: list[tuple[str, str, str]]
    _leader_timer: Timer | None
    _leader_pending: bool
    _last_active_pane: str | None
    _state_machine: Any
    _mock_profile: Any
    _active_database: str | None
    _query_target_database: str | None
    log: Any

    # === SSMSTUI methods that mixins call on each other ===

    def refresh_tree(self) -> None:
        """Refresh the explorer tree."""
        ...

    def populate_connected_tree(self) -> None:
        """Populate tree with database objects when connected."""
        ...

    def _update_status_bar(self) -> None:
        """Update status bar with connection and vim mode info."""
        ...

    def _update_footer_bindings(self) -> None:
        """Update footer with context-appropriate bindings."""
        ...

    def _update_connecting_indicator(self) -> None:
        """Update UI to reflect connecting state."""
        ...

    def _set_connecting_state(self, config: ConnectionConfig | None, refresh: bool = True) -> None:
        """Update connection-in-progress state."""
        ...

    def _select_connected_node(self) -> None:
        """Select the tree node for the active connection."""
        ...

    def _hide_autocomplete(self) -> None:
        """Hide the autocomplete dropdown."""
        ...

    def _load_schema_cache(self) -> None:
        """Load database schema for autocomplete asynchronously."""
        ...

    def _load_schema_directly(self) -> None:
        """Load schema using threaded workers."""
        ...

    def _stop_schema_spinner(self) -> None:
        """Stop the schema indexing spinner animation."""
        ...

    def _disconnect_silent(self) -> None:
        """Disconnect from current database without notification."""
        ...

    def connect_to_server(self, config: ConnectionConfig) -> None:
        """Connect to a database (async, non-blocking)."""
        ...

    def action_execute_query(self) -> None:
        """Execute the current query."""
        ...

    # === Tree Mixin methods ===

    def _db_type_badge(self, db_type: str) -> str:
        """Get short badge for database type."""
        ...

    def _format_connection_label(self, conn: Any, status: str, spinner: str | None = None) -> str:
        """Format a connection label for the tree."""
        ...

    def _connect_spinner_frame(self) -> str:
        """Get the current connection spinner frame."""
        ...

    def _get_node_kind(self, node: Any) -> str:
        """Get node kind string."""
        ...

    def _add_database_object_nodes(self, node: Any, database: str | None) -> None:
        """Add database object nodes (tables, views, etc.) to a folder."""
        ...

    def _ensure_database_connection(self, target_db: str) -> bool:
        """Ensure connection is switched to target database."""
        ...

    def _fallback_reconnect_and_retry(self, node: Any, action: Callable[[], None]) -> None:
        """Reconnect and retry an action."""
        ...

    def _reconnect_to_database(self, db_name: str) -> None:
        """Reconnect to a specific database."""
        ...

    def _update_database_labels(self) -> None:
        """Update database labels in the tree."""
        ...

    def set_default_database(self, db_name: str | None) -> None:
        """Set the default database for the connection."""
        ...

    def _show_index_info(self, data: Any) -> None:
        """Show index details."""
        ...

    def _show_trigger_info(self, data: Any) -> None:
        """Show trigger details."""
        ...

    def _show_sequence_info(self, data: Any) -> None:
        """Show sequence details."""
        ...

    def _display_object_info(self, header: str, body: dict[str, Any]) -> None:
        """Display object information in results."""
        ...

    def _restore_subtree_expansion(self, node: Any) -> None:
        """Restore expansion state for a subtree."""
        ...

    def _get_node_path(self, node: Any) -> str:
        """Get unique path for a node."""
        ...

    def _save_expanded_state(self) -> None:
        """Save currently expanded nodes."""
        ...

    def action_tree_filter(self) -> None:
        """Open the tree filter."""
        ...

    def action_tree_filter_close(self) -> None:
        """Close the tree filter."""
        ...

    def action_tree_filter_accept(self) -> None:
        """Accept tree filter selection."""
        ...

    def action_tree_filter_next(self) -> None:
        """Move to next tree filter match."""
        ...

    def action_tree_filter_prev(self) -> None:
        """Move to previous tree filter match."""
        ...

    def _update_tree_filter(self) -> None:
        """Update the tree filter state."""
        ...

    def _jump_to_current_match(self) -> None:
        """Jump to current tree filter match."""
        ...

    def _expand_ancestors(self, node: Any) -> None:
        """Expand ancestor nodes to show a target node."""
        ...

    def _restore_tree_labels(self) -> None:
        """Restore tree node labels."""
        ...

    def _show_all_tree_nodes(self) -> None:
        """Show all tree nodes."""
        ...

    def _count_all_nodes(self) -> int:
        """Count all tree nodes."""
        ...

    def _find_matching_nodes(self, node: Any, matches: list[Any]) -> bool:
        """Find matching nodes for tree filter."""
        ...

    def _get_node_label_text(self, node: Any) -> str:
        """Get label text for a tree node."""
        ...

    def _apply_filter_to_tree(self) -> None:
        """Apply tree filter to node visibility."""
        ...

    def _set_node_visibility(
        self, node: Any, match_ids: set[Any], ancestor_ids: set[Any], visible: bool
    ) -> None:
        """Set node visibility in tree filter."""
        ...

    def _rebuild_label_with_highlight(self, node: Any, highlighted_text: str) -> str:
        """Rebuild node label with highlight markup."""
        ...

    def _load_columns_async(self, node: Any, data: Any) -> None:
        """Load columns for a table or view asynchronously."""
        ...

    def _load_folder_async(self, node: Any, data: Any) -> None:
        """Load folder content asynchronously."""
        ...

    def _on_columns_loaded(
        self, node: Any, db_name: str | None, schema_name: str, obj_name: str, columns: list[Any]
    ) -> None:
        """Handle columns loaded event."""
        ...

    def _on_tree_load_error(self, node: Any, error_message: str) -> None:
        """Handle tree loading error."""
        ...

    def _on_folder_loaded(self, node: Any, db_name: str | None, folder_type: str, items: list[Any]) -> None:
        """Handle folder content loaded event."""
        ...

    def _add_schema_grouped_items(
        self, node: Any, db_name: str | None, folder_type: str, items: list[Any], default_schema: str
    ) -> None:
        """Add items grouped by schema."""
        ...

    # === Autocomplete Mixin methods ===

    def _load_columns_for_table(self, table_name: str) -> None:
        """Load columns for a table for autocomplete."""
        ...

    def _on_autocomplete_columns_loaded(self, table_name: str, actual_table_name: str, column_names: list[str]) -> None:
        """Handle columns loaded for autocomplete."""
        ...

    def _location_to_offset(self, text: str, location: tuple[int, int]) -> int:
        """Convert row/col location to string offset."""
        ...

    def _offset_to_location(self, text: str, offset: int) -> tuple[int, int]:
        """Convert string offset to row/col location."""
        ...

    def _get_word_before_cursor(self, text: str, cursor_pos: int) -> tuple[str, str]:
        """Get the word before the cursor."""
        ...

    def _run_db_call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a db call through the session executor when available."""
        ...

    def _get_current_word(self, text: str, cursor_pos: int) -> str:
        """Get the current word at the cursor."""
        ...

    def _build_alias_map(self, text: str) -> dict[str, str]:
        """Build alias map for table references."""
        ...

    def _get_autocomplete_suggestions(self, text: str, cursor_pos: int) -> list[str]:
        """Get autocomplete suggestions."""
        ...

    def _trigger_autocomplete(self, text_area: Any) -> None:
        """Trigger autocomplete after debounce."""
        ...

    def _has_tables_needing_columns(self, text: str) -> bool:
        """Check if query has tables needing columns."""
        ...

    def _preload_columns_for_query(self) -> None:
        """Preload columns for tables in the current query."""
        ...

    def action_exit_insert_mode(self) -> None:
        """Exit insert mode."""
        ...

    def _show_autocomplete(self, suggestions: list[str], filter_text: str) -> None:
        """Show the autocomplete dropdown."""
        ...

    def _apply_autocomplete(self) -> None:
        """Apply the selected autocomplete suggestion."""
        ...

    def _start_schema_spinner(self) -> None:
        """Start the schema indexing spinner."""
        ...

    def _load_schema_cache_async(self) -> Awaitable[None]:
        """Load schema cache asynchronously."""
        ...

    def _animate_schema_spinner(self) -> None:
        """Animate the schema spinner."""
        ...

    def _update_schema_cache(
        self, schema_cache: dict[str, Any], table_metadata: dict[str, tuple[str, str, str | None]] | None = None
    ) -> None:
        """Update the schema cache."""
        ...

    def _on_databases_loaded(self, databases: list[Any]) -> None:
        """Handle database list loaded."""
        ...

    def _on_databases_error(self, error: Exception) -> None:
        """Handle database list error."""
        ...

    def _load_tables_job(self, database: str | None) -> None:
        """Load tables for a database."""
        ...

    def _load_views_job(self, database: str | None) -> None:
        """Load views for a database."""
        ...

    def _load_procedures_job(self, database: str | None) -> None:
        """Load procedures for a database."""
        ...

    def _on_tables_loaded(self, tables: list[Any], database: str | None, cache_key: str) -> None:
        """Handle tables loaded."""
        ...

    def _on_tables_error(self, error: Exception, database: str | None) -> None:
        """Handle tables error."""
        ...

    def _process_tables_result(self, tables: list[Any], database: str | None, cache_key: str) -> None:
        """Process tables result."""
        ...

    def _on_views_loaded(self, views: list[Any], database: str | None, cache_key: str) -> None:
        """Handle views loaded."""
        ...

    def _on_views_error(self, error: Exception, database: str | None) -> None:
        """Handle views error."""
        ...

    def _process_views_result(self, views: list[Any], database: str | None, cache_key: str) -> None:
        """Process views result."""
        ...

    def _on_procedures_loaded(self, procedures: list[Any], database: str | None, cache_key: str) -> None:
        """Handle procedures loaded."""
        ...

    def _on_procedures_error(self, error: Exception, database: str | None) -> None:
        """Handle procedures error."""
        ...

    def _process_procedures_result(self, procedures: list[Any], cache_key: str) -> None:
        """Process procedures result."""
        ...

    def _schema_job_complete(self) -> None:
        """Mark a schema job complete."""
        ...

    # === Results Mixin methods ===

    def _copy_text(self, text: str) -> bool:
        """Copy text to clipboard."""
        ...

    def _flash_table_yank(self, table: SqlitDataTable, scope: str) -> None:
        """Flash the table to indicate copy."""
        ...

    def _format_tsv(self, columns: list[str], rows: list[tuple[Any, ...]]) -> str:
        """Format results as TSV."""
        ...

    def _replace_results_table(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        """Replace results table data."""
        ...

    def _replace_results_table_raw(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        """Replace results table data without formatting."""
        ...

    def _restore_results_table(self) -> None:
        """Restore the results table to the last full state."""
        ...

    def _get_debounce_ms(self, row_count: int) -> int:
        """Get debounce delay based on row count."""
        ...

    def action_results_filter(self) -> None:
        """Open the results filter."""
        ...

    def action_results_filter_close(self) -> None:
        """Close results filter."""
        ...

    def action_results_filter_accept(self) -> None:
        """Accept results filter."""
        ...

    def action_results_filter_next(self) -> None:
        """Move to next results filter match."""
        ...

    def action_results_filter_prev(self) -> None:
        """Move to previous results filter match."""
        ...

    def _jump_to_current_results_match(self) -> None:
        """Jump to current results filter match."""
        ...

    def _schedule_filter_update(self) -> None:
        """Schedule results filter update."""
        ...

    def _do_debounced_filter_update(self) -> None:
        """Run debounced results filter update."""
        ...

    def _update_results_filter(self) -> None:
        """Update results filter state and table."""
        ...

    def _rebuild_results_with_matches(self, matching_rows: list[tuple[Any, ...]], search_text: str) -> None:
        """Rebuild results table with matches."""
        ...

    def _highlight_substring(self, text: str, search_lower: str) -> str:
        """Highlight substring matches in text."""
        ...

    # === Query Mixin methods ===

    @property
    def _query_service(self) -> QueryService | None:
        """The query execution service."""
        ...

    def _execute_query_common(self, keep_insert_mode: bool) -> None:
        """Common logic for executing queries."""
        ...

    def _start_query_spinner(self) -> None:
        """Start the query execution spinner."""
        ...

    def _run_query_async(self, query: str, keep_insert_mode: bool) -> Awaitable[None]:
        """Run a query asynchronously."""
        ...

    def _animate_spinner(self) -> None:
        """Animate the query spinner."""
        ...

    def _display_query_error(self, error_message: str) -> None:
        """Display a query error."""
        ...

    def _stop_query_spinner(self) -> None:
        """Stop the query spinner."""
        ...

    def _display_query_results(
        self, columns: list[str], rows: list[tuple[Any, ...]], row_count: int, truncated: bool, elapsed_ms: float
    ) -> None:
        """Display query results in the table."""
        ...

    def _display_non_query_result(self, affected: int, elapsed_ms: float) -> None:
        """Display non-query result (rows affected)."""
        ...

    def _restore_insert_mode(self) -> None:
        """Restore insert mode if it was active."""
        ...

    def _handle_history_result(self, result: Any) -> None:
        """Handle result from history screen."""
        ...

    def _delete_history_entry(self, timestamp: str) -> None:
        """Delete a history entry."""
        ...

    def action_show_history(self) -> None:
        """Show query history."""
        ...

    def action_copy_query(self) -> None:
        """Copy the current query."""
        ...

    def action_copy_cell(self) -> None:
        """Copy the current cell."""
        ...

    def _toggle_star(self, query: str) -> None:
        """Toggle starred status for a query."""
        ...

    def _clear_query_target_database(self) -> None:
        """Clear the active query target database."""
        ...

    # === Connection Mixin methods ===

    def _set_connection_screen_footer(self) -> None:
        """Set footer for connection screen."""
        ...

    def _wrap_connection_result(self, result: tuple[Any, ...] | None) -> None:
        """Wrap connection result."""
        ...

    def call_next(self, *args: Any, **kwargs: Any) -> None:
        """Call next handler (Installer pattern)."""
        ...

    def handle_connection_result(self, result: tuple[Any, ...] | None) -> None:
        """Handle connection result."""
        ...

    def _do_delete_connection(self, config: ConnectionConfig) -> None:
        """Delete connection."""
        ...

    def _handle_connection_picker_result(self, result: str | None) -> None:
        """Handle connection picker result."""
        ...

    def _populate_credentials_if_missing(self, config: ConnectionConfig) -> None:
        """Populate missing credentials if available."""
        ...

    def _connect_with_db_password_check(self, config: ConnectionConfig) -> None:
        """Check db password then connect."""
        ...

    def _do_connect(self, config: ConnectionConfig) -> None:
        """Perform the actual connection."""
        ...

    def _start_connect_spinner(self) -> None:
        """Start the connection spinner."""
        ...

    def _stop_connect_spinner(self) -> None:
        """Stop the connection spinner."""
        ...

    def _on_connect_spinner_tick(self) -> None:
        """Handle a connection spinner tick."""
        ...

    def _get_connection_config_from_data(self, data: Any) -> ConnectionConfig | None:
        """Get connection config from node data."""
        ...

    def action_new_connection(self) -> None:
        """Create a new connection."""
        ...

    def _handle_docker_container_result(self, result: Any) -> None:
        """Handle docker connection picker result."""
        ...

    def _handle_azure_resource_result(self, result: Any) -> None:
        """Handle Azure connection picker result."""
        ...

    def _handle_cloud_connection_result(self, result: Any) -> None:
        """Handle cloud connection picker result."""
        ...

    def _find_matching_saved_connection(self, container: Any) -> ConnectionConfig | None:
        """Find saved connection matching a container."""
        ...

    def _get_connection_config_from_node(self, node: Any) -> ConnectionConfig | None:
        """Get connection config for a tree node."""
        ...

    # === UI Navigation Mixin methods ===

    def _set_fullscreen_mode(self, mode: str) -> None:
        """Set fullscreen mode."""
        ...

    def _update_section_labels(self) -> None:
        """Update section labels."""
        ...

    def _sync_active_pane_title(self) -> None:
        """Sync active pane title to UI."""
        ...

    def _update_idle_scheduler_bar(self) -> None:
        """Update idle scheduler UI bar."""
        ...

    def _show_error_in_results(self, message: str, timestamp: str) -> None:
        """Show error in results table."""
        ...

    def _show_leader_menu(self) -> None:
        """Show leader menu."""
        ...

    def _cancel_leader_pending(self) -> None:
        """Cancel leader pending state."""
        ...

    def _handle_leader_result(self, result: str | None) -> None:
        """Handle leader menu result."""
        ...

    def _execute_leader_command(self, action: str) -> None:
        """Execute leader command."""
        ...

    def action_close_value_view(self) -> None:
        """Close the inline value view."""
        ...

    def action_copy_value_view(self) -> None:
        """Copy the inline value view content."""
        ...

    @property
    def idle_scheduler_bar(self) -> Static:
        """Status bar for idle scheduler."""
        ...
