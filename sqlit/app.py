"""Main Textual application for sqlit."""

from __future__ import annotations

from typing import Any

try:
    import pyodbc

    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

from textual.app import App, ComposeResult, SystemCommand
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import DataTable, Static, TextArea, Tree

from .adapters import DatabaseAdapter, get_adapter
from .config import (
    ConnectionConfig,
    load_connections,
    load_settings,
    save_connections,
    save_settings,
)
from .screens import (
    ConfirmScreen,
    ConnectionScreen,
    HelpScreen,
    ValueViewScreen,
)
from .widgets import AutocompleteDropdown, ContextFooter, KeyBinding, VimMode


class SSMSTUI(App):
    """Main SSMS TUI application."""

    TITLE = "sqlit"

    CSS = """
    Screen {
        background: $surface;
    }

    DataTable.flash-cell:focus > .datatable--cursor,
    DataTable.flash-row:focus > .datatable--cursor,
    DataTable.flash-all:focus > .datatable--cursor {
        background: $success;
        color: $background;
        text-style: bold;
    }

    DataTable.flash-all {
        border: solid $success;
    }

    Screen.results-fullscreen #sidebar {
        display: none;
    }

    Screen.results-fullscreen #query-area {
        display: none;
    }

    Screen.results-fullscreen #results-area {
        height: 1fr;
    }

    Screen.query-fullscreen #sidebar {
        display: none;
    }

    Screen.query-fullscreen #results-area {
        display: none;
    }

    Screen.query-fullscreen #query-area {
        height: 1fr;
        border-bottom: none;
    }

    Screen.explorer-fullscreen #main-panel {
        display: none;
    }

    Screen.explorer-fullscreen #sidebar {
        width: 1fr;
        border-right: none;
    }

    #main-container {
        width: 100%;
        height: 100%;
    }

    #content {
        height: 1fr;
    }

    #sidebar {
        width: 35;
        border-right: solid $primary;
        padding: 1;
    }

    #object-tree {
        height: 1fr;
    }

    #main-panel {
        width: 1fr;
    }

    #query-area {
        height: 50%;
        border-bottom: solid $primary;
        padding: 1;
    }

    #query-input {
        height: 1fr;
    }

    #results-area {
        height: 50%;
        padding: 1;
    }

    #results-table {
        height: 1fr;
    }

    #status-bar {
        height: 1;
        background: $surface-darken-1;
        padding: 0 1;
    }

    .section-label {
        height: 1;
        color: $text-muted;
        padding: 0 1;
        margin-bottom: 1;
    }

    .section-label.active {
        color: $primary;
        text-style: bold;
    }

    #autocomplete-dropdown {
        layer: autocomplete;
        position: absolute;
        display: none;
    }

    #autocomplete-dropdown.visible {
        display: block;
    }
    """

    LAYERS = ["autocomplete"]

    BINDINGS = [
        Binding("n", "new_connection", "New", show=False),
        Binding("s", "select_table", "Select", show=False),
        Binding("R", "refresh_tree", "Refresh", show=False),
        Binding("e", "edit_connection", "Edit", show=False),
        Binding("d", "delete_connection", "Delete", show=False),
        Binding("delete", "delete_connection", "Delete", show=False),
        Binding("x", "disconnect", "Disconnect", show=False),
        Binding("ctrl+p", "command_palette", "Commands", show=False),
        Binding("ctrl+q", "quit", "Quit", show=False),
        Binding("question_mark", "show_help", "Help", show=False),
        Binding("e", "focus_explorer", "Explorer", show=False),
        Binding("q", "focus_query", "Query", show=False),
        Binding("r", "focus_results", "Results", show=False),
        Binding("i", "enter_insert_mode", "Insert", show=False),
        Binding("escape", "exit_insert_mode", "Normal", show=False),
        Binding("enter", "execute_query", "Execute", show=False),
        Binding("f5", "execute_query_insert", "Execute", show=False),
        Binding("d", "clear_query", "Clear", show=False),
        Binding("n", "new_query", "New", show=False),
        Binding("h", "show_history", "History", show=False),
        Binding("z", "collapse_tree", "Collapse", show=False),
        Binding("f", "toggle_fullscreen", "Fullscreen", show=False),
        Binding("v", "view_cell", "View cell", show=False),
        Binding("y", "copy_cell", "Copy cell", show=False),
        Binding("Y", "copy_row", "Copy row", show=False),
        Binding("a", "copy_results", "Copy results", show=False),
    ]

    def __init__(self):
        super().__init__()
        self.connections: list[ConnectionConfig] = []
        self.current_connection: Any | None = None
        self.current_config: ConnectionConfig | None = None
        self.current_adapter: DatabaseAdapter | None = None
        self.vim_mode: VimMode = VimMode.NORMAL
        self._expanded_paths: set[str] = set()
        self._schema_cache: dict = {
            "tables": [],
            "views": [],
            "columns": {},
            "procedures": [],
        }
        self._autocomplete_visible: bool = False
        self._autocomplete_items: list[str] = []
        self._autocomplete_index: int = 0
        self._autocomplete_filter: str = ""
        self._autocomplete_just_applied: bool = False
        self._last_result_columns: list[str] = []
        self._last_result_rows: list[tuple] = []
        self._last_result_row_count: int = 0
        self._internal_clipboard: str = ""
        self._fullscreen_mode: str = "none"
        self._connection_health: dict[str, bool] = {}

    def _set_fullscreen_mode(self, mode: str) -> None:
        """Set fullscreen mode: none|explorer|query|results."""
        self._fullscreen_mode = mode
        self.screen.remove_class("results-fullscreen")
        self.screen.remove_class("query-fullscreen")
        self.screen.remove_class("explorer-fullscreen")
        if mode == "results":
            self.screen.add_class("results-fullscreen")
        elif mode == "query":
            self.screen.add_class("query-fullscreen")
        elif mode == "explorer":
            self.screen.add_class("explorer-fullscreen")

    def _set_connection_health(self, name: str, ok: bool | None) -> None:
        """Record a connection health status to affect tree coloring."""
        if ok is None:
            self._connection_health.pop(name, None)
            return
        self._connection_health[name] = ok

    def _apply_connection_health(self, name: str, ok: bool | None) -> None:
        """Apply connection health update and refresh tree."""
        self._set_connection_health(name, ok)
        try:
            self.refresh_tree()
        except Exception:
            pass

    def action_test_connections(self) -> None:
        """Test all configured connections and mark failures in the tree."""
        self.notify("Testing connections…")
        self._test_connection_health()

    def _test_connection_health(self) -> None:
        """Test all configured connections in the background and mark failures."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        connections = list(self.connections)

        def test_one(config: ConnectionConfig) -> tuple[str, bool | None]:
            # Avoid auth flows that require user interaction.
            if (
                getattr(config, "db_type", "") == "mssql"
                and getattr(config, "auth_type", "") == "ad_interactive"
            ):
                return config.name, None

            try:
                adapter = get_adapter(config.db_type)
                conn = adapter.connect(config)
                try:
                    close = getattr(conn, "close", None)
                    if callable(close):
                        close()
                except Exception:
                    pass
                return config.name, True
            except (ModuleNotFoundError, ImportError):
                return config.name, None
            except Exception:
                return config.name, False

        def work() -> None:
            max_workers = min(32, max(1, len(connections)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(test_one, c) for c in connections]
                for future in as_completed(futures):
                    try:
                        name, ok = future.result()
                    except Exception:
                        continue
                    self.call_from_thread(self._apply_connection_health, name, ok)

        self.run_worker(
            work,
            group="connection-health",
            name="connection-health",
            description="Testing connections",
            thread=True,
            exclusive=True,
        )

    def _db_type_badge(self, db_type: str) -> str:
        badge_map = {
            "mssql": "MSSQL",
            "postgresql": "PG",
            "mysql": "MySQL",
            "mariadb": "MariaDB",
            "sqlite": "SQLite",
            "oracle": "Oracle",
            "duckdb": "DuckDB",
            "cockroachdb": "CRDB",
        }
        return badge_map.get(db_type, db_type.upper() if db_type else "DB")

    def _copy_text(self, text: str) -> bool:
        """Copy text to clipboard if possible, otherwise store internally."""
        self._internal_clipboard = text

        # Prefer Textual's clipboard support (OSC52 where available).
        try:
            self.copy_to_clipboard(text)
            return True
        except Exception:
            pass

        # Fallback to system clipboard via pyperclip (requires platform support).
        try:
            import pyperclip  # type: ignore

            pyperclip.copy(text)
            return True
        except Exception:
            return False

    def _flash_table_yank(self, table: DataTable, scope: str) -> None:
        """Briefly flash the yanked cell(s) to confirm a copy action."""
        previous_cursor_type = getattr(table, "cursor_type", "cell")
        css_class = "flash-cell"
        target_cursor_type = "cell"

        if scope == "row":
            css_class = "flash-row"
            target_cursor_type = "row"
        elif scope == "all":
            css_class = "flash-all"
            target_cursor_type = previous_cursor_type

        try:
            table.cursor_type = target_cursor_type
        except Exception:
            pass

        table.add_class(css_class)

        def _clear() -> None:
            try:
                table.remove_class(css_class)
                try:
                    table.cursor_type = previous_cursor_type
                except Exception:
                    pass
            except Exception:
                pass

        table.set_timer(0.15, _clear)

    def _format_tsv(self, columns: list[str], rows: list[tuple]) -> str:
        def fmt(value: object) -> str:
            if value is None:
                return "NULL"
            return str(value).replace("\t", " ").replace("\r", "").replace("\n", "\\n")

        lines: list[str] = []
        if columns:
            lines.append("\t".join(columns))
        for row in rows:
            lines.append("\t".join(fmt(v) for v in row))
        return "\n".join(lines)

    def check_action(self, action: str, parameters: tuple) -> bool | None:
        """Only allow actions when their context is active."""
        try:
            tree = self.query_one("#object-tree", Tree)
            query_input = self.query_one("#query-input", TextArea)
            results_table = self.query_one("#results-table", DataTable)
        except Exception:
            return True

        tree_focused = tree.has_focus
        query_focused = query_input.has_focus
        results_focused = results_table.has_focus
        in_insert_mode = self.vim_mode == VimMode.INSERT

        node = tree.cursor_node
        node_type = None
        is_root = node == tree.root if node else False
        if node and node.data:
            node_type = node.data[0]

        if action == "enter_insert_mode":
            return query_focused and not in_insert_mode
        elif action == "exit_insert_mode":
            return in_insert_mode

        if in_insert_mode:
            if action in (
                "quit",
                "exit_insert_mode",
                "command_palette",
                "execute_query_insert",
            ):
                return True
            return False

        if action == "new_connection":
            return tree_focused
        elif action == "refresh_tree":
            return tree_focused
        elif action == "collapse_tree":
            return tree_focused
        elif action == "edit_connection":
            return tree_focused and node_type == "connection"
        elif action == "delete_connection":
            return tree_focused and node_type == "connection"
        elif action == "connect_selected":
            if not tree_focused or node_type != "connection":
                return False
            # Allow connecting if not connected, or if selecting a different server
            config = node.data[1] if node and node.data else None
            if not self.current_connection:
                return True
            return config and self.current_config and config.name != self.current_config.name
        elif action == "disconnect":
            return (
                tree_focused
                and node_type == "connection"
                and self.current_connection is not None
            )
        elif action == "select_table":
            return tree_focused and node_type in ("table", "view")
        elif action == "execute_query":
            return (
                query_focused or results_focused
            ) and self.current_connection is not None
        elif action == "execute_query_insert":
            return query_focused and self.current_connection is not None
        elif action in ("clear_query", "new_query"):
            return query_focused and self.vim_mode == VimMode.NORMAL
        elif action == "show_history":
            return query_focused and self.vim_mode == VimMode.NORMAL and self.current_config is not None
        elif action == "focus_explorer":
            if tree_focused and node_type == "connection":
                return False
            return True
        elif action in ("focus_query", "focus_results"):
            return True
        elif action == "toggle_fullscreen":
            return True
        elif action == "test_connections":
            return True
        elif action in ("view_cell", "copy_cell", "copy_row", "copy_results"):
            return results_focused and self.current_connection is not None
        elif action in (
            "quit",
            "show_help",
            "command_palette",
            "toggle_dark",
        ):
            return True

        return True

    def compose(self) -> ComposeResult:
        with Vertical(id="main-container"):
            with Horizontal(id="content"):
                with Vertical(id="sidebar"):
                    yield Static(
                        r"\[e] Object Explorer", classes="section-label", id="label-explorer"
                    )
                    tree = Tree("Servers", id="object-tree")
                    tree.show_root = False
                    tree.guide_depth = 2
                    yield tree

                with Vertical(id="main-panel"):
                    with Container(id="query-area"):
                        yield Static(
                            r"\[q] Query", classes="section-label", id="label-query"
                        )
                        yield TextArea(
                            "",
                            language="sql",
                            id="query-input",
                            read_only=True,
                        )
                        yield AutocompleteDropdown(id="autocomplete-dropdown")

                    with Container(id="results-area"):
                        yield Static(
                            r"\[r] Results", classes="section-label", id="label-results"
                        )
                        yield DataTable(id="results-table")

            yield Static("Not connected", id="status-bar")

        yield ContextFooter()

    def on_mount(self) -> None:
        """Initialize the app."""
        if not PYODBC_AVAILABLE:
            self.notify(
                "pyodbc not installed. Run: pip install pyodbc",
                severity="warning",
                timeout=10,
            )

        settings = load_settings()
        if "theme" in settings:
            try:
                self.theme = settings["theme"]
            except Exception:
                self.theme = "tokyo-night"
        else:
            self.theme = "tokyo-night"

        settings = load_settings()
        self._expanded_paths = set(settings.get("expanded_nodes", []))

        self.connections = load_connections()
        self.refresh_tree()
        self._update_footer_bindings()

        tree = self.query_one("#object-tree", Tree)
        tree.focus()
        self._update_section_labels()

        # Check for ODBC drivers
        self._check_drivers()

    def _check_drivers(self) -> None:
        """Check if ODBC drivers are installed and show setup if needed."""
        # Only check drivers if there are SQL Server connections configured
        has_mssql = any(c.db_type == "mssql" for c in self.connections)
        if not has_mssql:
            return

        if not PYODBC_AVAILABLE:
            return

        from .drivers import get_installed_drivers

        installed = get_installed_drivers()
        if not installed:
            self.call_later(self._show_driver_setup)

    def _show_driver_setup(self) -> None:
        """Show the driver setup screen."""
        from .drivers import get_installed_drivers
        from .screens import DriverSetupScreen

        installed = get_installed_drivers()
        self.push_screen(DriverSetupScreen(installed), self._handle_driver_result)

    def _handle_driver_result(self, result) -> None:
        """Handle result from driver setup screen."""
        if not result:
            return

        action = result[0]
        if action == "select":
            driver = result[1]
            self.notify(f"Selected driver: {driver}")
        elif action == "install":
            commands = result[1]
            self._run_driver_install(commands)

    def _run_driver_install(self, commands: list[str]) -> None:
        """Run driver installation commands."""
        import subprocess

        self.notify("Running installation commands...", timeout=3)

        # Combine commands and run in a terminal
        full_command = " && ".join(commands)

        try:
            # Run in a new terminal window so user can see progress and enter sudo password
            import shutil

            if shutil.which("gnome-terminal"):
                subprocess.Popen([
                    "gnome-terminal", "--", "bash", "-c",
                    f'{full_command}; echo ""; echo "Press Enter to close..."; read'
                ])
            elif shutil.which("konsole"):
                subprocess.Popen([
                    "konsole", "-e", "bash", "-c",
                    f'{full_command}; echo ""; echo "Press Enter to close..."; read'
                ])
            elif shutil.which("xterm"):
                subprocess.Popen([
                    "xterm", "-e", "bash", "-c",
                    f'{full_command}; echo ""; echo "Press Enter to close..."; read'
                ])
            elif shutil.which("open"):  # macOS
                # Write commands to a temp script
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
                    f.write("#!/bin/bash\n")
                    f.write(full_command + "\n")
                    f.write('echo ""\necho "Press Enter to close..."\nread\n')
                    script_path = f.name
                import os
                os.chmod(script_path, 0o755)
                subprocess.Popen(["open", "-a", "Terminal", script_path])
            else:
                # Fallback: just run it
                self.notify(
                    "No terminal found. Run these commands manually:\n" + full_command,
                    severity="warning",
                    timeout=15,
                )
                return

            self.notify(
                "Installation started in new terminal. Restart sqlit when done.",
                timeout=10,
            )
        except Exception as e:
            self.notify(f"Failed to start installation: {e}", severity="error")

    def watch_theme(self, old_theme: str, new_theme: str) -> None:
        """Save theme whenever it changes."""
        settings = load_settings()
        settings["theme"] = new_theme
        save_settings(settings)

    def _update_section_labels(self) -> None:
        """Update section labels to highlight the active pane."""
        try:
            label_explorer = self.query_one("#label-explorer", Static)
            label_query = self.query_one("#label-query", Static)
            label_results = self.query_one("#label-results", Static)
        except Exception:
            return

        label_explorer.remove_class("active")
        label_query.remove_class("active")
        label_results.remove_class("active")

        focused = self.focused
        if focused:
            widget = focused
            while widget:
                widget_id = getattr(widget, "id", None)
                if widget_id == "object-tree" or widget_id == "sidebar":
                    label_explorer.add_class("active")
                    break
                elif widget_id == "query-input" or widget_id == "query-area":
                    label_query.add_class("active")
                    break
                elif widget_id == "results-table" or widget_id == "results-area":
                    label_results.add_class("active")
                    break
                widget = getattr(widget, "parent", None)

    def action_focus_explorer(self) -> None:
        """Focus the Object Explorer pane."""
        if self._fullscreen_mode != "none":
            self._set_fullscreen_mode("none")
        self.query_one("#object-tree", Tree).focus()

    def action_focus_query(self) -> None:
        """Focus the Query pane (in NORMAL mode)."""
        if self._fullscreen_mode != "none":
            self._set_fullscreen_mode("none")
        self.vim_mode = VimMode.NORMAL
        query_input = self.query_one("#query-input", TextArea)
        query_input.read_only = True
        query_input.focus()
        self._update_status_bar()

    def action_focus_results(self) -> None:
        """Focus the Results pane."""
        if self._fullscreen_mode != "none":
            self._set_fullscreen_mode("none")
        self.query_one("#results-table", DataTable).focus()

    def action_enter_insert_mode(self) -> None:
        """Enter INSERT mode for query editing."""
        query_input = self.query_one("#query-input", TextArea)
        if query_input.has_focus and self.vim_mode == VimMode.NORMAL:
            self.vim_mode = VimMode.INSERT
            query_input.read_only = False
            self._update_status_bar()
            self._update_footer_bindings()

    def action_exit_insert_mode(self) -> None:
        """Exit INSERT mode, return to NORMAL mode."""
        if self.vim_mode == VimMode.INSERT:
            self.vim_mode = VimMode.NORMAL
            query_input = self.query_one("#query-input", TextArea)
            query_input.read_only = True
            self._hide_autocomplete()
            self._update_status_bar()
            self._update_footer_bindings()


    def _get_word_before_cursor(self, text: str, cursor_pos: int) -> tuple[str, str]:
        """Get the current word being typed and the context keyword before it."""
        if cursor_pos <= 0 or cursor_pos > len(text):
            return "", ""

        before_cursor = text[:cursor_pos]

        word_start = cursor_pos
        while word_start > 0 and before_cursor[word_start - 1] not in " \t\n,()[]":
            word_start -= 1
        current_word = before_cursor[word_start:cursor_pos]

        if "." in current_word:
            parts = current_word.rsplit(".", 1)
            table_name = parts[0].strip("[]")
            return parts[1] if len(parts) > 1 else "", f"column:{table_name}"

        context_text = before_cursor[:word_start].upper().strip()

        table_keywords = ["FROM", "JOIN", "INTO", "UPDATE", "TABLE"]
        for kw in table_keywords:
            if context_text.endswith(kw):
                return current_word, "table"

        if context_text.endswith("EXEC") or context_text.endswith("EXECUTE"):
            return current_word, "procedure"

        if context_text.endswith("SELECT") or context_text.endswith(","):
            return current_word, "column_or_table"

        return current_word, ""

    def _get_autocomplete_suggestions(self, word: str, context: str) -> list[str]:
        """Get autocomplete suggestions based on context."""
        suggestions = []

        if context == "table":
            suggestions = self._schema_cache["tables"] + self._schema_cache["views"]
        elif context == "procedure":
            suggestions = self._schema_cache["procedures"]
        elif context.startswith("column:"):
            table_name = context.split(":", 1)[1].lower()
            suggestions = self._schema_cache["columns"].get(table_name, [])
        elif context == "column_or_table":
            all_columns = []
            for cols in self._schema_cache["columns"].values():
                all_columns.extend(cols)
            suggestions = list(set(all_columns)) + self._schema_cache["tables"]

        if word:
            word_lower = word.lower()
            suggestions = [s for s in suggestions if s.lower().startswith(word_lower)]

        return suggestions[:50]

    def _show_autocomplete(self, suggestions: list[str], filter_text: str) -> None:
        """Show the autocomplete dropdown with suggestions."""
        if not suggestions:
            self._hide_autocomplete()
            return

        dropdown = self.query_one("#autocomplete-dropdown", AutocompleteDropdown)
        dropdown.set_items(suggestions, filter_text)

        try:
            query_input = self.query_one("#query-input", TextArea)
            cursor_loc = query_input.cursor_location
            dropdown.styles.offset = (cursor_loc[1] + 2, cursor_loc[0] + 1)
        except Exception:
            pass

        dropdown.show()
        self._autocomplete_visible = True

    def _hide_autocomplete(self) -> None:
        """Hide the autocomplete dropdown."""
        try:
            dropdown = self.query_one("#autocomplete-dropdown", AutocompleteDropdown)
            dropdown.hide()
            self._autocomplete_visible = False
        except Exception:
            pass

    def _apply_autocomplete(self) -> None:
        """Apply the selected autocomplete suggestion."""
        dropdown = self.query_one("#autocomplete-dropdown", AutocompleteDropdown)
        selected = dropdown.get_selected()

        if not selected:
            self._hide_autocomplete()
            return

        self._autocomplete_just_applied = True

        query_input = self.query_one("#query-input", TextArea)
        text = query_input.text
        cursor_loc = query_input.cursor_location
        cursor_pos = self._location_to_offset(text, cursor_loc)

        word_start = cursor_pos
        while word_start > 0 and text[word_start - 1] not in " \t\n,()[]":
            word_start -= 1

        if word_start > 0 and text[word_start - 1] == ".":
            new_text = (
                text[:cursor_pos]
                + selected[len(text[word_start:cursor_pos]) :]
                + text[cursor_pos:]
            )
        else:
            new_text = text[:word_start] + selected + text[cursor_pos:]

        query_input.text = new_text

        new_cursor_pos = word_start + len(selected)
        new_loc = self._offset_to_location(new_text, new_cursor_pos)
        query_input.cursor_location = new_loc

        self._hide_autocomplete()

    def _location_to_offset(self, text: str, location: tuple) -> int:
        """Convert (row, col) location to text offset."""
        row, col = location
        lines = text.split("\n")
        offset = sum(len(lines[i]) + 1 for i in range(row))
        offset += col
        return min(offset, len(text))

    def _offset_to_location(self, text: str, offset: int) -> tuple:
        """Convert text offset to (row, col) location."""
        lines = text.split("\n")
        current_offset = 0
        for row, line in enumerate(lines):
            if current_offset + len(line) >= offset:
                return (row, offset - current_offset)
            current_offset += len(line) + 1
        return (len(lines) - 1, len(lines[-1]) if lines else 0)

    def on_text_area_changed(self, event: TextArea.Changed) -> None:
        """Handle text changes in the query editor for autocomplete."""
        if event.text_area.id != "query-input":
            return

        if self._autocomplete_just_applied:
            self._autocomplete_just_applied = False
            self._hide_autocomplete()
            return

        if self.vim_mode != VimMode.INSERT:
            self._hide_autocomplete()
            return

        if not self.current_connection:
            return

        text = event.text_area.text
        cursor_loc = event.text_area.cursor_location
        cursor_pos = self._location_to_offset(text, cursor_loc)

        word, context = self._get_word_before_cursor(text, cursor_pos)

        if context:
            is_column_context = context.startswith("column:")
            if is_column_context or len(word) >= 1:
                suggestions = self._get_autocomplete_suggestions(word, context)
                if suggestions:
                    self._show_autocomplete(suggestions, word)
                else:
                    self._hide_autocomplete()
            else:
                self._hide_autocomplete()
        else:
            self._hide_autocomplete()

    def on_key(self, event) -> None:
        """Handle key events for autocomplete navigation."""
        if not self._autocomplete_visible:
            return

        dropdown = self.query_one("#autocomplete-dropdown", AutocompleteDropdown)

        if event.key == "down":
            dropdown.move_selection(1)
            event.prevent_default()
            event.stop()
        elif event.key == "up":
            dropdown.move_selection(-1)
            event.prevent_default()
            event.stop()
        elif event.key == "tab":
            if self.vim_mode == VimMode.INSERT and dropdown.filtered_items:
                self._apply_autocomplete()
                event.prevent_default()
                event.stop()
        elif event.key == "escape":
            self._hide_autocomplete()

    def _update_status_bar(self) -> None:
        """Update status bar with connection and vim mode info."""
        status = self.query_one("#status-bar", Static)
        conn_info = "Not connected"
        if self.current_config:
            display_info = self.current_config.get_display_info()
            conn_info = f"[#90EE90]Connected to {self.current_config.name}[/] ({display_info})"

        try:
            query_input = self.query_one("#query-input", TextArea)
            if query_input.has_focus:
                if self.vim_mode == VimMode.NORMAL:
                    mode_str = f"[bold orange1]-- {self.vim_mode.value} --[/]"
                else:
                    mode_str = f"[dim]-- {self.vim_mode.value} --[/]"
                status.update(f"{mode_str}  {conn_info}")
            else:
                status.update(conn_info)
        except Exception:
            status.update(conn_info)

    def refresh_tree(self) -> None:
        """Refresh the object explorer tree."""
        tree = self.query_one("#object-tree", Tree)
        tree.clear()
        tree.root.expand()

        for conn in self.connections:
            display_info = conn.get_display_info()
            db_type_label = self._db_type_badge(conn.db_type)
            health = self._connection_health.get(conn.name)
            if health is False:
                name_markup = f"[dim #ff6b6b]{conn.name}[/]"
            else:
                name_markup = f"[dim]{conn.name}[/dim]"
            node = tree.root.add(
                f"{name_markup} [{db_type_label}] ({display_info})"
            )
            node.data = ("connection", conn)
            node.allow_expand = True

        if self.current_connection and self.current_config:
            self.populate_connected_tree()

    def populate_connected_tree(self) -> None:
        """Populate tree with database objects when connected."""
        if not self.current_connection or not self.current_config or not self.current_adapter:
            return

        tree = self.query_one("#object-tree", Tree)
        adapter = self.current_adapter

        def get_conn_label(config, connected=False):
            display_info = config.get_display_info()
            db_type_label = self._db_type_badge(config.db_type)
            health = self._connection_health.get(config.name)
            if connected:
                name = f"[green]{config.name}[/green]"
            elif health is False:
                name = f"[#ff6b6b]{config.name}[/]"
            else:
                name = config.name
            return f"{name} [{db_type_label}] ({display_info})"

        active_node = None
        for child in tree.root.children:
            if child.data and child.data[0] == "connection":
                if child.data[1].name == self.current_config.name:
                    child.set_label(get_conn_label(self.current_config, connected=True))
                    active_node = child
                    break

        if not active_node:
            active_node = tree.root.add(
                get_conn_label(self.current_config, connected=True)
            )
            active_node.data = ("connection", self.current_config)

        active_node.remove_children()

        try:
            # Check if this database type supports multiple databases
            if adapter.supports_multiple_databases:
                specific_db = self.current_config.database
                if specific_db and specific_db.lower() not in ("", "master"):
                    # Show objects for a specific database
                    self._add_database_object_nodes(active_node, specific_db)
                    active_node.expand()
                else:
                    # Show all databases
                    dbs_node = active_node.add("Databases")
                    dbs_node.data = ("folder", "databases")

                    databases = adapter.get_databases(self.current_connection)
                    for db_name in databases:
                        db_node = dbs_node.add(db_name)
                        db_node.data = ("database", db_name)
                        db_node.allow_expand = True
                        self._add_database_object_nodes(db_node, db_name)

                    active_node.expand()
                    dbs_node.expand()
            else:
                # SQLite and other single-database systems
                self._add_database_object_nodes(active_node, None)
                active_node.expand()

            self.call_later(lambda: self._restore_subtree_expansion(active_node))

        except Exception as e:
            self.notify(f"Error loading objects: {e}", severity="error")

    def _add_database_object_nodes(self, parent_node, database: str | None) -> None:
        """Add Tables, Views, and optionally Stored Procedures nodes."""
        tables_node = parent_node.add("Tables")
        tables_node.data = ("folder", "tables", database)
        tables_node.allow_expand = True

        views_node = parent_node.add("Views")
        views_node.data = ("folder", "views", database)
        views_node.allow_expand = True

        if self.current_adapter and self.current_adapter.supports_stored_procedures:
            procs_node = parent_node.add("Stored Procedures")
            procs_node.data = ("folder", "procedures", database)
            procs_node.allow_expand = True

    def _get_node_path(self, node) -> str:
        """Get a unique path string for a tree node."""
        parts = []
        current = node
        while current and current.parent:
            if current.data:
                data = current.data
                if data[0] == "connection":
                    parts.append(f"conn:{data[1].name}")
                elif data[0] == "database":
                    parts.append(f"db:{data[1]}")
                elif data[0] == "folder":
                    parts.append(f"folder:{data[1]}")
                elif data[0] in ("table", "view"):
                    parts.append(f"{data[0]}:{data[2]}")
            current = current.parent
        return "/".join(reversed(parts))

    def _restore_subtree_expansion(self, node) -> None:
        """Recursively expand nodes that should be expanded."""
        for child in node.children:
            if child.data:
                path = self._get_node_path(child)
                if path in self._expanded_paths:
                    child.expand()
            self._restore_subtree_expansion(child)

    def _save_expanded_state(self) -> None:
        """Save which nodes are expanded."""
        try:
            tree = self.query_one("#object-tree", Tree)
        except Exception:
            return

        expanded = []

        def collect_expanded(node):
            if node.is_expanded and node.data:
                path = self._get_node_path(node)
                if path:
                    expanded.append(path)
            for child in node.children:
                collect_expanded(child)

        collect_expanded(tree.root)

        self._expanded_paths = set(expanded)
        settings = load_settings()
        settings["expanded_nodes"] = expanded
        save_settings(settings)

    def on_tree_node_collapsed(self, event: Tree.NodeCollapsed) -> None:
        """Save state when a node is collapsed."""
        self.call_later(self._save_expanded_state)

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        """Load child objects when a node is expanded."""
        node = event.node

        self.call_later(self._save_expanded_state)

        if not node.data or not self.current_connection or not self.current_adapter:
            return

        data = node.data
        adapter = self.current_adapter

        if len(list(node.children)) > 0:
            return

        try:
            if data[0] == "table" and len(data) >= 3:
                db_name = data[1]
                table_name = data[2]
                columns = adapter.get_columns(self.current_connection, table_name, db_name)
                for col in columns:
                    child = node.add_leaf(f"[dim]{col.name}[/] [italic dim]{col.data_type}[/]")
                    child.data = ("column", db_name, table_name, col.name)
                return

            if data[0] == "view" and len(data) >= 3:
                db_name = data[1]
                view_name = data[2]
                columns = adapter.get_columns(self.current_connection, view_name, db_name)
                for col in columns:
                    child = node.add_leaf(f"[dim]{col.name}[/] [italic dim]{col.data_type}[/]")
                    child.data = ("column", db_name, view_name, col.name)
                return

            if data[0] != "folder" or len(data) < 3:
                return

            folder_type = data[1]
            db_name = data[2]

            if folder_type == "tables":
                tables = adapter.get_tables(self.current_connection, db_name)
                for table_name in tables:
                    child = node.add(table_name)
                    child.data = ("table", db_name, table_name)
                    child.allow_expand = True

            elif folder_type == "views":
                views = adapter.get_views(self.current_connection, db_name)
                for view_name in views:
                    child = node.add(view_name)
                    child.data = ("view", db_name, view_name)
                    child.allow_expand = True

            elif folder_type == "procedures":
                if adapter.supports_stored_procedures:
                    procedures = adapter.get_procedures(self.current_connection, db_name)
                    for proc_name in procedures:
                        child = node.add(proc_name)
                        child.data = ("procedure", db_name, proc_name)

        except Exception as e:
            self.notify(f"Error loading: {e}", severity="error")

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle tree node selection (double-click/enter)."""
        node = event.node
        if not node.data:
            return

        data = node.data

        if data[0] == "connection":
            config = data[1]
            # If already connected to this one, do nothing
            if self.current_config and self.current_config.name == config.name:
                return
            # If connected to another, disconnect first
            if self.current_connection:
                self._disconnect_silent()
            self.connect_to_server(config)

    def action_new_connection(self) -> None:
        """Show new connection dialog."""
        self._set_connection_screen_footer()
        self.push_screen(ConnectionScreen(), self._wrap_connection_result)

    def action_edit_connection(self) -> None:
        """Edit the selected connection."""
        tree = self.query_one("#object-tree", Tree)
        node = tree.cursor_node

        if not node or not node.data:
            return

        data = node.data
        if data[0] != "connection":
            return

        config = data[1]
        self._set_connection_screen_footer()
        self.push_screen(
            ConnectionScreen(config, editing=True), self._wrap_connection_result
        )

    def _set_connection_screen_footer(self) -> None:
        """Set footer bindings for connection screen."""
        try:
            footer = self.query_one(ContextFooter)
        except Exception:
            return

        left_bindings = [
            KeyBinding("^t", "Test", "test_connection"),
            KeyBinding("^s", "Save", "save"),
        ]
        right_bindings = [
            KeyBinding("esc", "Cancel", "cancel"),
        ]
        # Commands are shown in the connection dialog border subtitle.
        footer.set_bindings([], [])

    def _wrap_connection_result(self, result: tuple | None) -> None:
        """Wrapper to restore footer after connection dialog."""
        self._update_footer_bindings()
        self.handle_connection_result(result)

    def handle_connection_result(self, result: tuple | None) -> None:
        """Handle result from connection dialog."""
        if not result:
            return

        action, config = result

        if action == "save":
            self.connections = [c for c in self.connections if c.name != config.name]
            self.connections.append(config)
            save_connections(self.connections)
            self.refresh_tree()
            self.notify(f"Connection '{config.name}' saved")

    def connect_to_server(self, config: ConnectionConfig) -> None:
        """Connect to a database."""
        # Check for pyodbc only if it's a SQL Server connection
        if config.db_type == "mssql" and not PYODBC_AVAILABLE:
            self.notify("pyodbc not installed. Run: pip install pyodbc", severity="error")
            return

        try:
            adapter = get_adapter(config.db_type)
            self.current_connection = adapter.connect(config)
            self.current_config = config
            self.current_adapter = adapter
            self._set_connection_health(config.name, True)

            status = self.query_one("#status-bar", Static)
            display_info = config.get_display_info()
            status.update(f"[#90EE90]Connected to {config.name}[/] ({display_info})")

            self.refresh_tree()
            self._load_schema_cache()
            self.notify(f"Connected to {config.name}")

        except Exception as e:
            self._set_connection_health(config.name, False)
            self.refresh_tree()
            self.notify(f"Connection failed: {e}", severity="error")

    def _load_schema_cache(self) -> None:
        """Load database schema for autocomplete."""
        if not self.current_connection or not self.current_config or not self.current_adapter:
            return

        self._schema_cache = {
            "tables": [],
            "views": [],
            "columns": {},
            "procedures": [],
        }

        adapter = self.current_adapter

        try:
            # Determine which databases to load
            if adapter.supports_multiple_databases:
                db = self.current_config.database
                if db and db.lower() not in ("", "master"):
                    databases = [db]
                else:
                    # Get user databases (skip system databases for SQL Server)
                    all_dbs = adapter.get_databases(self.current_connection)
                    # Filter out system databases
                    system_dbs = {"master", "tempdb", "model", "msdb"}
                    databases = [d for d in all_dbs if d.lower() not in system_dbs]
            else:
                # Single database systems like SQLite
                databases = [None]

            for database in databases:
                try:
                    # Load tables
                    tables = adapter.get_tables(self.current_connection, database)
                    for table_name in tables:
                        self._schema_cache["tables"].append(table_name)
                        if database:
                            full_name = f"{adapter.quote_identifier(database)}.{adapter.quote_identifier(table_name)}"
                            self._schema_cache["tables"].append(full_name)

                        # Load columns for each table
                        columns = adapter.get_columns(self.current_connection, table_name, database)
                        self._schema_cache["columns"][table_name.lower()] = [c.name for c in columns]

                    # Load views
                    views = adapter.get_views(self.current_connection, database)
                    for view_name in views:
                        self._schema_cache["views"].append(view_name)
                        if database:
                            full_name = f"{adapter.quote_identifier(database)}.{adapter.quote_identifier(view_name)}"
                            self._schema_cache["views"].append(full_name)

                        # Load columns for each view
                        columns = adapter.get_columns(self.current_connection, view_name, database)
                        self._schema_cache["columns"][view_name.lower()] = [c.name for c in columns]

                    # Load procedures (if supported)
                    if adapter.supports_stored_procedures:
                        procedures = adapter.get_procedures(self.current_connection, database)
                        self._schema_cache["procedures"].extend(procedures)

                except Exception:
                    pass

            # Deduplicate
            self._schema_cache["tables"] = list(dict.fromkeys(self._schema_cache["tables"]))
            self._schema_cache["views"] = list(dict.fromkeys(self._schema_cache["views"]))
            self._schema_cache["procedures"] = list(dict.fromkeys(self._schema_cache["procedures"]))

        except Exception as e:
            self.notify(f"Error loading schema: {e}", severity="warning")

    def _disconnect_silent(self) -> None:
        """Disconnect from current database without notification."""
        if self.current_connection:
            try:
                self.current_connection.close()
            except Exception:
                pass
            self.current_connection = None
            self.current_config = None
            self.current_adapter = None

    def action_disconnect(self) -> None:
        """Disconnect from current database."""
        if self.current_connection:
            self._disconnect_silent()

            status = self.query_one("#status-bar", Static)
            status.update("Disconnected")

            self.refresh_tree()
            self.notify("Disconnected")

    def action_execute_query(self) -> None:
        """Execute the current query."""
        self._execute_query_common(keep_insert_mode=False)

    def action_execute_query_insert(self) -> None:
        """Execute query in INSERT mode without leaving it."""
        self._execute_query_common(keep_insert_mode=True)

    def _execute_query_common(self, keep_insert_mode: bool) -> None:
        if not self.current_connection or not self.current_adapter:
            self.notify("Not connected to a database", severity="warning")
            return

        query_input = self.query_one("#query-input", TextArea)
        query = query_input.text.strip()

        if not query:
            self.notify("No query to execute", severity="warning")
            return

        results_table = self.query_one("#results-table", DataTable)
        results_table.clear(columns=True)

        try:
            query_type = query.strip().upper().split()[0] if query.strip() else ""
            is_select_query = query_type in (
                "SELECT",
                "WITH",
                "SHOW",
                "DESCRIBE",
                "EXPLAIN",
                "PRAGMA",
            )

            if is_select_query:
                columns, rows = self.current_adapter.execute_query(
                    self.current_connection, query
                )
                row_count = len(rows)

                self._last_result_columns = columns
                self._last_result_rows = list(rows)
                self._last_result_row_count = row_count

                results_table.add_columns(*columns)
                for row in rows[:1000]:
                    str_row = tuple(str(v) if v is not None else "NULL" for v in row)
                    results_table.add_row(*str_row)

                self.notify(f"Query returned {row_count} rows")
            else:
                affected = self.current_adapter.execute_non_query(
                    self.current_connection, query
                )
                self._last_result_columns = ["Result"]
                self._last_result_rows = [(f"{affected} row(s) affected",)]
                self._last_result_row_count = 1

                results_table.add_column("Result")
                results_table.add_row(f"{affected} row(s) affected")
                self.notify(f"Query executed: {affected} row(s) affected")

            if self.current_config:
                from .config import save_query_to_history

                save_query_to_history(self.current_config.name, query)

            if keep_insert_mode:
                self.vim_mode = VimMode.INSERT
                query_input.read_only = False
                query_input.focus()
                self._update_footer_bindings()
                self._update_status_bar()

        except Exception as e:
            self._last_result_columns = ["Error"]
            self._last_result_rows = [(str(e),)]
            self._last_result_row_count = 1

            results_table.add_column("Error")
            results_table.add_row(str(e))
            self.notify(f"Query error: {e}", severity="error")

    def action_toggle_fullscreen(self) -> None:
        """Toggle fullscreen for the currently focused pane."""
        try:
            tree = self.query_one("#object-tree", Tree)
            query_input = self.query_one("#query-input", TextArea)
            results_table = self.query_one("#results-table", DataTable)
        except Exception:
            return

        if tree.has_focus:
            target = "explorer"
        elif query_input.has_focus:
            target = "query"
        elif results_table.has_focus:
            target = "results"
        else:
            target = "none"

        if target != "none" and self._fullscreen_mode == target:
            self._set_fullscreen_mode("none")
        else:
            self._set_fullscreen_mode(target)

        if self._fullscreen_mode == "explorer":
            tree.focus()
        elif self._fullscreen_mode == "query":
            query_input.focus()
        elif self._fullscreen_mode == "results":
            results_table.focus()

        self._update_section_labels()
        self._update_footer_bindings()

    def action_view_cell(self) -> None:
        """View the full value of the selected cell."""
        table = self.query_one("#results-table", DataTable)
        if table.row_count <= 0:
            self.notify("No results", severity="warning")
            return
        try:
            value = table.get_cell_at(table.cursor_coordinate)
        except Exception:
            return
        self.push_screen(
            ValueViewScreen(
                str(value) if value is not None else "NULL", title="Cell Value"
            )
        )

    def action_copy_cell(self) -> None:
        """Copy the selected cell to clipboard (or internal clipboard)."""
        table = self.query_one("#results-table", DataTable)
        if table.row_count <= 0:
            self.notify("No results", severity="warning")
            return
        try:
            value = table.get_cell_at(table.cursor_coordinate)
        except Exception:
            return
        self._copy_text(str(value) if value is not None else "NULL")
        self._flash_table_yank(table, "cell")

    def action_copy_row(self) -> None:
        """Copy the selected row to clipboard (TSV)."""
        table = self.query_one("#results-table", DataTable)
        if table.row_count <= 0:
            self.notify("No results", severity="warning")
            return
        try:
            row_values = table.get_row_at(table.cursor_row)
        except Exception:
            return

        text = self._format_tsv([], [tuple(row_values)])
        self._copy_text(text)
        self._flash_table_yank(table, "row")

    def action_copy_results(self) -> None:
        """Copy the entire results (last query) to clipboard (TSV)."""
        if not self._last_result_columns and not self._last_result_rows:
            self.notify("No results", severity="warning")
            return

        text = self._format_tsv(self._last_result_columns, self._last_result_rows)
        self._copy_text(text)
        try:
            table = self.query_one("#results-table", DataTable)
            self._flash_table_yank(table, "all")
        except Exception:
            pass

    def action_clear_query(self) -> None:
        """Clear the query input."""
        query_input = self.query_one("#query-input", TextArea)
        query_input.text = ""

    def action_new_query(self) -> None:
        """Start a new query (clear input and results)."""
        query_input = self.query_one("#query-input", TextArea)
        query_input.text = ""
        results_table = self.query_one("#results-table", DataTable)
        results_table.clear(columns=True)

    def action_show_history(self) -> None:
        """Show query history for the current connection."""
        if not self.current_config:
            self.notify("Not connected to a database", severity="warning")
            return

        from .config import load_query_history
        from .screens import QueryHistoryScreen

        history = load_query_history(self.current_config.name)
        self.push_screen(
            QueryHistoryScreen(history, self.current_config.name),
            self._handle_history_result,
        )

    def _handle_history_result(self, result) -> None:
        """Handle the result from the history screen."""
        if result is None:
            return

        action, data = result
        if action == "select":
            query_input = self.query_one("#query-input", TextArea)
            query_input.text = data
        elif action == "delete":
            # Delete the entry from history
            self._delete_history_entry(data)
            # Re-open history screen
            self.action_show_history()

    def _delete_history_entry(self, timestamp: str) -> None:
        """Delete a specific history entry by timestamp."""
        from .config import HISTORY_PATH
        import json

        if not HISTORY_PATH.exists():
            return

        try:
            with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                entries = json.load(f)

            # Remove entry with matching timestamp and connection
            entries = [
                e for e in entries
                if not (e.get("timestamp") == timestamp and e.get("connection_name") == self.current_config.name)
            ]

            with open(HISTORY_PATH, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=2)
        except (json.JSONDecodeError, TypeError):
            pass

    def action_delete_connection(self) -> None:
        """Delete the selected connection."""
        tree = self.query_one("#object-tree", Tree)
        node = tree.cursor_node

        if not node or not node.data:
            return

        data = node.data
        if data[0] != "connection":
            return

        config = data[1]

        if self.current_config and self.current_config.name == config.name:
            self.notify("Disconnect first before deleting", severity="warning")
            return

        self.push_screen(
            ConfirmScreen(f"Delete '{config.name}'?"),
            lambda confirmed: self._do_delete_connection(config) if confirmed else None,
        )

    def _do_delete_connection(self, config: ConnectionConfig) -> None:
        """Actually delete the connection after confirmation."""
        self.connections = [c for c in self.connections if c.name != config.name]
        save_connections(self.connections)
        self.refresh_tree()
        self.notify(f"Connection '{config.name}' deleted")

    def action_connect_selected(self) -> None:
        """Connect to the selected connection."""
        tree = self.query_one("#object-tree", Tree)
        node = tree.cursor_node

        if not node or not node.data:
            return

        data = node.data
        if data[0] == "connection":
            config = data[1]
            # If already connected to this one, do nothing
            if self.current_config and self.current_config.name == config.name:
                return
            # If connected to another, disconnect first
            if self.current_connection:
                self._disconnect_silent()
            self.connect_to_server(config)

    def action_refresh_tree(self) -> None:
        """Refresh the object explorer."""
        self.refresh_tree()
        self.notify("Refreshed")

    def get_system_commands(self, screen):
        yield from super().get_system_commands(screen)
        yield SystemCommand(
            "Refresh Object Explorer",
            "Reload the object explorer tree",
            self.action_refresh_tree,
        )
        yield SystemCommand(
            "Test connections",
            "Attempt to connect to all saved connections",
            self.action_test_connections,
        )

    def action_collapse_tree(self) -> None:
        """Collapse all nodes in the object explorer."""
        tree = self.query_one("#object-tree", Tree)

        def collapse_all(node):
            for child in node.children:
                collapse_all(child)
                child.collapse()

        collapse_all(tree.root)
        self._expanded_paths.clear()
        self._save_expanded_state()

    def action_select_table(self) -> None:
        """Generate and execute SELECT query for selected table/view."""
        if not self.current_adapter:
            return

        tree = self.query_one("#object-tree", Tree)
        node = tree.cursor_node

        if not node or not node.data:
            return

        data = node.data
        if data[0] not in ("table", "view"):
            return

        db_name = data[1]
        obj_name = data[2]
        query_input = self.query_one("#query-input", TextArea)
        query_input.text = self.current_adapter.build_select_query(obj_name, 100, db_name)
        self.action_execute_query()

    def _update_footer_bindings(self) -> None:
        """Update footer with context-appropriate bindings."""
        # Don't update if a modal screen is open
        if len(self.screen_stack) > 1:
            return

        try:
            footer = self.query_one(ContextFooter)
            tree = self.query_one("#object-tree", Tree)
            query_input = self.query_one("#query-input", TextArea)
            results_table = self.query_one("#results-table", DataTable)
        except Exception:
            return

        left_bindings: list[KeyBinding] = []

        if tree.has_focus:
            node = tree.cursor_node
            node_type = None
            is_root = node == tree.root if node else False

            if node and node.data:
                node_type = node.data[0]

            if is_root or node_type is None:
                left_bindings.append(KeyBinding("n", "New Connection", "new_connection"))
                left_bindings.append(KeyBinding("R", "Refresh", "refresh_tree"))
                left_bindings.append(KeyBinding("f", "Fullscreen", "toggle_fullscreen"))

            elif node_type == "connection":
                config = node.data[1] if node and node.data else None
                is_current = self.current_config and config and config.name == self.current_config.name
                if is_current and self.current_connection:
                    left_bindings.append(KeyBinding("x", "Disconnect", "disconnect"))
                else:
                    left_bindings.append(KeyBinding("enter", "Connect", "connect_selected"))
                left_bindings.append(KeyBinding("n", "New", "new_connection"))
                left_bindings.append(KeyBinding("e", "Edit", "edit_connection"))
                left_bindings.append(KeyBinding("d", "Delete", "delete_connection"))
                left_bindings.append(KeyBinding("R", "Refresh", "refresh_tree"))
                left_bindings.append(KeyBinding("f", "Fullscreen", "toggle_fullscreen"))

            elif node_type in ("table", "view"):
                left_bindings.append(KeyBinding("enter", "Columns", "toggle_node"))
                left_bindings.append(KeyBinding("s", "Select TOP 100", "select_table"))
                left_bindings.append(KeyBinding("R", "Refresh", "refresh_tree"))
                left_bindings.append(KeyBinding("f", "Fullscreen", "toggle_fullscreen"))

            elif node_type == "database":
                left_bindings.append(KeyBinding("enter", "Expand", "toggle_node"))
                left_bindings.append(KeyBinding("R", "Refresh", "refresh_tree"))
                left_bindings.append(KeyBinding("f", "Fullscreen", "toggle_fullscreen"))

            elif node_type == "folder":
                left_bindings.append(KeyBinding("enter", "Expand", "toggle_node"))
                left_bindings.append(KeyBinding("R", "Refresh", "refresh_tree"))
                left_bindings.append(KeyBinding("f", "Fullscreen", "toggle_fullscreen"))

        elif query_input.has_focus:
            if self.vim_mode == VimMode.NORMAL:
                left_bindings.append(KeyBinding("i", "Insert Mode", "enter_insert_mode"))
                if self.current_connection:
                    left_bindings.append(KeyBinding("enter", "Execute", "execute_query"))
                    left_bindings.append(KeyBinding("h", "History", "show_history"))
                left_bindings.append(KeyBinding("d", "Clear", "clear_query"))
                left_bindings.append(KeyBinding("n", "New", "new_query"))
                left_bindings.append(KeyBinding("f", "Fullscreen", "toggle_fullscreen"))
            else:
                left_bindings.append(KeyBinding("esc", "Normal Mode", "exit_insert_mode"))
                if self.current_connection:
                    left_bindings.append(KeyBinding("f5", "Execute", "execute_query_insert"))
                left_bindings.append(KeyBinding("tab", "Autocomplete", "autocomplete_accept"))

        elif results_table.has_focus:
            if self.current_connection:
                left_bindings.append(KeyBinding("enter", "Re-run", "execute_query"))
                left_bindings.append(KeyBinding("f", "Fullscreen", "toggle_fullscreen"))
                left_bindings.append(KeyBinding("v", "View cell", "view_cell"))
                left_bindings.append(KeyBinding("y", "Copy cell", "copy_cell"))
                left_bindings.append(KeyBinding("Y", "Copy row", "copy_row"))
                left_bindings.append(KeyBinding("a", "Copy all", "copy_results"))

        right_bindings: list[KeyBinding] = []
        if self.vim_mode != VimMode.INSERT:
            right_bindings.extend(
                [
                    KeyBinding("?", "Help", "show_help"),
                    KeyBinding("^p", "Commands", "command_palette"),
                    KeyBinding("^q", "Quit", "quit"),
                ]
            )
        else:
            right_bindings.append(KeyBinding("^q", "Quit", "quit"))

        footer.set_bindings(left_bindings, right_bindings)

    def action_show_help(self) -> None:
        """Show help with all keybindings."""
        help_text = """
[bold]Object Explorer:[/]
  enter    Connect/Expand/Columns
  s        Select TOP 100 (table/view)
  n        New connection
  e        Edit connection
  d        Delete connection
  R        Refresh
  f        Fullscreen explorer
  z        Collapse all
  x        Disconnect

[bold]Query Editor (Vim Mode):[/]
  i        Enter INSERT mode
  esc      Exit to NORMAL mode
  enter    Execute query (NORMAL)
  f5       Execute query (stay INSERT)
  h        Query history
  d        Clear query
  n        New query (clear all)
  tab      Accept autocomplete (INSERT)
  f        Fullscreen query

[bold]Panes (NORMAL mode):[/]
  e        Object Explorer
  q        Query
  r        Results

[bold]Results:[/]
  f        Fullscreen results
  v        View selected cell
  y        Copy selected cell
  Y        Copy selected row
  a        Copy all results

[bold]General:[/]
  ?        Show this help
  ^p       Command palette
  ^q       Quit
  (Cmd)    Test connections (Ctrl+P)
"""
        self.push_screen(HelpScreen(help_text))

    def on_descendant_focus(self, event) -> None:
        """Handle focus changes to update section labels and footer."""
        self._update_section_labels()
        try:
            query_input = self.query_one("#query-input", TextArea)
            if not query_input.has_focus and self.vim_mode == VimMode.INSERT:
                self.vim_mode = VimMode.NORMAL
                query_input.read_only = True
        except Exception:
            pass
        self._update_footer_bindings()
        self._update_status_bar()

    def on_descendant_blur(self, event) -> None:
        """Handle blur to update section labels."""
        self.call_later(self._update_section_labels)

    def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        """Update footer when tree selection changes."""
        self._update_footer_bindings()
