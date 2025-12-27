"""Main Textual application for sqlit."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.lazy import Lazy
from textual.screen import ModalScreen
from textual.theme import Theme
from textual.timer import Timer
from textual.widgets import Static, TextArea, Tree
from textual.widgets.text_area import TextAreaTheme
from textual.worker import Worker
from rich.style import Style

from .config import (
    ConnectionConfig,
    load_connections,
    load_settings,
    save_settings,
)
from .db import DatabaseAdapter
from .mock_settings import apply_mock_environment, build_mock_profile_from_settings
from .mocks import MockProfile
from .omarchy import (
    DEFAULT_THEME,
    get_current_theme_name,
    get_matching_textual_theme,
    is_omarchy_installed,
)
from .state_machine import (
    UIStateMachine,
    get_leader_bindings,
)
from .ui.mixins import (
    AutocompleteMixin,
    ConnectionMixin,
    QueryMixin,
    ResultsFilterMixin,
    ResultsMixin,
    TreeFilterMixin,
    TreeMixin,
    UINavigationMixin,
)
from .widgets import (
    AutocompleteDropdown,
    ContextFooter,
    ResultsFilterInput,
    SqlitDataTable,
    TreeFilterInput,
    VimMode,
)


class SSMSTUI(
    TreeMixin,
    TreeFilterMixin,
    ConnectionMixin,
    QueryMixin,
    AutocompleteMixin,
    ResultsMixin,
    ResultsFilterMixin,
    UINavigationMixin,
    App,
):
    """Main SSMS TUI application."""

    TITLE = "sqlit"

    _SQLIT_THEMES = [
        Theme(
            name="sqlit",
            primary="#97CB93",
            secondary="#6D8DC4",
            accent="#6D8DC4",
            warning="#f59e0b",
            error="#BE728C",
            success="#4ADE80",
            foreground="#a9b1d6",
            background="#1A1B26",
            surface="#24283B",
            panel="#414868",
            dark=True,
            variables={
                "border": "#414868",
                "footer-background": "#24283B",
                "footer-key-foreground": "#7FA1DE",
                "button-color-foreground": "#1A1B26",
                "input-selection-background": "#2a3144 40%",
            },
        ),
        Theme(
            name="sqlit-light",
            primary="#2E7D32",
            secondary="#1565C0",
            accent="#1565C0",
            warning="#F57C00",
            error="#C62828",
            success="#2E7D32",
            foreground="#37474F",
            background="#FAFAFA",
            surface="#FFFFFF",
            panel="#ECEFF1",
            dark=False,
            variables={
                "border": "#B0BEC5",
                "footer-background": "#ECEFF1",
                "footer-key-foreground": "#1565C0",
                "button-color-foreground": "#FFFFFF",
                "input-selection-background": "#1565C0 25%",
            },
        ),
        Theme(
            name="hackerman",
            primary="#00FF21",
            secondary="#149414",
            accent="#00FF21",
            warning="#FFFF33",
            error="#FF0000",
            success="#00FF21",
            foreground="#00FF21",
            background="#0D0D0D",
            surface="#171B1F",
            panel="#1E2428",
            dark=True,
            variables={
                "border": "#0e6b0e",
                "footer-background": "#0D0D0D",
                "footer-key-foreground": "#00FF21",
                "button-color-foreground": "#0D0D0D",
                "input-selection-background": "#149414 40%",
            },
        ),
        Theme(
            name="everforest",
            primary="#A7C080",
            secondary="#83C092",
            accent="#7FBBB3",
            warning="#DBBC7F",
            error="#E67E80",
            success="#A7C080",
            foreground="#D3C6AA",
            background="#232A2E",
            surface="#2D353B",
            panel="#3D484D",
            dark=True,
            variables={
                "border": "#3D484D",
                "footer-background": "#2D353B",
                "footer-key-foreground": "#A7C080",
                "button-color-foreground": "#232A2E",
                "input-selection-background": "#3D484D 40%",
            },
        ),
        Theme(
            name="kanagawa",
            primary="#7E9CD8",
            secondary="#7FB4CA",
            accent="#D27E99",
            warning="#FFA066",
            error="#E46876",
            success="#98BB6C",
            foreground="#DCD7BA",
            background="#1F1F28",
            surface="#16161D",
            panel="#223249",
            dark=True,
            variables={
                "border": "#2D4F67",
                "footer-background": "#16161D",
                "footer-key-foreground": "#7E9CD8",
                "button-color-foreground": "#1F1F28",
                "input-selection-background": "#2D4F67 40%",
            },
        ),
        Theme(
            name="matte-black",
            primary="#FFFFFF",
            secondary="#888888",
            accent="#FFFFFF",
            warning="#FFA500",
            error="#FF4444",
            success="#44FF44",
            foreground="#CCCCCC",
            background="#000000",
            surface="#0A0A0A",
            panel="#1A1A1A",
            dark=True,
            variables={
                "border": "#333333",
                "footer-background": "#0A0A0A",
                "footer-key-foreground": "#FFFFFF",
                "button-color-foreground": "#000000",
                "input-selection-background": "#333333 40%",
            },
        ),
        Theme(
            name="ristretto",
            primary="#F5D1C8",
            secondary="#C9A9A6",
            accent="#F5D1C8",
            warning="#EBD9B4",
            error="#E67E7E",
            success="#A8C4A0",
            foreground="#D5C4A1",
            background="#1C1410",
            surface="#2D2420",
            panel="#3D322C",
            dark=True,
            variables={
                "border": "#4D3C36",
                "footer-background": "#2D2420",
                "footer-key-foreground": "#F5D1C8",
                "button-color-foreground": "#1C1410",
                "input-selection-background": "#3D322C 40%",
            },
        ),
        Theme(
            name="osaka-jade",
            primary="#7DCFB6",
            secondary="#6BA89D",
            accent="#7DCFB6",
            warning="#E8C57B",
            error="#E88B8B",
            success="#7DCFB6",
            foreground="#C5D1CE",
            background="#0D1512",
            surface="#1A2420",
            panel="#253530",
            dark=True,
            variables={
                "border": "#2D4038",
                "footer-background": "#1A2420",
                "footer-key-foreground": "#7DCFB6",
                "button-color-foreground": "#0D1512",
                "input-selection-background": "#253530 40%",
            },
        ),
        # Terminal Default - uses ANSI colors from terminal
        Theme(
            name="textual-ansi",
            primary="#FFFFFF",
            secondary="ansi_blue",
            accent="ansi_blue",
            warning="ansi_yellow",
            error="ansi_red",
            success="ansi_green",
            foreground="ansi_default",
            background="ansi_default",
            surface="ansi_default",
            panel="ansi_default",
            dark=True,
            variables={
                "border": "#555555",
            },
        ),
        # Built-in theme overrides (only adding border variable for contrast)
        Theme(
            name="dracula",
            primary="#BD93F9",
            secondary="#FF79C6",
            accent="#BD93F9",
            warning="#F1FA8C",
            error="#FF5555",
            success="#50FA7B",
            foreground="#F8F8F2",
            background="#282A36",
            surface="#2B2E3B",
            panel="#313442",
            dark=True,
            variables={
                "border": "#44475A",
                "button-color-foreground": "#282A36",
            },
        ),
        Theme(
            name="flexoki",
            primary="#205EA6",
            secondary="#DA702C",
            accent="#205EA6",
            warning="#D0A215",
            error="#D14D41",
            success="#879A39",
            foreground="#CECDC3",
            background="#100F0F",
            surface="#1C1B1A",
            panel="#282726",
            dark=True,
            variables={
                "border": "#282726",
                "input-cursor-foreground": "#5E409D",
                "input-cursor-background": "#FFFCF0",
                "input-selection-background": "#6F6E69 35%",
                "button-color-foreground": "#FFFCF0",
            },
        ),
        Theme(
            name="monokai",
            primary="#AE81FF",
            secondary="#66D9EF",
            accent="#AE81FF",
            warning="#E6DB74",
            error="#F92672",
            success="#A6E22E",
            foreground="#F8F8F2",
            background="#272822",
            surface="#2e2e2e",
            panel="#3E3D32",
            dark=True,
            variables={
                "border": "#3E3D32",
                "foreground-muted": "#797979",
                "input-selection-background": "#575b6190",
                "button-color-foreground": "#272822",
            },
        ),
        Theme(
            name="solarized-dark",
            primary="#268bd2",
            secondary="#2AA198",
            accent="#268bd2",
            warning="#B58900",
            error="#DC322F",
            success="#859900",
            foreground="#839496",
            background="#002b36",
            surface="#073642",
            panel="#073642",
            dark=True,
            variables={
                "border": "#586e75",
                "border-blurred": "#2f4a52",
                "button-color-foreground": "#fdf6e3",
                "footer-background": "#268bd2",
                "footer-key-foreground": "#fdf6e3",
                "footer-description-foreground": "#fdf6e3",
                "input-selection-background": "#073642",
            },
        ),
        Theme(
            name="tokyo-night",
            primary="#BB9AF7",
            secondary="#7AA2F7",
            accent="#BB9AF7",
            warning="#E0AF68",
            error="#F7768E",
            success="#9ECE6A",
            foreground="#C0CAF5",
            background="#1A1B26",
            surface="#24283B",
            panel="#414868",
            dark=True,
            variables={
                "border": "#414868",
                "button-color-foreground": "#24283B",
            },
        ),
        Theme(
            name="gruvbox",
            primary="#85A598",
            secondary="#FABD2F",
            accent="#85A598",
            warning="#FE8019",
            error="#FB4934",
            success="#B8BB26",
            foreground="#EBDBB2",
            background="#282828",
            surface="#3c3836",
            panel="#504945",
            dark=True,
            variables={
                "border": "#504945",
                "block-cursor-foreground": "#fbf1c7",
                "input-selection-background": "#689d6a40",
                "button-color-foreground": "#282828",
            },
        ),
        Theme(
            name="nord",
            primary="#88C0D0",
            secondary="#81A1C1",
            accent="#88C0D0",
            warning="#EBCB8B",
            error="#BF616A",
            success="#A3BE8C",
            foreground="#ECEFF4",
            background="#2E3440",
            surface="#3B4252",
            panel="#434C5E",
            dark=True,
            variables={
                "border": "#434C5E",
                "block-cursor-background": "#88C0D0",
                "block-cursor-foreground": "#2E3440",
                "block-cursor-text-style": "none",
                "footer-key-foreground": "#88C0D0",
                "input-selection-background": "#81a1c1 35%",
                "button-color-foreground": "#2E3440",
                "button-focus-text-style": "reverse",
            },
        ),
        Theme(
            name="textual-dark",
            primary="#0178D4",
            secondary="#004578",
            accent="#0178D4",
            warning="#ffa62b",
            error="#ba3c5b",
            success="#4EBF71",
            foreground="#e0e0e0",
            background="#121212",
            surface="#1e1e1e",
            panel="#2d2d2d",
            dark=True,
            variables={
                "border": "#2d2d2d",
            },
        ),
    ]

    _LIGHT_THEME_NAMES = {
        "sqlit-light",
        "textual-light",
        "solarized-light",
        "catppuccin-latte",
        "rose-pine-dawn",
    }

    # Custom TextArea themes for SQL syntax highlighting (no background colors - inherits from app theme)
    _SQLIT_TEXTAREA_THEMES: dict[str, TextAreaTheme] = {
        "sqlit-light": TextAreaTheme(
            name="sqlit-light",
            syntax_styles={
                "keyword": Style(color="#D73A49", bold=True),
                "keyword.operator": Style(color="#D73A49"),
                "string": Style(color="#032F62"),
                "string.special": Style(color="#032F62"),
                "comment": Style(color="#6A737D", italic=True),
                "number": Style(color="#005CC5"),
                "float": Style(color="#005CC5"),
                "operator": Style(color="#D73A49"),
                "punctuation": Style(color="#24292E"),
                "punctuation.bracket": Style(color="#24292E"),
                "punctuation.delimiter": Style(color="#24292E"),
                "function": Style(color="#6F42C1"),
                "function.call": Style(color="#6F42C1"),
                "type": Style(color="#005CC5"),
                "variable": Style(color="#24292E"),
                "constant": Style(color="#005CC5"),
                "identifier": Style(color="#24292E"),
            },
        ),
        "hackerman": TextAreaTheme(
            name="hackerman",
            syntax_styles={
                "keyword": Style(color="#00FF21", bold=True),
                "keyword.operator": Style(color="#00FF21"),
                "string": Style(color="#7CFC00"),
                "string.special": Style(color="#7CFC00"),
                "comment": Style(color="#228B22", italic=True),
                "number": Style(color="#00FA9A"),
                "float": Style(color="#00FA9A"),
                "operator": Style(color="#32CD32"),
                "punctuation": Style(color="#149414"),
                "punctuation.bracket": Style(color="#149414"),
                "punctuation.delimiter": Style(color="#149414"),
                "function": Style(color="#39FF14"),
                "function.call": Style(color="#39FF14"),
                "type": Style(color="#00FF7F"),
                "variable": Style(color="#90EE90"),
                "constant": Style(color="#00FA9A"),
                "identifier": Style(color="#00FF21"),
            },
        ),
    }

    CSS = """
    Screen {
        background: $surface;
    }

    TextArea {
        & > .text-area--cursor-line {
            background: transparent;
        }
        &:focus > .text-area--cursor-line {
            background: $surface-lighten-1;
        }
    }

    DataTable.flash-cell:focus > .datatable--cursor,
    DataTable.flash-row:focus > .datatable--cursor,
    DataTable.flash-all:focus > .datatable--cursor {
        background: $success 30%;
    }

    DataTable.flash-all {
        border: solid $success 30%;
    }

    .flash {
        background: $success 30%;
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
    }

    Screen.explorer-hidden #sidebar {
        display: none;
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
        border: round $border;
        padding: 1;
        margin: 0;
    }

    #object-tree {
        height: 1fr;
    }

    #main-panel {
        width: 1fr;
    }

    #query-area {
        height: 50%;
        border: round $border;
        padding: 1;
        margin: 0;
    }

    #query-input {
        height: 1fr;
        border: none;
    }

    #results-area {
        height: 50%;
        padding: 1;
        border: round $border;
        margin: 0;
    }

    #sidebar.active-pane,
    #query-area.active-pane,
    #results-area.active-pane {
        border: round $primary;
        border-title-color: $primary;
    }


    #results-area DataTable {
        height: 1fr;
    }

    /* FastDataTable header styling */
    DataTable > .datatable--header {
        background: $surface-lighten-1;
        color: $primary;
        text-style: bold;
    }

    DataTable:focus > .datatable--header {
        background: $primary 20%;
        color: $text;
    }

    /* FastDataTable already has zebra stripes with $primary 10% */

    #status-bar {
        height: 1;
        background: $surface-darken-1;
        padding: 0 1;
    }

    #sidebar,
    #query-area,
    #results-area {
        border-title-align: left;
        border-title-color: $border;
        border-title-background: $surface;
        border-title-style: bold;
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
        # Leader combo bindings - generated from keymap provider
        *get_leader_bindings(),
        # Regular bindings
        Binding("n", "new_connection", "New", show=False),
        Binding("s", "select_table", "Select", show=False),
        Binding("R", "refresh_tree", "Refresh", show=False),
        Binding("f", "refresh_tree", "Refresh", show=False),
        Binding("e", "edit_connection", "Edit", show=False),
        Binding("d", "delete_connection", "Delete", show=False),
        Binding("D", "duplicate_connection", "Duplicate", show=False),
        Binding("delete", "delete_connection", "Delete", show=False),
        Binding("x", "disconnect", "Disconnect", show=False),
        Binding("space", "leader_key", "Commands", show=False, priority=True),
        Binding("ctrl+q", "quit", "Quit", show=False),
        Binding("question_mark", "show_help", "Help", show=False),
        Binding("e", "focus_explorer", "Explorer", show=False),
        Binding("q", "focus_query", "Query", show=False),
        Binding("r", "focus_results", "Results", show=False),
        Binding("i", "enter_insert_mode", "Insert", show=False),
        Binding("escape", "exit_insert_mode", "Normal", show=False),
        Binding("enter", "execute_query", "Execute", show=False),
        Binding("f5", "execute_query_insert", "Execute", show=False),
        Binding("ctrl+enter", "execute_query_insert", "Execute", show=False),
        Binding("d", "clear_query", "Clear", show=False),
        Binding("n", "new_query", "New", show=False),
        Binding("h", "show_history", "History", show=False),
        Binding("z", "collapse_tree", "Collapse", show=False),
        Binding("j", "tree_cursor_down", "Down", show=False),
        Binding("k", "tree_cursor_up", "Up", show=False),
        Binding("u", "use_database", "Use as default", show=False),
        Binding("v", "view_cell", "View cell", show=False),
        Binding("u", "edit_cell", "Update cell", show=False),
        Binding("h", "results_cursor_left", "Left", show=False),
        Binding("j", "results_cursor_down", "Down", show=False),
        Binding("k", "results_cursor_up", "Up", show=False),
        Binding("l", "results_cursor_right", "Right", show=False),
        Binding("y", "copy_context", "Copy", show=False),
        Binding("Y", "copy_row", "Copy row", show=False),
        Binding("a", "copy_results", "Copy results", show=False),
        Binding("x", "clear_results", "Clear results", show=False),
        Binding("ctrl+z", "cancel_operation", "Cancel", show=False),
        Binding("ctrl+j", "autocomplete_next", "Next suggestion", show=False),
        Binding("ctrl+k", "autocomplete_prev", "Prev suggestion", show=False),
        Binding("slash", "tree_filter", "Filter", show=False),
        Binding("escape", "tree_filter_close", "Close filter", show=False),
        Binding("enter", "tree_filter_accept", "Select", show=False),
        Binding("n", "tree_filter_next", "Next match", show=False),
        Binding("N", "tree_filter_prev", "Prev match", show=False),
        # Results filter bindings
        Binding("slash", "results_filter", "Filter results", show=False),
        Binding("escape", "results_filter_close", "Close filter", show=False),
        Binding("enter", "results_filter_accept", "Select", show=False),
        Binding("n", "results_filter_next", "Next match", show=False),
        Binding("N", "results_filter_prev", "Prev match", show=False),
    ]

    def __init__(
        self,
        mock_profile: MockProfile | None = None,
        startup_connection: ConnectionConfig | None = None,
    ):
        super().__init__()
        self._mock_profile = mock_profile
        self._startup_connection = startup_connection
        self._startup_connect_config: ConnectionConfig | None = None
        self._debug_mode = os.environ.get("SQLIT_DEBUG") == "1"
        self._startup_profile = os.environ.get("SQLIT_PROFILE_STARTUP") == "1"
        self._startup_mark = self._parse_startup_mark(os.environ.get("SQLIT_STARTUP_MARK"))
        self._startup_init_time = time.perf_counter()
        self._startup_events: list[tuple[str, float]] = []
        self._launch_ms: float | None = None
        self._startup_stamp("init_start")
        self.connections: list[ConnectionConfig] = []
        self.current_connection: Any | None = None
        self.current_config: ConnectionConfig | None = None
        self.current_adapter: DatabaseAdapter | None = None
        self.current_ssh_tunnel: Any | None = None
        self.vim_mode: VimMode = VimMode.NORMAL
        self._expanded_paths: set[str] = set()
        self._loading_nodes: set[str] = set()
        self._session: Any | None = None
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
        self._results_table_counter: int = 0
        self._internal_clipboard: str = ""
        self._fullscreen_mode: str = "none"
        self._last_notification: str = ""
        self._last_notification_severity: str = "information"
        self._last_notification_time: str = ""
        self._notification_timer: Timer | None = None
        self._notification_history: list = []
        self._connection_failed: bool = False
        self._leader_timer: Timer | None = None
        self._leader_pending: bool = False
        self._dialog_open: bool = False
        self._last_active_pane: str | None = None
        self._query_worker: Worker[Any] | None = None
        self._query_executing: bool = False
        self._cancellable_query: Any | None = None
        self._spinner_index: int = 0
        self._spinner_timer: Timer | None = None
        # Schema indexing state
        self._schema_indexing: bool = False
        self._schema_worker: Worker[Any] | None = None
        self._schema_spinner_index: int = 0
        self._schema_spinner_timer: Timer | None = None
        self._table_metadata: dict = {}
        self._columns_loading: set[str] = set()
        self._state_machine = UIStateMachine()
        self._session_factory: Any | None = None
        self._last_query_table: dict | None = None
        # Omarchy theme sync state
        self._omarchy_theme_watcher: Timer | None = None
        self._omarchy_last_theme_name: str | None = None

        if mock_profile:
            self._session_factory = self._create_mock_session_factory(mock_profile)
        self._startup_stamp("init_end")

    def _create_mock_session_factory(self, profile: MockProfile) -> Any:
        """Create a session factory that uses mock adapters."""
        from .services import ConnectionSession

        def mock_adapter_factory(db_type: str) -> Any:
            """Return mock adapter for the given db type."""
            return profile.get_adapter(db_type)

        def mock_tunnel_factory(config: Any) -> Any:
            """Return no tunnel for mock connections."""
            return None, config.server, int(config.port or "0")

        def factory(config: Any) -> Any:
            return ConnectionSession.create(
                config,
                adapter_factory=mock_adapter_factory,
                tunnel_factory=mock_tunnel_factory,
            )

        return factory

    @property
    def object_tree(self) -> Tree:
        return self.query_one("#object-tree", Tree)

    @property
    def query_input(self) -> TextArea:
        return self.query_one("#query-input", TextArea)

    @property
    def results_table(self) -> SqlitDataTable:
        # The results table ID changes when replaced (results-table, results-table-1, etc.)
        # Query for any DataTable within the results-area container
        return self.query_one("#results-area DataTable")  # type: ignore[return-value]

    @property
    def sidebar(self) -> Any:
        return self.query_one("#sidebar")

    @property
    def main_panel(self) -> Any:
        return self.query_one("#main-panel")

    @property
    def query_area(self) -> Any:
        return self.query_one("#query-area")

    @property
    def results_area(self) -> Any:
        return self.query_one("#results-area")

    @property
    def status_bar(self) -> Static:
        return self.query_one("#status-bar", Static)

    @property
    def autocomplete_dropdown(self) -> Any:
        from .widgets import AutocompleteDropdown

        return self.query_one("#autocomplete-dropdown", AutocompleteDropdown)

    @property
    def tree_filter_input(self) -> TreeFilterInput:
        return self.query_one("#tree-filter", TreeFilterInput)

    @property
    def results_filter_input(self) -> ResultsFilterInput:
        return self.query_one("#results-filter", ResultsFilterInput)

    def push_screen(
        self,
        screen: Any,
        callback: Callable[[Any], None] | Callable[[Any], Awaitable[None]] | None = None,
        wait_for_dismiss: bool = False,
    ) -> Any:
        """Override push_screen to update footer when screen changes."""
        if wait_for_dismiss:
            future = super().push_screen(screen, callback, wait_for_dismiss=True)
            self._update_footer_bindings()
            self._update_dialog_state()
            return future
        mount = super().push_screen(screen, callback, wait_for_dismiss=False)
        self._update_footer_bindings()
        self._update_dialog_state()
        return mount

    def pop_screen(self) -> Any:
        """Override pop_screen to update footer when screen changes."""
        result = super().pop_screen()
        self._update_footer_bindings()
        self._update_dialog_state()
        return result

    def _update_dialog_state(self) -> None:
        """Track whether a modal dialog is open and update pane title styling."""
        self._dialog_open = any(isinstance(screen, ModalScreen) for screen in self.screen_stack)
        self._update_section_labels()

    def check_action(self, action: str, parameters: tuple) -> bool | None:
        """Check if an action is allowed in the current state.

        This method is pure - it only checks, never mutates state.
        State transitions happen in the action methods themselves.
        """
        return self._state_machine.check_action(self, action)

    def _compute_restart_argv(self) -> list[str]:
        """Compute a best-effort argv to restart the app."""
        # Linux provides the most reliable answer via /proc.
        try:
            cmdline_path = "/proc/self/cmdline"
            if os.path.exists(cmdline_path):
                raw = open(cmdline_path, "rb").read()
                parts = [p.decode(errors="surrogateescape") for p in raw.split(b"\0") if p]
                if parts:
                    return parts
        except Exception:
            pass

        # Fallback: sys.argv (good enough for most invocations).
        argv = [sys.argv[0], *sys.argv[1:]] if sys.argv else []
        if argv:
            return argv
        return [sys.executable]

    def restart(self) -> None:
        """Restart the current process in-place."""
        argv = getattr(self, "_restart_argv", None) or self._compute_restart_argv()
        exe = argv[0]
        # execv doesn't search PATH; use execvp for bare commands (e.g. "sqlit").
        if os.sep in exe:
            os.execv(exe, argv)
        else:
            os.execvp(exe, argv)

    def compose(self) -> ComposeResult:
        self._startup_stamp("compose_start")
        with Vertical(id="main-container"):
            with Horizontal(id="content"):
                with Vertical(id="sidebar"):
                    yield TreeFilterInput(id="tree-filter")
                    tree: Tree[Any] = Tree("Servers", id="object-tree")
                    tree.show_root = False
                    tree.guide_depth = 2
                    yield tree

                with Vertical(id="main-panel"):
                    with Container(id="query-area"):
                        yield TextArea(
                            "",
                            language="sql",
                            id="query-input",
                            read_only=True,
                        )
                        yield Lazy(AutocompleteDropdown(id="autocomplete-dropdown"))

                    with Container(id="results-area"):
                        yield ResultsFilterInput(id="results-filter")
                        yield Lazy(SqlitDataTable(id="results-table", zebra_stripes=True, show_header=False))

            yield Static("Not connected", id="status-bar")

        yield ContextFooter()
        self._startup_stamp("compose_end")

    def on_mount(self) -> None:
        """Initialize the app."""
        self._startup_stamp("on_mount_start")
        self._restart_argv = self._compute_restart_argv()

        for theme in self._SQLIT_THEMES:
            self.register_theme(theme)

        # Register custom TextArea themes for syntax highlighting
        for textarea_theme in self._SQLIT_TEXTAREA_THEMES.values():
            self.query_input.register_theme(textarea_theme)

        settings = load_settings()
        self._startup_stamp("settings_loaded")

        # Initialize Omarchy theme sync
        self._init_omarchy_theme(settings)

        # Apply matching TextArea theme for syntax highlighting
        self._apply_textarea_theme(self.theme)

        self._expanded_paths = set(settings.get("expanded_nodes", []))
        self._startup_stamp("settings_applied")

        self._apply_mock_settings(settings)

        if self._mock_profile:
            self.connections = self._mock_profile.connections.copy()
        else:
            self.connections = load_connections(load_credentials=False)
        if self._startup_connection:
            self._setup_startup_connection(self._startup_connection)
        self._startup_stamp("connections_loaded")

        self.refresh_tree()
        self._startup_stamp("tree_refreshed")

        self.object_tree.focus()
        self._startup_stamp("tree_focused")
        # Move cursor to first node if available
        if self.object_tree.root.children:
            self.object_tree.cursor_line = 0
        self._update_section_labels()
        self._maybe_restore_connection_screen()
        self._startup_stamp("restore_checked")
        if self._debug_mode:
            self.call_after_refresh(self._record_launch_ms)
        self.call_after_refresh(self._update_status_bar)
        self._update_footer_bindings()
        self._startup_stamp("footer_updated")
        if self._startup_connect_config:
            self.call_after_refresh(lambda: self.connect_to_server(self._startup_connect_config))  # type: ignore[arg-type]
        self._log_startup_timing()

    def _apply_mock_settings(self, settings: dict) -> None:
        apply_mock_environment(settings)
        if self._mock_profile:
            return
        mock_profile = build_mock_profile_from_settings(settings)
        if mock_profile:
            self._mock_profile = mock_profile
            self._session_factory = self._create_mock_session_factory(mock_profile)

    def _setup_startup_connection(self, config: ConnectionConfig) -> None:
        """Set up a startup connection to auto-connect after mount."""
        if not config.name:
            config.name = "Temp Connection"
        self._startup_connect_config = config

    def _startup_stamp(self, name: str) -> None:
        if not self._startup_profile:
            return
        self._startup_events.append((name, time.perf_counter()))

    def _log_startup_timing(self) -> None:
        if not self._startup_profile:
            return
        now = time.perf_counter()
        if self._startup_mark is not None:
            since_start = (now - self._startup_mark) * 1000
        else:
            since_start = None
        init_to_mount = (now - self._startup_init_time) * 1000

        parts = []
        if since_start is not None:
            parts.append(f"start_to_mount_ms={since_start:.2f}")
        parts.append(f"init_to_mount_ms={init_to_mount:.2f}")
        print(f"[sqlit] startup {' '.join(parts)}", file=sys.stderr)
        self._log_startup_steps()

        def after_refresh() -> None:
            now_refresh = time.perf_counter()
            if self._startup_mark is not None:
                start_to_refresh = (now_refresh - self._startup_mark) * 1000
            else:
                start_to_refresh = None
            init_to_refresh = (now_refresh - self._startup_init_time) * 1000

            self._log_startup_step("first_refresh", now_refresh)
            refresh_parts = []
            if start_to_refresh is not None:
                refresh_parts.append(f"start_to_first_refresh_ms={start_to_refresh:.2f}")
            refresh_parts.append(f"init_to_first_refresh_ms={init_to_refresh:.2f}")
            print(f"[sqlit] startup {' '.join(refresh_parts)}", file=sys.stderr)

        self.call_after_refresh(after_refresh)

    def _log_startup_steps(self) -> None:
        for name, ts in self._startup_events:
            self._log_startup_step(name, ts)

    def _log_startup_step(self, name: str, timestamp: float) -> None:
        if not self._startup_profile:
            return
        parts = [f"step={name}"]
        if self._startup_mark is not None:
            parts.append(f"start_ms={(timestamp - self._startup_mark) * 1000:.2f}")
        parts.append(f"init_ms={(timestamp - self._startup_init_time) * 1000:.2f}")
        print(f"[sqlit] startup {' '.join(parts)}", file=sys.stderr)

    def _get_restart_cache_path(self) -> Path:
        return Path(tempfile.gettempdir()) / "sqlit-driver-install-restore.json"

    @staticmethod
    def _parse_startup_mark(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _record_launch_ms(self) -> None:
        base = self._startup_mark if self._startup_mark is not None else self._startup_init_time
        self._launch_ms = (time.perf_counter() - base) * 1000
        self._update_status_bar()

    def _maybe_restore_connection_screen(self) -> None:
        """Restore an in-progress connection form after a driver-install restart."""
        cache_path = self._get_restart_cache_path()
        if not cache_path.exists():
            return

        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            try:
                cache_path.unlink(missing_ok=True)
            except Exception:
                pass
            return

        try:
            cache_path.unlink(missing_ok=True)
        except Exception:
            pass

        if not isinstance(payload, dict) or payload.get("version") != 1:
            return

        values = payload.get("values")
        if not isinstance(values, dict):
            return

        editing = bool(payload.get("editing"))
        original_name = payload.get("original_name")
        post_install_message = payload.get("post_install_message")
        active_tab = payload.get("active_tab")

        config = None
        if editing and isinstance(original_name, str) and original_name:
            config = next((c for c in self.connections if getattr(c, "name", None) == original_name), None)

        if config is None:
            from .config import ConnectionConfig

            config = ConnectionConfig(
                name=str(values.get("name", "")),
                db_type=str(values.get("db_type", "mssql") or "mssql"),
            )
            editing = False

        prefill_values = {
            "values": values,
            "active_tab": active_tab,
        }

        from .ui.screens import ConnectionScreen

        self._set_connection_screen_footer()
        self.push_screen(
            ConnectionScreen(
                config,
                editing=editing,
                prefill_values=prefill_values,
                post_install_message=post_install_message if isinstance(post_install_message, str) else None,
            ),
            self._wrap_connection_result,
        )

    def watch_theme(self, old_theme: str, new_theme: str) -> None:
        """Save theme whenever it changes."""
        settings = load_settings()
        settings["theme"] = new_theme
        save_settings(settings)

        # Apply matching TextArea theme for syntax highlighting
        self._apply_textarea_theme(new_theme)

    def _apply_textarea_theme(self, theme_name: str) -> None:
        """Apply a matching TextArea theme for syntax highlighting if available."""
        try:
            if theme_name in self._SQLIT_TEXTAREA_THEMES:
                self.query_input.theme = theme_name
            elif theme_name in self._LIGHT_THEME_NAMES:
                self.query_input.theme = "sqlit-light"
            else:
                self.query_input.theme = "css"
        except Exception:
            pass  # Ignore errors during theme application

    def _init_omarchy_theme(self, settings: dict) -> None:
        """Initialize theme on startup, with Omarchy matching if installed.

        Strategy:
        1. If Omarchy is installed, try to match the Omarchy theme to a Textual theme
        2. If a match is found and no user override is saved, use it and start watching for changes
        3. If no match or Omarchy not installed, use saved theme or default
        """
        saved_theme = settings.get("theme")

        # Check if Omarchy is installed
        if not is_omarchy_installed():
            # No Omarchy, use saved theme or default
            self._apply_theme_safe(saved_theme or DEFAULT_THEME)
            return

        # Omarchy is installed - match theme and start watcher
        matched_theme = get_matching_textual_theme(self.available_themes)
        self._omarchy_last_theme_name = get_current_theme_name()
        if (
            isinstance(saved_theme, str)
            and saved_theme in self.available_themes
            and saved_theme != matched_theme
        ):
            self._apply_theme_safe(saved_theme)
            return

        self._apply_theme_safe(matched_theme)
        self._start_omarchy_watcher()

    def _apply_theme_safe(self, theme_name: str) -> None:
        """Apply a theme with fallback to default on error."""
        try:
            self.theme = theme_name
        except Exception:
            try:
                self.theme = DEFAULT_THEME
            except Exception:
                self.theme = "sqlit"

    def _start_omarchy_watcher(self) -> None:
        """Start watching for Omarchy theme changes."""
        if self._omarchy_theme_watcher is not None:
            return  # Already watching

        # Check for theme changes every 2 seconds
        self._omarchy_theme_watcher = self.set_interval(2.0, self._check_omarchy_theme_change)

    def _stop_omarchy_watcher(self) -> None:
        """Stop watching for Omarchy theme changes."""
        if self._omarchy_theme_watcher is not None:
            self._omarchy_theme_watcher.stop()
            self._omarchy_theme_watcher = None

    def _check_omarchy_theme_change(self) -> None:
        """Check if the Omarchy theme has changed and apply if so."""
        current_name = get_current_theme_name()
        if current_name is None:
            return

        # Check if theme name changed
        if current_name != self._omarchy_last_theme_name:
            self._omarchy_last_theme_name = current_name
            self._apply_omarchy_theme()

    def _apply_omarchy_theme(self) -> None:
        """Match and apply the current Omarchy theme."""
        matched_theme = get_matching_textual_theme(self.available_themes)
        self._apply_theme_safe(matched_theme)
