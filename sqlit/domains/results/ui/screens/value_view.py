"""Value view screen for displaying cell contents."""

from __future__ import annotations

import json

from rich.syntax import Syntax
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Static

from sqlit.shared.ui.widgets import Dialog
from sqlit.shared.ui.widgets_json_tree import JSONTreeView


class ValueViewScreen(ModalScreen):
    """Modal screen for viewing a single (potentially long) value with tree/syntax toggle."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("enter", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
        Binding("y", "copy", "Copy"),
        Binding("t", "toggle_view", "Toggle View"),
        Binding("E", "expand_all", "Expand All", show=False),
        Binding("e", "expand_all", "Expand All", show=False),
        Binding("Z", "collapse_all", "Collapse All", show=False),
    ]

    CSS = """
    ValueViewScreen {
        align: center middle;
        background: transparent;
    }

    #value-dialog {
        width: 90;
        height: 70%;
    }

    #value-scroll {
        height: 1fr;
        border: solid $primary-darken-2;
        padding: 1;
    }
    
    #value-scroll.hidden {
        display: none;
    }

    #value-text {
        width: auto;
        height: auto;
    }
    
    #json-tree-modal {
        height: 1fr;
        border: solid $primary-darken-2;
    }
    
    #json-tree-modal.hidden {
        display: none;
    }
    
    #mode-hint {
        height: 1;
        background: $surface-darken-1;
        color: $text-muted;
        padding: 0 1;
    }
    """

    def __init__(self, value: str, title: str = "Value"):
        super().__init__()
        self._raw_value = value
        self._title = title
        self._is_json = False
        self._parsed_json: dict | list | None = None
        self._tree_mode = True
        self._detect_json()

    @property
    def value(self) -> str:
        return self._raw_value

    def _detect_json(self) -> None:
        """Check if the value is valid JSON."""
        import ast

        stripped = self._raw_value.strip()
        self._is_json = False
        self._parsed_json = None

        if stripped and stripped[0] in "{[":
            try:
                self._parsed_json = json.loads(stripped)
                self._is_json = True
                return
            except (json.JSONDecodeError, ValueError):
                pass
            try:
                parsed = ast.literal_eval(stripped)
                if isinstance(parsed, dict | list):
                    self._parsed_json = parsed
                    self._is_json = True
            except (ValueError, SyntaxError):
                pass

    def _format_syntax_value(self) -> str | Syntax:
        """Format value for syntax view."""
        if self._is_json and self._parsed_json is not None:
            formatted = json.dumps(self._parsed_json, indent=2, ensure_ascii=False)
            return Syntax(formatted, "json", theme="ansi_dark", word_wrap=True)
        return self._raw_value

    def compose(self) -> ComposeResult:
        shortcuts = [("Toggle", "t"), ("Copy", "y"), ("Close", "<enter>")]
        with Dialog(id="value-dialog", title=self._title, shortcuts=shortcuts):
            with VerticalScroll(id="value-scroll", classes="hidden"):
                yield Static(self._format_syntax_value(), id="value-text", markup=False)
            yield JSONTreeView("JSON", id="json-tree-modal", classes="hidden")
            yield Static("", id="mode-hint")

    def on_mount(self) -> None:
        self._rebuild()

    def _rebuild(self) -> None:
        """Rebuild the display based on current mode."""
        try:
            scroll_widget = self.query_one("#value-scroll", VerticalScroll)
            tree_widget = self.query_one("#json-tree-modal", JSONTreeView)
            hint_widget = self.query_one("#mode-hint", Static)

            if self._is_json and self._tree_mode and self._parsed_json is not None:
                scroll_widget.add_class("hidden")
                tree_widget.remove_class("hidden")

                tree_widget.set_json(self._parsed_json, self._title)
                tree_widget.focus()

                hint_widget.update("Syntax: [bold]t[/]  Expand: [bold]E[/]  Collapse: [bold]Z[/]")
            else:
                tree_widget.add_class("hidden")
                scroll_widget.remove_class("hidden")
                scroll_widget.focus()

                if self._is_json:
                    hint_widget.update("Tree: [bold]t[/]")
                else:
                    hint_widget.update("")
        except Exception:
            pass

    def action_dismiss(self) -> None:  # type: ignore[override]
        self.dismiss(None)

    def action_toggle_view(self) -> None:
        """Toggle between tree and syntax view."""
        if self._is_json:
            self._tree_mode = not self._tree_mode
            self._rebuild()

    def action_expand_all(self) -> None:
        """Expand all tree nodes."""
        if self._is_json and self._tree_mode:
            try:
                self.query_one("#json-tree-modal", JSONTreeView).action_expand_all()
            except Exception:
                pass

    def action_collapse_all(self) -> None:
        """Collapse all tree nodes."""
        if self._is_json and self._tree_mode:
            try:
                self.query_one("#json-tree-modal", JSONTreeView).action_collapse_all()
            except Exception:
                pass

    def action_copy(self) -> None:
        from sqlit.shared.ui.widgets import flash_widget

        copied = getattr(self.app, "_copy_text", None)
        if callable(copied):
            copied(self.value)
            try:
                if self._tree_mode and self._is_json:
                    flash_widget(self.query_one("#json-tree-modal"))
                else:
                    flash_widget(self.query_one("#value-text"))
            except Exception:
                pass
        else:
            self.notify("Copy unavailable", timeout=2)
