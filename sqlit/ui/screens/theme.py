"""Theme selection dialog screen."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import OptionList
from textual.widgets.option_list import Option

from ...widgets import Dialog

# Light themes (listed first)
LIGHT_THEMES = {
    "sqlit-light": "Sqlit Light",
    "textual-light": "Textual Light",
    "solarized-light": "Solarized Light",
    "catppuccin-latte": "Catppuccin Latte",
    "rose-pine-dawn": "Rose Pine Dawn",
}

# Dark themes
DARK_THEMES = {
    "textual-ansi": "Terminal Default",
    "sqlit": "Sqlit",
    "textual-dark": "Textual Dark",
    "nord": "Nord",
    "gruvbox": "Gruvbox",
    "tokyo-night": "Tokyo Night",
    "solarized-dark": "Solarized Dark",
    "monokai": "Monokai",
    "flexoki": "Flexoki",
    "catppuccin-mocha": "Catppuccin Mocha",
    "rose-pine": "Rose Pine",
    "rose-pine-moon": "Rose Pine Moon",
    "dracula": "Dracula",
    "hackerman": "Hackerman",
    "everforest": "Everforest",
    "kanagawa": "Kanagawa",
    "matte-black": "Matte Black",
    "ristretto": "Ristretto",
    "osaka-jade": "Osaka Jade",
}

# Combined for backwards compatibility and building the full list
THEME_LABELS = {**LIGHT_THEMES, **DARK_THEMES}


class ThemeScreen(ModalScreen[str | None]):
    """Modal screen for theme selection with live preview."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("enter", "select_option", "Select"),
    ]

    CSS = """
    ThemeScreen {
        align: center middle;
        background: transparent;
    }

    #theme-dialog {
        width: 40;
    }

    #theme-list {
        height: auto;
        max-height: 16;
        border: none;
    }

    #theme-list > .option-list--option {
        padding: 0 1;
    }
    """

    def __init__(self, current_theme: str):
        super().__init__()
        self.current_theme = current_theme
        self._original_theme = current_theme  # Store for restore on cancel
        self._theme_ids: list[str] = []

    def _build_theme_list(self) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        """Build categorized theme lists.

        Returns:
            Tuple of (light_themes, dark_themes) where each is a list of (id, name) tuples.
        """
        available = set(self.app.available_themes)
        light: list[tuple[str, str]] = []
        dark: list[tuple[str, str]] = []
        seen: set[str] = set()

        # Add light themes first
        for theme_id, theme_name in LIGHT_THEMES.items():
            if theme_id in available:
                light.append((theme_id, theme_name))
                seen.add(theme_id)

        # Add dark themes
        for theme_id, theme_name in DARK_THEMES.items():
            if theme_id in available:
                dark.append((theme_id, theme_name))
                seen.add(theme_id)

        # Add any unknown themes to dark section
        for theme_id in sorted(available - seen):
            theme_name = " ".join(part.capitalize() for part in theme_id.split("-"))
            dark.append((theme_id, theme_name))

        return light, dark

    def compose(self) -> ComposeResult:
        shortcuts = [("Select", "<enter>"), ("Cancel", "<esc>")]
        with Dialog(id="theme-dialog", title="Select Theme", shortcuts=shortcuts):
            options: list[Option] = []
            light_themes, dark_themes = self._build_theme_list()
            self._theme_ids = []

            # Add light themes section
            if light_themes:
                options.append(Option("─ Light ─", disabled=True))
                for theme_id, theme_name in light_themes:
                    prefix = "> " if theme_id == self.current_theme else "  "
                    options.append(Option(f"{prefix}{theme_name}", id=theme_id))
                    self._theme_ids.append(theme_id)

            # Add dark themes section
            if dark_themes:
                options.append(Option("─ Dark ─", disabled=True))
                for theme_id, theme_name in dark_themes:
                    prefix = "> " if theme_id == self.current_theme else "  "
                    options.append(Option(f"{prefix}{theme_name}", id=theme_id))
                    self._theme_ids.append(theme_id)

            yield OptionList(*options, id="theme-list")

    def on_mount(self) -> None:
        option_list = self.query_one("#theme-list", OptionList)
        option_list.focus()
        # Highlight current theme
        for i, theme_id in enumerate(self._theme_ids):
            if theme_id == self.current_theme:
                option_list.highlighted = i
                break

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted
    ) -> None:
        """Apply theme live as user browses options."""
        theme_id = event.option.id
        if theme_id and theme_id in self.app.available_themes:
            try:
                self.app.theme = theme_id
            except Exception:
                pass  # Ignore errors during preview

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self.dismiss(event.option.id)

    def action_select_option(self) -> None:
        option_list = self.query_one("#theme-list", OptionList)
        if option_list.highlighted is not None:
            option = option_list.get_option_at_index(option_list.highlighted)
            self.dismiss(option.id)

    def action_cancel(self) -> None:
        # Restore original theme on cancel
        if self._original_theme in self.app.available_themes:
            try:
                self.app.theme = self._original_theme
            except Exception:
                pass
        self.dismiss(None)
