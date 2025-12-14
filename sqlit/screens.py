"""Modal screens for sqlit."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    DataTable,
    Input,
    OptionList,
    Select,
    Static,
    TabbedContent,
    TabPane,
    TextArea,
)
from textual.widgets.option_list import Option

from .adapters import get_adapter
from .config import (
    AUTH_TYPE_LABELS,
    AuthType,
    ConnectionConfig,
    DATABASE_TYPE_LABELS,
    DatabaseType,
)
from .fields import FieldDefinition, FieldGroup, FieldType
from .widgets import Dialog


class ConfirmScreen(ModalScreen):
    """Modal screen for confirmation dialogs."""

    BINDINGS = [
        Binding("y", "confirm", "Yes"),
        Binding("n", "cancel", "No"),
        Binding("escape", "cancel", "Cancel"),
        Binding("enter", "select_option", "Select"),
    ]

    CSS = """
    ConfirmScreen {
        align: center middle;
        background: transparent;
    }

    #confirm-dialog {
        width: 36;
    }

    #confirm-list {
        height: auto;
        border: none;
    }

    #confirm-list > .option-list--option {
        padding: 0;
    }
    """

    def __init__(self, title: str):
        super().__init__()
        self.title_text = title

    def compose(self) -> ComposeResult:
        with Dialog(id="confirm-dialog", title=self.title_text):
            option_list = OptionList(
                Option(r"\[Y] Yes", id="yes"),
                Option(r"\[N] No", id="no"),
                id="confirm-list",
            )
            yield option_list

    def on_mount(self) -> None:
        self.query_one("#confirm-list", OptionList).focus()

    def on_option_list_option_selected(self, event) -> None:
        self.dismiss(event.option.id == "yes")

    def action_select_option(self) -> None:
        option_list = self.query_one("#confirm-list", OptionList)
        if option_list.highlighted is not None:
            self.dismiss(
                option_list.get_option_at_index(option_list.highlighted).id == "yes"
            )

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


class HelpScreen(ModalScreen):
    """Modal screen showing keyboard shortcuts and navigation tips."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    CSS = """
    HelpScreen {
        align: center middle;
        background: transparent;
    }

    #help-dialog {
        width: 90;
        max-width: 90%;
        max-height: 90%;
    }

    #help-scroll {
        height: auto;
        background: $surface;
        border: none;
    }
    """

    def __init__(self, help_text: str):
        super().__init__()
        self.help_text = help_text

    def compose(self) -> ComposeResult:
        with Dialog(id="help-dialog", title="Help", subtitle="Esc Close"):
            with VerticalScroll(id="help-scroll"):
                yield Static(self.help_text)

    def action_dismiss(self) -> None:
        self.dismiss(None)


class ConnectionScreen(ModalScreen):
    """Modal screen for adding/editing a connection."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+s", "save", "Save", priority=True),
        Binding("ctrl+t", "test_connection", "Test", priority=True),
        Binding("tab", "next_field", "Next field", priority=True),
        Binding("shift+tab", "prev_field", "Previous field", priority=True),
    ]

    CSS = """
    ConnectionScreen {
        align: center middle;
        background: transparent;
    }

    #connection-dialog {
        width: 62;
        height: auto;
        max-height: 85%;
        border: solid $primary;
        background: $surface;
        padding: 1;
        border-title-align: left;
        border-title-color: $text-muted;
        border-title-background: $surface;
        border-title-style: bold;
        border-subtitle-align: right;
        border-subtitle-color: $primary;
        border-subtitle-background: $surface;
        border-subtitle-style: bold;
    }

    #connection-title {
        display: none;
    }

    #connection-dialog Input, #connection-dialog Select {
        margin-bottom: 0;
    }

    .field-container {
        position: relative;
        height: auto;
        border: solid $primary-darken-2;
        background: $surface;
        padding: 0;
        margin-top: 0;
        border-title-align: left;
        border-title-color: $text-muted;
        border-title-background: $surface;
        border-title-style: none;
    }

    .field-container.hidden {
        display: none;
    }

    .field-container.invalid {
        border: solid $error;
    }

    .field-container.focused {
        border: solid $primary;
    }

    .field-container.invalid.focused {
        border: solid $error;
    }

    .field-container Input {
        border: none;
        height: 1;
        padding: 0;
        background: $surface;
    }

    .field-container Input:focus {
        border: none;
        background-tint: $foreground 5%;
    }

    .field-container Select {
        border: none;
        background: $surface;
        padding: 0;
    }

    .field-container .select-field {
        border: none;
        background: $surface;
        padding: 0;
    }

    #connection-tabs {
        height: auto;
    }

    TabbedContent {
        height: auto;
    }

    TabbedContent > ContentSwitcher {
        height: auto;
    }

    TabPane {
        height: auto;
    }

    Tab:disabled {
        text-style: strike;
    }

    .field-group {
        height: auto;
    }

    .field-group.hidden {
        display: none;
    }

    .field-row {
        height: auto;
        width: 100%;
    }

    .field-flex {
        width: 1fr;
        height: auto;
    }

    .field-fixed {
        height: auto;
        margin-left: 1;
    }

    .select-field {
        height: auto;
        max-height: 6;
        padding: 0;
        margin-bottom: 0;
    }

    .select-field > .option-list--option {
        padding: 0 1;
    }

    .error-text {
        color: $error;
        height: auto;
    }

    .error-text.hidden {
        display: none;
    }

    #test-status {
        height: auto;
        color: $text-muted;
        margin-top: 0;
    }

    #test-error {
        height: 6;
        border: solid $primary-darken-2;
        background: $surface-darken-1;
        margin-top: 1;
    }

    #test-error.hidden {
        display: none;
    }
    """

    def __init__(self, config: ConnectionConfig | None = None, editing: bool = False):
        super().__init__()
        self.config = config
        self.editing = editing
        self._field_widgets: dict[str, Input | OptionList] = {}
        self._field_definitions: dict[str, FieldDefinition] = {}
        self._current_db_type: DatabaseType = self._get_initial_db_type()
        self._last_test_error: str = ""
        self._last_test_ok: bool | None = None
        self._focused_container_id: str | None = None

    def _get_initial_db_type(self) -> DatabaseType:
        """Get the initial database type from config."""
        if self.config:
            return self.config.get_db_type()
        return DatabaseType.MSSQL

    def _get_adapter_for_type(self, db_type: DatabaseType):
        """Get the adapter instance for a database type."""
        return get_adapter(db_type.value)

    def _get_field_value(self, field_name: str) -> str:
        """Get the current value of a field from config or default."""
        if self.config and hasattr(self.config, field_name):
            return getattr(self.config, field_name) or ""
        return ""

    def _get_current_form_values(self) -> dict:
        """Get all current form values as a dictionary."""
        values = {}
        for name, widget in self._field_widgets.items():
            if isinstance(widget, Input):
                values[name] = widget.value
            elif isinstance(widget, OptionList):
                field_def = self._field_definitions.get(name)
                if field_def and field_def.options and widget.highlighted is not None:
                    idx = widget.highlighted
                    if idx < len(field_def.options):
                        values[name] = field_def.options[idx].value
                    else:
                        values[name] = field_def.default
                else:
                    values[name] = field_def.default if field_def else ""
            elif isinstance(widget, Select):
                values[name] = str(widget.value) if widget.value is not None else ""
        return values

    def _create_field_widget(self, field_def: FieldDefinition, group_name: str) -> ComposeResult:
        """Create widgets for a field definition."""
        field_id = f"field-{field_def.name}"
        container_id = f"container-{field_def.name}"

        # Determine initial visibility
        initial_visible = True
        if field_def.visible_when:
            # Use config values for initial visibility check
            initial_values = {}
            if self.config:
                for attr in ["auth_type", "driver", "server", "port", "database", "username", "password", "file_path"]:
                    if hasattr(self.config, attr):
                        initial_values[attr] = getattr(self.config, attr) or ""
            initial_visible = field_def.visible_when(initial_values)

        hidden_class = "" if initial_visible else " hidden"

        if field_def.field_type == FieldType.SELECT:
            container = Container(id=container_id, classes=f"field-container{hidden_class}")
            container.border_title = field_def.label
            with container:
                if field_def.name == "auth_type":
                    select = Select(
                        options=[(opt.label, opt.value) for opt in field_def.options],
                        value=(self._get_field_value(field_def.name) or field_def.default),
                        allow_blank=False,
                        compact=True,
                        id=field_id,
                    )
                    self._field_widgets[field_def.name] = select
                    yield select
                else:
                    options = [Option(opt.label, id=opt.value) for opt in field_def.options]
                    option_list = OptionList(*options, id=field_id, classes="select-field")
                    self._field_widgets[field_def.name] = option_list
                    yield option_list
                self._field_definitions[field_def.name] = field_def
                yield Static("", id=f"error-{field_def.name}", classes="error-text hidden")
        else:
            # TEXT, PASSWORD, FILE all use Input
            value = self._get_field_value(field_def.name) or field_def.default
            container = Container(id=container_id, classes=f"field-container{hidden_class}")
            container.border_title = field_def.label
            with container:
                input_widget = Input(
                    value=value,
                    placeholder=field_def.placeholder,
                    id=field_id,
                    password=False,
                )
                self._field_widgets[field_def.name] = input_widget
                self._field_definitions[field_def.name] = field_def
                yield input_widget
                yield Static("", id=f"error-{field_def.name}", classes="error-text hidden")

    def _create_field_group(self, group: FieldGroup) -> ComposeResult:
        """Create widgets for a field group."""
        # Group fields by row_group
        row_groups: dict[str | None, list[FieldDefinition]] = {}
        for field_def in group.fields:
            row_key = field_def.row_group
            if row_key not in row_groups:
                row_groups[row_key] = []
            row_groups[row_key].append(field_def)

        with Container(classes="field-group"):
            for row_key, fields in row_groups.items():
                if row_key is None:
                    # Single field, not in a row
                    for field_def in fields:
                        yield from self._create_field_widget(field_def, group.name)
                else:
                    # Multiple fields in a horizontal row
                    with Horizontal(classes="field-row"):
                        for field_def in fields:
                            width_class = "field-flex" if field_def.width == "flex" else "field-fixed"
                            with Container(classes=width_class):
                                yield from self._create_field_widget(field_def, group.name)

    def _split_groups_by_advanced(
        self, groups: list[FieldGroup]
    ) -> tuple[list[FieldGroup], list[FieldGroup]]:
        general: list[FieldGroup] = []
        advanced: list[FieldGroup] = []
        for group in groups:
            general_fields = [f for f in group.fields if not f.advanced]
            advanced_fields = [f for f in group.fields if f.advanced]
            if general_fields:
                general.append(
                    FieldGroup(
                        name=group.name,
                        fields=general_fields,
                        visible_when=group.visible_when,
                    )
                )
            if advanced_fields:
                advanced.append(
                    FieldGroup(
                        name=group.name,
                        fields=advanced_fields,
                        visible_when=group.visible_when,
                    )
                )
        return general, advanced

    def _set_advanced_tab_enabled(self, enabled: bool) -> None:
        """Enable/disable the Advanced tab (disabled tabs are struck through)."""
        try:
            tabs = self.query_one("#connection-tabs", TabbedContent)
            advanced_pane = self.query_one("#tab-advanced", TabPane)
        except Exception:
            return

        advanced_pane.disabled = not enabled
        try:
            tab = tabs.get_tab(advanced_pane)
            tab.disabled = not enabled
        except Exception:
            pass

        if not enabled:
            try:
                if tabs.active == advanced_pane.id:
                    tabs.active = "tab-general"
            except Exception:
                pass

    def compose(self) -> ComposeResult:
        title = "Edit Connection" if self.editing else "New Connection"
        db_type = self._get_initial_db_type()

        with Dialog(
            id="connection-dialog",
            title=title,
            subtitle="^T Test  ^S Save  Esc Cancel",
        ):

            with TabbedContent(id="connection-tabs"):
                with TabPane("General", id="tab-general"):
                    name_container = Container(
                        id="container-name", classes="field-container"
                    )
                    name_container.border_title = "Name"
                    with name_container:
                        yield Input(
                            value=self.config.name if self.config else "",
                            placeholder="",
                            id="conn-name",
                        )
                        yield Static("", id="error-name", classes="error-text hidden")

                    db_types = list(DatabaseType)
                    dbtype_container = Container(
                        id="container-dbtype", classes="field-container"
                    )
                    dbtype_container.border_title = "Database Type"
                    with dbtype_container:
                        yield Select(
                            options=[(DATABASE_TYPE_LABELS[dt], dt.value) for dt in db_types],
                            value=db_type.value,
                            allow_blank=False,
                            compact=True,
                            id="dbtype-select",
                        )

                    with Container(id="dynamic-fields-general"):
                        adapter = self._get_adapter_for_type(db_type)
                        general_groups, _advanced_groups = self._split_groups_by_advanced(
                            adapter.get_connection_fields()
                        )
                        for group in general_groups:
                            yield from self._create_field_group(group)

                with TabPane("Advanced", id="tab-advanced"):
                    with Container(id="dynamic-fields-advanced"):
                        adapter = self._get_adapter_for_type(db_type)
                        _general_groups, advanced_groups = self._split_groups_by_advanced(
                            adapter.get_connection_fields()
                        )
                        for group in advanced_groups:
                            yield from self._create_field_group(group)

            yield Static("", id="test-status")
            yield TextArea("", id="test-error", read_only=True, classes="hidden")

    def on_mount(self) -> None:
        self.query_one("#conn-name", Input).focus()

        # Set initial values for select fields
        self._set_initial_select_values()
        self._update_field_visibility()
        self._validate_name_unique()
        adapter = self._get_adapter_for_type(self._current_db_type)
        _general, advanced = self._split_groups_by_advanced(adapter.get_connection_fields())
        self._set_advanced_tab_enabled(bool(advanced))

    def on_descendant_focus(self, event) -> None:
        focused = self.focused
        if focused is None:
            return

        container_id: str | None = None
        focused_id = getattr(focused, "id", None)
        if focused_id == "conn-name":
            container_id = "container-name"
        elif focused_id == "dbtype-select":
            container_id = "container-dbtype"
        elif focused_id and str(focused_id).startswith("field-"):
            field_name = str(focused_id).removeprefix("field-")
            container_id = f"container-{field_name}"

        if container_id is None:
            return

        if self._focused_container_id and self._focused_container_id != container_id:
            try:
                self.query_one(
                    f"#{self._focused_container_id}", Container
                ).remove_class("focused")
            except Exception:
                pass

        self._focused_container_id = container_id
        try:
            self.query_one(f"#{container_id}", Container).add_class("focused")
        except Exception:
            pass

    def _set_initial_select_values(self) -> None:
        """Set initial highlighted values for select fields based on config."""
        for name, widget in self._field_widgets.items():
            if isinstance(widget, OptionList):
                field_def = self._field_definitions.get(name)
                if not field_def:
                    continue

                # Get the value from config or default
                value = self._get_field_value(name) or field_def.default

                # Find the index of this value in options
                for i, opt in enumerate(field_def.options):
                    if opt.value == value:
                        widget.highlighted = i
                        break
            elif isinstance(widget, Select):
                field_def = self._field_definitions.get(name)
                if not field_def:
                    continue
                value = self._get_field_value(name) or field_def.default
                widget.value = value

    def _rebuild_dynamic_fields(self, db_type: DatabaseType) -> None:
        """Rebuild the dynamic fields for a new database type."""
        self._current_db_type = db_type
        self._field_widgets.clear()
        self._field_definitions.clear()

        general_container = self.query_one("#dynamic-fields-general", Container)
        advanced_container = self.query_one("#dynamic-fields-advanced", Container)
        general_container.remove_children()
        advanced_container.remove_children()

        adapter = self._get_adapter_for_type(db_type)
        general_groups, advanced_groups = self._split_groups_by_advanced(
            adapter.get_connection_fields()
        )
        self._set_advanced_tab_enabled(bool(advanced_groups))
        for group in general_groups:
            for widget in self._create_field_group_widgets(group):
                general_container.mount(widget)
        for group in advanced_groups:
            for widget in self._create_field_group_widgets(group):
                advanced_container.mount(widget)

    def _create_field_group_widgets(self, group: FieldGroup) -> list:
        """Create widget instances for a field group (for mounting)."""
        widgets = []

        # Group fields by row_group
        row_groups: dict[str | None, list[FieldDefinition]] = {}
        for field_def in group.fields:
            row_key = field_def.row_group
            if row_key not in row_groups:
                row_groups[row_key] = []
            row_groups[row_key].append(field_def)

        group_container = Container(classes="field-group")

        for row_key, fields in row_groups.items():
            if row_key is None:
                # Single fields
                for field_def in fields:
                    for w in self._create_field_widget_instances(field_def, group.name):
                        group_container.compose_add_child(w)
            else:
                # Row of fields
                row = Horizontal(classes="field-row")
                for field_def in fields:
                    width_class = "field-flex" if field_def.width == "flex" else "field-fixed"
                    field_container = Container(classes=width_class)
                    for w in self._create_field_widget_instances(field_def, group.name):
                        field_container.compose_add_child(w)
                    row.compose_add_child(field_container)
                group_container.compose_add_child(row)

        widgets.append(group_container)
        return widgets

    def _create_field_widget_instances(self, field_def: FieldDefinition, group_name: str) -> list:
        """Create widget instances for a field (for mounting)."""
        widgets = []
        field_id = f"field-{field_def.name}"
        container_id = f"container-{field_def.name}"

        # Determine initial visibility
        initial_visible = True
        if field_def.visible_when:
            initial_values = self._get_current_form_values()
            initial_visible = field_def.visible_when(initial_values)

        hidden_class = "" if initial_visible else " hidden"

        container = Container(id=container_id, classes=f"field-container{hidden_class}")
        container.border_title = field_def.label

        if field_def.field_type == FieldType.SELECT:
            if field_def.name == "auth_type":
                select = Select(
                    options=[(opt.label, opt.value) for opt in field_def.options],
                    value=(self._get_field_value(field_def.name) or field_def.default),
                    allow_blank=False,
                    compact=True,
                    id=field_id,
                )
                self._field_widgets[field_def.name] = select
                container.compose_add_child(select)
            else:
                options = [Option(opt.label, id=opt.value) for opt in field_def.options]
                option_list = OptionList(*options, id=field_id, classes="select-field")
                self._field_widgets[field_def.name] = option_list
                container.compose_add_child(option_list)
            self._field_definitions[field_def.name] = field_def
            container.compose_add_child(
                Static("", id=f"error-{field_def.name}", classes="error-text hidden")
            )
        else:
            value = self._get_field_value(field_def.name) or field_def.default
            input_widget = Input(
                value=value,
                placeholder=field_def.placeholder,
                id=field_id,
                password=False,
            )
            self._field_widgets[field_def.name] = input_widget
            self._field_definitions[field_def.name] = field_def
            container.compose_add_child(input_widget)
            container.compose_add_child(
                Static("", id=f"error-{field_def.name}", classes="error-text hidden")
            )

        widgets.append(container)
        return widgets

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "dbtype-select":
            try:
                db_type = DatabaseType(str(event.value))
            except Exception:
                return
            if db_type != self._current_db_type:
                self._rebuild_dynamic_fields(db_type)
                self._set_initial_select_values()
                self._update_field_visibility()
                self._focus_first_required()
            return

        if event.select.id and str(event.select.id).startswith("field-"):
            self._update_field_visibility()

    def on_option_list_option_highlighted(self, event) -> None:
        # A select field changed - update visibility of dependent fields
        if event.option_list.id and event.option_list.id.startswith("field-"):
            self._update_field_visibility()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "conn-name":
            self._validate_name_unique()

    def _update_field_visibility(self) -> None:
        """Update visibility of fields based on current form values."""
        values = self._get_current_form_values()

        for name, field_def in self._field_definitions.items():
            container = self.query_one(f"#container-{name}", Container)
            should_show = True
            if field_def.visible_when:
                should_show = bool(field_def.visible_when(values))
            if should_show:
                container.remove_class("hidden")
            else:
                container.add_class("hidden")

    def _get_focusable_fields(self) -> list:
        """Get list of focusable fields in order."""
        fields = [
            self.query_one("#conn-name", Input),
            self.query_one("#dbtype-select", Select),
        ]

        # Add all visible field widgets
        for name, widget in self._field_widgets.items():
            try:
                container = self.query_one(f"#container-{name}", Container)
                if "hidden" not in container.classes:
                    fields.append(widget)
            except Exception:
                pass

        return fields

    def _clear_field_error(self, name: str) -> None:
        try:
            container = self.query_one(f"#container-{name}", Container)
            container.remove_class("invalid")
        except Exception:
            pass
        try:
            error = self.query_one(f"#error-{name}", Static)
            error.update("")
            error.add_class("hidden")
        except Exception:
            pass

    def _set_field_error(self, name: str, message: str) -> None:
        try:
            container = self.query_one(f"#container-{name}", Container)
            container.add_class("invalid")
        except Exception:
            pass
        try:
            error = self.query_one(f"#error-{name}", Static)
            # Keep text minimal; border color is the primary indicator.
            error.update("" if message == "Required." else message)
            if message == "Required.":
                error.add_class("hidden")
            else:
                error.remove_class("hidden")
        except Exception:
            pass

    def _validate_name_unique(self) -> None:
        self._clear_field_error("name")
        name = self.query_one("#conn-name", Input).value.strip()
        if not name:
            return
        existing = []
        try:
            existing = getattr(self.app, "connections", []) or []
        except Exception:
            existing = []

        if self.editing and self.config and name == self.config.name:
            return
        if any(getattr(c, "name", None) == name for c in existing):
            self._set_field_error("name", "Name already exists.")

    def _focus_first_required(self) -> None:
        values = self._get_current_form_values()
        for field_name, field_def in self._field_definitions.items():
            if not field_def.required:
                continue
            is_visible = True
            if field_def.visible_when:
                is_visible = bool(field_def.visible_when(values))
            if field_def.advanced and not self._show_advanced:
                is_visible = False
            if not is_visible:
                continue
            widget = self._field_widgets.get(field_name)
            if isinstance(widget, Input) and not widget.value.strip():
                widget.focus()
                return
            if isinstance(widget, OptionList) and widget.highlighted is None:
                widget.focus()
                return

    def action_next_field(self) -> None:
        fields = self._get_focusable_fields()
        focused = self.focused
        if focused in fields:
            idx = fields.index(focused)
            next_idx = (idx + 1) % len(fields)
            fields[next_idx].focus()
        elif fields:
            fields[0].focus()

    def action_prev_field(self) -> None:
        fields = self._get_focusable_fields()
        focused = self.focused
        if focused in fields:
            idx = fields.index(focused)
            prev_idx = (idx - 1) % len(fields)
            fields[prev_idx].focus()
        elif fields:
            fields[-1].focus()

    def _get_config(self) -> ConnectionConfig | None:
        """Build a ConnectionConfig from the current form values."""
        self._clear_field_error("name")
        name_input = self.query_one("#conn-name", Input)
        name = name_input.value.strip()

        # Get selected database type
        db_type_value = self.query_one("#dbtype-select", Select).value
        try:
            db_type = DatabaseType(str(db_type_value))
        except Exception:
            db_type = DatabaseType.MSSQL

        # Collect all field values
        values = self._get_current_form_values()

        # Name suggestion
        if not name:
            suggestion = ""
            if db_type in (DatabaseType.SQLITE, DatabaseType.DUCKDB):
                fp = values.get("file_path", "").strip()
                suggestion = fp.split("/")[-1] if fp else db_type.value
            else:
                server = values.get("server", "").strip()
                suggestion = f"{db_type.value}-{server}" if server else db_type.value
            suggestion = suggestion.replace(" ", "-")[:40] or "connection"
            name_input.value = suggestion
            name = suggestion

        self._validate_name_unique()
        try:
            if "hidden" not in self.query_one("#error-name", Static).classes:
                name_input.focus()
                return None
        except Exception:
            pass

        # Validate required fields
        for field_name, field_def in self._field_definitions.items():
            self._clear_field_error(field_name)
            if field_def.required:
                # Check if field is visible
                is_visible = True
                if field_def.visible_when:
                    is_visible = field_def.visible_when(values)
                if field_def.advanced and not self._show_advanced:
                    is_visible = False

                if is_visible and not values.get(field_name):
                    self._set_field_error(field_name, "Required.")
                    return None

        # File path validation
        if db_type in (DatabaseType.SQLITE, DatabaseType.DUCKDB):
            from pathlib import Path

            fp = values.get("file_path", "").strip()
            if not fp:
                self._set_field_error("file_path", "Required.")
                return None
            if not Path(fp).exists():
                self._set_field_error("file_path", "File not found.")
                return None

        # Build config based on database type
        config_kwargs = {
            "name": name,
            "db_type": db_type.value,
        }

        # Add all field values to config
        for field_name, value in values.items():
            config_kwargs[field_name] = value

        # Handle SQL Server specific fields
        if db_type == DatabaseType.MSSQL:
            auth_type = values.get("auth_type", "sql")
            config_kwargs["trusted_connection"] = (auth_type == "windows")

        return ConnectionConfig(**config_kwargs)

    def _get_package_install_hint(self, db_type: str) -> str | None:
        """Get pip install command for missing database packages."""
        hints = {
            "postgresql": "pip install psycopg2-binary",
            "mysql": "pip install mysql-connector-python",
            "oracle": "pip install oracledb",
            "mariadb": "pip install mariadb",
            "duckdb": "pip install duckdb",
            "cockroachdb": "pip install psycopg2-binary",
        }
        return hints.get(db_type)

    def action_test_connection(self) -> None:
        """Test the connection without saving or closing."""
        config = self._get_config()
        if not config:
            return

        self.query_one("#test-error", TextArea).add_class("hidden")
        self.query_one("#test-status", Static).update("Testing…")
        self._last_test_ok = None
        self._last_test_error = ""
        try:
            adapter = get_adapter(config.db_type)
            conn = adapter.connect(config)
            conn.close()
            try:
                set_health = getattr(self.app, "_set_connection_health", None)
                if callable(set_health):
                    set_health(config.name, True)
            except Exception:
                pass
            self._last_test_ok = True
            self.query_one("#test-status", Static).update("Last test: OK")
        except ModuleNotFoundError as e:
            hint = self._get_package_install_hint(config.db_type)
            if hint:
                self.query_one("#test-status", Static).update(f"Last test: failed (missing package)")
                err = self.query_one("#test-error", TextArea)
                err.text = f"{e}\n\nInstall with:\n  {hint}"
                err.remove_class("hidden")
                self._last_test_error = err.text
            else:
                self.query_one("#test-status", Static).update("Last test: failed")
                err = self.query_one("#test-error", TextArea)
                err.text = f"{e}"
                err.remove_class("hidden")
                self._last_test_error = err.text
            self._last_test_ok = False
        except ImportError as e:
            hint = self._get_package_install_hint(config.db_type)
            if hint:
                self.query_one("#test-status", Static).update("Last test: failed (missing package)")
                err = self.query_one("#test-error", TextArea)
                err.text = f"{e}\n\nInstall with:\n  {hint}"
                err.remove_class("hidden")
                self._last_test_error = err.text
            else:
                self.query_one("#test-status", Static).update("Last test: failed")
                err = self.query_one("#test-error", TextArea)
                err.text = f"{e}"
                err.remove_class("hidden")
                self._last_test_error = err.text
            self._last_test_ok = False
        except Exception as e:
            try:
                set_health = getattr(self.app, "_set_connection_health", None)
                if callable(set_health):
                    set_health(config.name, False)
            except Exception:
                pass
            self._last_test_ok = False
            self.query_one("#test-status", Static).update("Last test: failed")
            err = self.query_one("#test-error", TextArea)
            err.text = str(e)
            err.remove_class("hidden")
            self._last_test_error = err.text

    def action_save(self) -> None:
        config = self._get_config()
        if config:
            self.dismiss(("save", config))

    def action_cancel(self) -> None:
        self.dismiss(None)


class ValueViewScreen(ModalScreen):
    """Modal screen for viewing a single (potentially long) value."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
        Binding("y", "copy", "Copy"),
    ]

    CSS = """
    ValueViewScreen {
        align: center middle;
        background: transparent;
    }

    #value-dialog {
        width: 90;
        height: 70%;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }

    #value-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #value-text {
        height: 1fr;
        border: solid $primary-darken-2;
    }

    #value-footer {
        height: 1;
        margin-top: 1;
        text-align: center;
        color: $text-muted;
    }
    """

    def __init__(self, value: str, title: str = "Value"):
        super().__init__()
        self.value = value
        self.title = title

    def compose(self) -> ComposeResult:
        with Container(id="value-dialog"):
            yield Static(self.title, id="value-title")
            yield TextArea(self.value, id="value-text", read_only=True)
            yield Static(r"[bold]\[Y][/] Copy  [bold]\[Esc][/] Close", id="value-footer")

    def on_mount(self) -> None:
        self.query_one("#value-text", TextArea).focus()

    def action_dismiss(self) -> None:
        self.dismiss(None)

    def action_copy(self) -> None:
        copied = getattr(self.app, "_copy_text", None)
        if callable(copied):
            copied(self.value)
        else:
            self.notify("Copy unavailable", timeout=2)


class DriverSetupScreen(ModalScreen):
    """Screen for setting up ODBC drivers."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("enter", "select", "Select"),
        Binding("i", "install_driver", "Install"),
    ]

    CSS = """
    DriverSetupScreen {
        align: center middle;
        background: transparent;
    }

    #driver-dialog {
        width: 80;
        height: auto;
        max-height: 90%;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }

    #driver-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #driver-message {
        margin-bottom: 1;
    }

    #driver-list {
        height: auto;
        max-height: 8;
        background: $surface;
        border: solid $primary-darken-2;
        margin-bottom: 1;
    }

    #install-commands {
        height: auto;
        max-height: 12;
        background: $surface-darken-1;
        padding: 1;
        margin-top: 1;
        overflow-y: auto;
    }

    #driver-footer {
        margin-top: 1;
        text-align: center;
    }
    """

    def __init__(self, installed_drivers: list[str] | None = None):
        super().__init__()
        self.installed_drivers = installed_drivers or []
        self._install_commands: list[str] = []

    def compose(self) -> ComposeResult:
        from .drivers import SUPPORTED_DRIVERS, get_install_commands, get_os_info

        os_type, os_version = get_os_info()
        has_drivers = len(self.installed_drivers) > 0

        with Container(id="driver-dialog"):
            if has_drivers:
                yield Static("Select ODBC Driver", id="driver-title")
                yield Static(
                    f"Found {len(self.installed_drivers)} installed driver(s):",
                    id="driver-message",
                )
            else:
                yield Static("No ODBC Driver Found", id="driver-title")
                yield Static(
                    f"Detected OS: [bold]{os_type}[/] {os_version}\n"
                    "You need an ODBC driver to connect to SQL Server.",
                    id="driver-message",
                )

            # Show installed drivers or available options
            options = []
            if has_drivers:
                for driver in self.installed_drivers:
                    options.append(Option(f"[green]{driver}[/]", id=driver))
            else:
                for driver in SUPPORTED_DRIVERS[:3]:  # Show top 3 options
                    options.append(Option(f"[dim]{driver}[/] (not installed)", id=driver))

            yield OptionList(*options, id="driver-list")

            # Show install commands if no drivers
            if not has_drivers:
                install_info = get_install_commands()
                if install_info:
                    self._install_commands = install_info.commands
                    commands_text = "\n".join(install_info.commands)
                    yield Static(
                        f"[bold]{install_info.description}:[/]\n\n{commands_text}",
                        id="install-commands",
                    )

            footer_text = r"[bold]\[Enter][/] Select"
            if not has_drivers:
                footer_text += r"  [bold]\[I][/] Install"
            footer_text += r"  [bold]\[Esc][/] Cancel"
            yield Static(footer_text, id="driver-footer")

    def on_mount(self) -> None:
        self.query_one("#driver-list", OptionList).focus()

    def action_select(self) -> None:
        option_list = self.query_one("#driver-list", OptionList)
        if option_list.highlighted is not None:
            option = option_list.get_option_at_index(option_list.highlighted)
            self.dismiss(("select", option.id))

    def on_option_list_option_selected(self, event) -> None:
        self.dismiss(("select", event.option.id))

    def action_install_driver(self) -> None:
        """Run installation commands for the selected driver."""
        if not self._install_commands:
            self.notify("No installation commands available", severity="warning")
            return

        from .drivers import get_os_info
        os_type, _ = get_os_info()

        # On Windows, just show instructions
        if os_type == "windows":
            self.notify(
                "Please download and run the installer from Microsoft",
                severity="information",
            )
            return

        self.notify("Installing driver... This may ask for your password.", timeout=5)
        self.dismiss(("install", self._install_commands))

    def action_cancel(self) -> None:
        self.dismiss(None)


class QueryHistoryScreen(ModalScreen):
    """Modal screen for query history selection."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("enter", "select", "Select"),
        Binding("d", "delete", "Delete"),
    ]

    CSS = """
    QueryHistoryScreen {
        align: center middle;
        background: transparent;
    }

    #history-dialog {
        width: 90;
        height: 80%;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }

    #history-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #history-list {
        height: 1fr;
        background: $surface;
        border: solid $primary-darken-2;
        padding: 0;
    }

    #history-list > .option-list--option {
        padding: 0 1;
    }

    #history-preview {
        height: 8;
        background: $surface-darken-1;
        border: solid $primary-darken-2;
        padding: 1;
        margin-top: 1;
        overflow-y: auto;
    }

    #history-footer {
        margin-top: 1;
        text-align: center;
        color: $text-muted;
    }
    """

    def __init__(self, history: list, connection_name: str):
        super().__init__()
        self.history = history  # list of QueryHistoryEntry
        self.connection_name = connection_name

    def compose(self) -> ComposeResult:
        from datetime import datetime

        with Container(id="history-dialog"):
            yield Static(f"Query History - {self.connection_name}", id="history-title")

            options = []
            for entry in self.history:
                # Format timestamp nicely
                try:
                    dt = datetime.fromisoformat(entry.timestamp)
                    time_str = dt.strftime("%Y-%m-%d %H:%M")
                except (ValueError, AttributeError):
                    time_str = "Unknown"

                # Truncate query for display
                query_preview = entry.query.replace("\n", " ")[:50]
                if len(entry.query) > 50:
                    query_preview += "..."

                options.append(Option(f"[dim]{time_str}[/] {query_preview}", id=entry.timestamp))

            if options:
                yield OptionList(*options, id="history-list")
            else:
                yield Static("No query history for this connection", id="history-list")

            yield Static("", id="history-preview")
            yield Static(r"[bold]\[Enter][/] Select  [bold]\[D][/] Delete  [bold]\[Esc][/] Cancel", id="history-footer")

    def on_mount(self) -> None:
        try:
            option_list = self.query_one("#history-list", OptionList)
            option_list.focus()
            if self.history:
                self._update_preview(0)
        except Exception:
            pass

    def on_option_list_option_highlighted(self, event) -> None:
        if event.option_list.id == "history-list":
            idx = event.option_list.highlighted
            if idx is not None:
                self._update_preview(idx)

    def _update_preview(self, idx: int) -> None:
        if idx < len(self.history):
            preview = self.query_one("#history-preview", Static)
            preview.update(self.history[idx].query)

    def action_select(self) -> None:
        if not self.history:
            self.dismiss(None)
            return

        try:
            option_list = self.query_one("#history-list", OptionList)
            idx = option_list.highlighted
            if idx is not None and idx < len(self.history):
                self.dismiss(("select", self.history[idx].query))
            else:
                self.dismiss(None)
        except Exception:
            self.dismiss(None)

    def on_option_list_option_selected(self, event) -> None:
        if event.option_list.id == "history-list":
            idx = event.option_list.highlighted
            if idx is not None and idx < len(self.history):
                self.dismiss(("select", self.history[idx].query))

    def action_delete(self) -> None:
        """Delete the selected history entry."""
        if not self.history:
            return

        try:
            option_list = self.query_one("#history-list", OptionList)
            idx = option_list.highlighted
            if idx is not None and idx < len(self.history):
                # Remove from history and refresh
                entry = self.history[idx]
                self.dismiss(("delete", entry.timestamp))
        except Exception:
            pass

    def action_cancel(self) -> None:
        self.dismiss(None)
