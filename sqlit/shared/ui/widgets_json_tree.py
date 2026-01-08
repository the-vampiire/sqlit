"""JSON tree viewer widget for sqlit."""

from __future__ import annotations

import json
from typing import Any

from rich.highlighter import ReprHighlighter
from rich.text import Text
from textual.widgets import Tree
from textual.widgets.tree import TreeNode


class JSONTreeView(Tree[Any]):
    """Interactive JSON tree viewer with expand/collapse support."""

    DEFAULT_CSS = """
    JSONTreeView {
        height: 1fr;
        background: $surface;
    }
    
    JSONTreeView > .tree--guides {
        color: $text-muted;
    }
    
    JSONTreeView > .tree--cursor {
        background: $accent;
        color: $text;
    }
    """

    def __init__(
        self,
        label: str = "JSON",
        *,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(label, id=id, classes=classes)
        self._raw_json: str = ""
        self._highlighter = ReprHighlighter()

    def set_json(self, data: str | dict | list, label: str = "JSON") -> None:
        """Set JSON data to display in the tree."""
        self._raw_json = data if isinstance(data, str) else json.dumps(data)

        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (json.JSONDecodeError, ValueError):
                self.root.set_label(Text("Invalid JSON", style="red"))
                return

        self.clear()
        self.root.set_label(Text(f"{{}} {label}" if isinstance(data, dict) else f"[] {label}"))
        self._add_json_node(self.root, data)
        self.root.expand()

    def _add_json_node(self, node: TreeNode[Any], data: Any, key: str | None = None) -> None:
        """Recursively add JSON data to tree nodes."""
        if isinstance(data, dict):
            if key is not None:
                label = Text.assemble(Text(f"{{}} ", style="bold cyan"), Text(key))
                child = node.add(label, data=data)
            else:
                child = node
            for k, v in data.items():
                self._add_json_node(child, v, k)
        elif isinstance(data, list):
            if key is not None:
                label = Text.assemble(
                    Text(f"[] ", style="bold magenta"),
                    Text(key),
                    Text(f" ({len(data)})", style="dim"),
                )
                child = node.add(label, data=data)
            else:
                child = node
            for i, v in enumerate(data):
                self._add_json_node(child, v, f"[{i}]")
        else:
            if key is not None:
                value_text = self._format_value(data)
                label = Text.assemble(
                    Text(f"{key}", style="bold"),
                    Text(": ", style="dim"),
                    value_text,
                )
                leaf = node.add_leaf(label, data=data)
                leaf.allow_expand = False
            else:
                node.add_leaf(self._format_value(data), data=data)

    def _format_value(self, value: Any) -> Text:
        """Format a leaf value with syntax highlighting."""
        if value is None:
            return Text("null", style="italic dim")
        elif isinstance(value, bool):
            return Text(str(value).lower(), style="italic cyan")
        elif isinstance(value, int | float):
            return Text(str(value), style="bold blue")
        elif isinstance(value, str):
            if len(value) > 100:
                display = f'"{value[:100]}..."'
            else:
                display = f'"{value}"'
            return Text(display, style="green")
        else:
            return self._highlighter(repr(value))

    def action_expand_all(self) -> None:
        """Expand all nodes in the tree."""

        def expand_recursive(node: TreeNode[Any]) -> None:
            node.expand()
            for child in node.children:
                expand_recursive(child)

        expand_recursive(self.root)

    def action_collapse_all(self) -> None:
        """Collapse all nodes except root."""

        def collapse_recursive(node: TreeNode[Any]) -> None:
            for child in node.children:
                collapse_recursive(child)
                child.collapse()

        collapse_recursive(self.root)
        self.root.expand()

    @property
    def raw_json(self) -> str:
        """Get the raw JSON string for copying."""
        return self._raw_json
