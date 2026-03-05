"""Keymap system for nless.

Provides built-in keymap presets (vim, less, emacs), custom keymap loading
from ~/.config/nless/keymaps/*.json, and resolution logic.
Follows the same pattern as theme.py.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class NlessKeymap:
    """A keymap is a name plus a dict of binding-id → key overrides."""

    name: str = "vim"
    bindings: dict[str, str] = field(default_factory=dict)


# ── Built-in keymaps ────────────────────────────────────────────────────────

BUILTIN_KEYMAPS: dict[str, NlessKeymap] = {
    # vim: default — empty overrides, all keys come from BINDINGS defaults
    "vim": NlessKeymap(name="vim"),
    # less: overrides to match less(1) conventions
    "less": NlessKeymap(
        name="less",
        bindings={
            "table.page_down": "space,f,ctrl+f",
            "table.page_up": "b,ctrl+b",
            "table.cursor_down": "down,j,e",
            "table.cursor_right": "right,l",
            "table.cursor_left": "left",
            "buffer.previous_search": "N",
            "buffer.toggle_tail": "F",
            "app.help": "h",
            "app.show_tab_previous": "P",
            "app.add_buffer": "ctrl+t",
            "app.filter": "i",
            "app.filter_cursor_word": "I",
            "app.exclude_filter": "x",
            "app.exclude_filter_cursor_word": "X",
        },
    ),
    # emacs: ctrl/alt-based overrides
    "emacs": NlessKeymap(
        name="emacs",
        bindings={
            "table.cursor_down": "ctrl+n,down",
            "table.cursor_up": "ctrl+p,up",
            "table.cursor_right": "ctrl+f,right",
            "table.cursor_left": "ctrl+b,left",
            "table.page_down": "ctrl+v",
            "table.page_up": "alt+v",
            "table.scroll_top": "alt+less_than_sign",
            "table.scroll_bottom": "alt+greater_than_sign",
            "table.scroll_to_beginning": "ctrl+a",
            "table.scroll_to_end": "ctrl+e",
            "app.search": "ctrl+s",
            "buffer.next_search": "alt+s",
            "buffer.previous_search": "ctrl+r",
            "app.close_active_buffer": "ctrl+g,q",
            "app.help": "ctrl+h",
            "app.show_tab_next": "alt+right",
            "app.show_tab_previous": "alt+left",
            "app.add_buffer": "alt+n",
            "app.filter": "alt+f",
            "app.filter_cursor_word": "alt+F",
            "app.exclude_filter": "alt+x",
            "app.exclude_filter_cursor_word": "alt+X",
            "app.write_to_file": "ctrl+w",
            "buffer.copy": "alt+w",
        },
    ),
}


def load_custom_keymaps(
    keymaps_dir: str = "~/.config/nless/keymaps",
) -> dict[str, NlessKeymap]:
    """Load custom keymap JSON files from *keymaps_dir*.

    Each file is a JSON object with:
      - "name": required keymap name
      - "extends": optional base preset name (default: "vim")
      - "bindings": dict of binding-id → key overrides

    Invalid files are silently skipped.
    """
    keymaps: dict[str, NlessKeymap] = {}
    expanded = os.path.expanduser(keymaps_dir)
    if not os.path.isdir(expanded):
        return keymaps

    for filename in sorted(os.listdir(expanded)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(expanded, filename)
        try:
            with open(filepath) as f:
                data = json.load(f)
            if not isinstance(data, dict) or "name" not in data:
                continue

            base_name = data.get("extends", "vim")
            base = BUILTIN_KEYMAPS.get(base_name, BUILTIN_KEYMAPS["vim"])

            # Merge: start from base bindings, overlay user bindings
            merged_bindings = dict(base.bindings)
            user_bindings = data.get("bindings", {})
            if isinstance(user_bindings, dict):
                merged_bindings.update(user_bindings)

            keymaps[data["name"]] = NlessKeymap(
                name=data["name"],
                bindings=merged_bindings,
            )
        except (json.JSONDecodeError, TypeError, OSError):
            continue

    return keymaps


def get_all_keymaps() -> dict[str, NlessKeymap]:
    """Return built-in keymaps merged with user custom keymaps."""
    keymaps = dict(BUILTIN_KEYMAPS)
    keymaps.update(load_custom_keymaps())
    return keymaps


def resolve_keymap(
    cli_keymap: str | None = None, config_keymap: str = "vim"
) -> NlessKeymap:
    """Resolve which keymap to use.  CLI arg wins over config."""
    all_keymaps = get_all_keymaps()
    name = cli_keymap or config_keymap
    return all_keymaps.get(name, BUILTIN_KEYMAPS["vim"])
