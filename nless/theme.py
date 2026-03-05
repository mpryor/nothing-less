"""Theming system for nless.

Provides a flat NlessTheme dataclass with 16 semantic color slots, 8 built-in
themes, custom theme loading from ~/.config/nless/themes/, and resolution logic.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, fields


@dataclass(frozen=True)
class NlessTheme:
    """Semantic color slots for the entire nless UI."""

    name: str = "default"

    # Table styles
    cursor_bg: str = "#0087d7"
    cursor_fg: str = "#d7ffff"
    header_bg: str = "#005f5f"
    header_fg: str = "#d7ffff"
    fixed_column_bg: str = "#111177"
    row_odd_bg: str = "#222222"
    row_even_bg: str = "#333333"
    col_odd_fg: str = "#bbbbbb"
    col_even_fg: str = "#dddddd"

    # Scrollbar
    scrollbar_bg: str = "#1a1a1a"
    scrollbar_fg: str = "#555555"

    # Search
    search_match_bg: str = "#005f00"
    search_match_fg: str = "#ffffff"

    # Semantic colors
    highlight: str = "#00ff00"
    accent: str = "#00ff00"
    status_tailing: str = "#00bb00"
    status_loading: str = "#ffaa00"
    muted: str = "#888888"
    border: str = "green"
    brand: str = "green"

    def markup(self, slot: str, text: str) -> str:
        """Wrap *text* in Rich markup using the color from *slot*."""
        color = getattr(self, slot)
        return f"[{color}]{text}[/{color}]"

    @property
    def highlight_re(self) -> re.Pattern[str]:
        """Compiled regex matching Rich markup tags for the highlight color."""
        # Cache on the instance to avoid recompiling on every access.
        # Use object.__setattr__ because the dataclass is frozen.
        try:
            return object.__getattribute__(self, "_highlight_re_cache")
        except AttributeError:
            escaped = re.escape(self.highlight)
            pat = re.compile(rf"\[{escaped}\](.*?)\[/{escaped}\]")
            object.__setattr__(self, "_highlight_re_cache", pat)
            return pat


# ── Built-in themes ──────────────────────────────────────────────────────────

BUILTIN_THEMES: dict[str, NlessTheme] = {
    "default": NlessTheme(),
    # Dracula — https://draculatheme.com/spec / dracula/vim
    # Visual: #44475a, CursorLine: #44475a, StatusLine: #44475a on fg
    # Search: #50fa7b bg, #282a36 fg
    "dracula": NlessTheme(
        name="dracula",
        cursor_bg="#44475a",
        cursor_fg="#f8f8f2",
        header_bg="#6272a4",
        header_fg="#f8f8f2",
        fixed_column_bg="#44475a",
        row_odd_bg="#21222c",
        row_even_bg="#282a36",
        col_odd_fg="#bfbfbf",
        col_even_fg="#f8f8f2",
        scrollbar_bg="#21222c",
        scrollbar_fg="#6272a4",
        search_match_bg="#50fa7b",
        search_match_fg="#282a36",
        highlight="#50fa7b",
        accent="#bd93f9",
        status_tailing="#50fa7b",
        status_loading="#ffb86c",
        muted="#6272a4",
        border="#bd93f9",
        brand="#bd93f9",
    ),
    # Monokai Pro — monokai.pro / vim-monokai-pro, loctvl842/monokai-pro.nvim
    # CursorLine: #2c292d→#363337, StatusLine: #403e41, Visual: #55535d
    # Search: #ffd866 bg, #2c292d fg
    "monokai": NlessTheme(
        name="monokai",
        cursor_bg="#55535d",
        cursor_fg="#ffd866",
        header_bg="#403e41",
        header_fg="#fcfcfa",
        fixed_column_bg="#403e41",
        row_odd_bg="#221f22",
        row_even_bg="#2c292d",
        col_odd_fg="#939293",
        col_even_fg="#fcfcfa",
        scrollbar_bg="#221f22",
        scrollbar_fg="#727072",
        search_match_bg="#ffd866",
        search_match_fg="#2c292d",
        highlight="#a9dc76",
        accent="#ff6188",
        status_tailing="#a9dc76",
        status_loading="#fc9867",
        muted="#727072",
        border="#ff6188",
        brand="#ff6188",
    ),
    # Nord — nordtheme.com / nordtheme/vim
    # Visual: nord2 #434c5e, CursorLine: nord1 #3b4252
    # Search: nord8 #88c0d0 bg, nord1 #3b4252 fg
    "nord": NlessTheme(
        name="nord",
        cursor_bg="#434c5e",
        cursor_fg="#eceff4",
        header_bg="#3b4252",
        header_fg="#eceff4",
        fixed_column_bg="#3b4252",
        row_odd_bg="#2e3440",
        row_even_bg="#3b4252",
        col_odd_fg="#d8dee9",
        col_even_fg="#eceff4",
        scrollbar_bg="#2e3440",
        scrollbar_fg="#4c566a",
        search_match_bg="#88c0d0",
        search_match_fg="#3b4252",
        highlight="#a3be8c",
        accent="#88c0d0",
        status_tailing="#a3be8c",
        status_loading="#ebcb8b",
        muted="#4c566a",
        border="#81a1c1",
        brand="#88c0d0",
    ),
    # Solarized Dark — ethanschoonover.com/solarized / altercation/vim-colors-solarized
    # Visual: base01 #586e75 reverse on base03, CursorLine: base02 #073642
    # Search: yellow #b58900 bg, base3 #fdf6e3 fg
    "solarized-dark": NlessTheme(
        name="solarized-dark",
        cursor_bg="#073642",
        cursor_fg="#93a1a1",
        header_bg="#073642",
        header_fg="#93a1a1",
        fixed_column_bg="#073642",
        row_odd_bg="#002b36",
        row_even_bg="#073642",
        col_odd_fg="#839496",
        col_even_fg="#93a1a1",
        scrollbar_bg="#002b36",
        scrollbar_fg="#586e75",
        search_match_bg="#b58900",
        search_match_fg="#fdf6e3",
        highlight="#b58900",
        accent="#268bd2",
        status_tailing="#859900",
        status_loading="#b58900",
        muted="#586e75",
        border="#268bd2",
        brand="#268bd2",
    ),
    # Gruvbox Dark — morhetz/gruvbox, gruvbox-community/gruvbox
    # Visual: bg3 #665c54 invert, CursorLine: bg1 #3c3836
    # Search: yellow #fabd2f bg, bg #282828 fg
    "gruvbox": NlessTheme(
        name="gruvbox",
        cursor_bg="#504945",
        cursor_fg="#ebdbb2",
        header_bg="#504945",
        header_fg="#ebdbb2",
        fixed_column_bg="#3c3836",
        row_odd_bg="#1d2021",
        row_even_bg="#282828",
        col_odd_fg="#bdae93",
        col_even_fg="#ebdbb2",
        scrollbar_bg="#1d2021",
        scrollbar_fg="#665c54",
        search_match_bg="#fabd2f",
        search_match_fg="#282828",
        highlight="#b8bb26",
        accent="#83a598",
        status_tailing="#b8bb26",
        status_loading="#fabd2f",
        muted="#928374",
        border="#fe8019",
        brand="#fe8019",
    ),
    # Tokyo Night — folke/tokyonight.nvim (night variant)
    # Visual: bg_visual #283457, CursorLine: bg_highlight #292e42
    # Search: orange #ff9e64 bg, bg_dark #16161e fg
    "tokyo-night": NlessTheme(
        name="tokyo-night",
        cursor_bg="#283457",
        cursor_fg="#c0caf5",
        header_bg="#16161e",
        header_fg="#c0caf5",
        fixed_column_bg="#292e42",
        row_odd_bg="#1a1b26",
        row_even_bg="#1f2335",
        col_odd_fg="#a9b1d6",
        col_even_fg="#c0caf5",
        scrollbar_bg="#1a1b26",
        scrollbar_fg="#3b4261",
        search_match_bg="#ff9e64",
        search_match_fg="#1a1b26",
        highlight="#9ece6a",
        accent="#7aa2f7",
        status_tailing="#9ece6a",
        status_loading="#e0af68",
        muted="#565f89",
        border="#7aa2f7",
        brand="#7aa2f7",
    ),
    # Catppuccin Mocha — catppuccin/nvim
    # Visual: surface2 #585b70, CursorLine: surface0 #313244
    # Search: yellow #f9e2af bg, base #1e1e2e fg
    "catppuccin-mocha": NlessTheme(
        name="catppuccin-mocha",
        cursor_bg="#585b70",
        cursor_fg="#cdd6f4",
        header_bg="#181825",
        header_fg="#cdd6f4",
        fixed_column_bg="#313244",
        row_odd_bg="#1e1e2e",
        row_even_bg="#313244",
        col_odd_fg="#bac2de",
        col_even_fg="#cdd6f4",
        scrollbar_bg="#1e1e2e",
        scrollbar_fg="#585b70",
        search_match_bg="#f9e2af",
        search_match_fg="#1e1e2e",
        highlight="#a6e3a1",
        accent="#cba6f7",
        status_tailing="#a6e3a1",
        status_loading="#f9e2af",
        muted="#6c7086",
        border="#cba6f7",
        brand="#cba6f7",
    ),
    # Solarized Light — ethanschoonover.com/solarized / altercation/vim-colors-solarized
    # Light mode swaps: base3 #fdf6e3 = bg, base2 #eee8d5 = bg highlight
    # Search: yellow #b58900 bg, base3 #fdf6e3 fg
    "solarized-light": NlessTheme(
        name="solarized-light",
        cursor_bg="#eee8d5",
        cursor_fg="#586e75",
        header_bg="#eee8d5",
        header_fg="#586e75",
        fixed_column_bg="#eee8d5",
        row_odd_bg="#fdf6e3",
        row_even_bg="#eee8d5",
        col_odd_fg="#657b83",
        col_even_fg="#586e75",
        scrollbar_bg="#eee8d5",
        scrollbar_fg="#93a1a1",
        search_match_bg="#b58900",
        search_match_fg="#fdf6e3",
        highlight="#b58900",
        accent="#268bd2",
        status_tailing="#859900",
        status_loading="#b58900",
        muted="#93a1a1",
        border="#268bd2",
        brand="#268bd2",
    ),
    # Catppuccin Latte — catppuccin/nvim (latte flavor)
    # Visual: surface2 #acb0be, CursorLine: surface0 #ccd0da
    # Search: yellow #df8e1d bg, base #eff1f5 fg
    "catppuccin-latte": NlessTheme(
        name="catppuccin-latte",
        cursor_bg="#acb0be",
        cursor_fg="#4c4f69",
        header_bg="#e6e9ef",
        header_fg="#4c4f69",
        fixed_column_bg="#ccd0da",
        row_odd_bg="#eff1f5",
        row_even_bg="#e6e9ef",
        col_odd_fg="#5c5f77",
        col_even_fg="#4c4f69",
        scrollbar_bg="#e6e9ef",
        scrollbar_fg="#9ca0b0",
        search_match_bg="#df8e1d",
        search_match_fg="#eff1f5",
        highlight="#40a02b",
        accent="#8839ef",
        status_tailing="#40a02b",
        status_loading="#df8e1d",
        muted="#9ca0b0",
        border="#8839ef",
        brand="#8839ef",
    ),
}

# Color slot names (excludes 'name')
_COLOR_SLOTS = frozenset(f.name for f in fields(NlessTheme) if f.name != "name")


def load_custom_themes(
    themes_dir: str = "~/.config/nless/themes",
) -> dict[str, NlessTheme]:
    """Load custom theme JSON files from *themes_dir*.

    Each file is a partial JSON object — omitted keys inherit from the default
    theme.  Invalid files are silently skipped and their names are collected in
    the returned *warnings* list (unused here but available for callers).
    """
    themes: dict[str, NlessTheme] = {}
    expanded = os.path.expanduser(themes_dir)
    if not os.path.isdir(expanded):
        return themes

    default = BUILTIN_THEMES["default"]
    default_dict = asdict(default)

    for filename in sorted(os.listdir(expanded)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(expanded, filename)
        try:
            with open(filepath) as f:
                data = json.load(f)
            if not isinstance(data, dict) or "name" not in data:
                continue
            # Merge: start from default, overlay user values
            merged = dict(default_dict)
            for key, value in data.items():
                if key in _COLOR_SLOTS or key == "name":
                    merged[key] = value
            themes[merged["name"]] = NlessTheme(**merged)
        except (json.JSONDecodeError, TypeError, OSError):
            continue

    return themes


def get_all_themes() -> dict[str, NlessTheme]:
    """Return built-in themes merged with user custom themes."""
    themes = dict(BUILTIN_THEMES)
    themes.update(load_custom_themes())
    return themes


def resolve_theme(
    cli_theme: str | None = None, config_theme: str = "default"
) -> NlessTheme:
    """Resolve which theme to use.  CLI arg wins over config."""
    all_themes = get_all_themes()
    name = cli_theme or config_theme
    return all_themes.get(name, BUILTIN_THEMES["default"])
