"""Regex delimiter builder wizard for NlessApp.

Detects unnamed capture groups in user-entered regex patterns, walks the user
through naming each one, shows a preview of parsed data, and applies the final
named-group regex.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .app import NlessApp
    from .autocomplete import AutocompleteInput


@dataclass
class RegexWizardState:
    original_regex: str
    pattern: re.Pattern
    group_fragments: list[str]
    group_names: list[str] = field(default_factory=list)
    context: str = "delimiter"  # "delimiter" or "column_delimiter"


def _extract_group_fragments(pattern_str: str) -> list[str]:
    """Extract the sub-pattern text inside each unnamed capturing group.

    Skips escaped parens ``\\(``, character classes ``[(]``, non-capturing
    groups ``(?:...)``, and already-named groups ``(?P<...>...)``.
    """
    fragments: list[str] = []
    i = 0
    n = len(pattern_str)
    while i < n:
        ch = pattern_str[i]
        if ch == "\\" and i + 1 < n:
            i += 2
            continue
        if ch == "[":
            # skip character class
            i += 1
            while i < n and pattern_str[i] != "]":
                if pattern_str[i] == "\\" and i + 1 < n:
                    i += 1
                i += 1
            i += 1  # skip closing ]
            continue
        if ch == "(":
            # Check what follows
            if i + 1 < n and pattern_str[i + 1] == "?":
                # Non-capturing / named / lookahead — skip
                i += 1
                continue
            # Unnamed capturing group — extract content
            depth = 1
            start = i + 1
            i += 1
            while i < n and depth > 0:
                if pattern_str[i] == "\\" and i + 1 < n:
                    i += 2
                    continue
                if pattern_str[i] == "[":
                    i += 1
                    while i < n and pattern_str[i] != "]":
                        if pattern_str[i] == "\\" and i + 1 < n:
                            i += 1
                        i += 1
                    i += 1
                    continue
                if pattern_str[i] == "(":
                    depth += 1
                elif pattern_str[i] == ")":
                    depth -= 1
                i += 1
            fragments.append(pattern_str[start : i - 1])
            continue
        i += 1
    return fragments


def _inject_group_names(pattern_str: str, names: list[str]) -> str:
    """Replace each unnamed ``(`` with ``(?P<name>`` using names in order.

    Skips escaped parens ``\\(``, character classes ``[(]``, and
    ``(?...`` (non-capturing, lookahead, named groups).
    """
    result: list[str] = []
    name_idx = 0
    i = 0
    n = len(pattern_str)
    while i < n:
        ch = pattern_str[i]
        if ch == "\\" and i + 1 < n:
            result.append(pattern_str[i : i + 2])
            i += 2
            continue
        if ch == "[":
            # Copy entire character class
            start = i
            i += 1
            while i < n and pattern_str[i] != "]":
                if pattern_str[i] == "\\" and i + 1 < n:
                    i += 1
                i += 1
            i += 1  # closing ]
            result.append(pattern_str[start:i])
            continue
        if ch == "(":
            if i + 1 < n and pattern_str[i + 1] == "?":
                # Non-capturing / named / lookahead — copy as-is
                result.append("(")
                i += 1
                continue
            # Unnamed capturing group — inject name
            if name_idx < len(names):
                result.append(f"(?P<{names[name_idx]}>")
                name_idx += 1
            else:
                result.append("(")
            i += 1
            continue
        result.append(ch)
        i += 1
    return "".join(result)


class RegexWizardMixin:
    """Mixin providing regex wizard methods for NlessApp."""

    def _start_regex_wizard(
        self: NlessApp, regex_str: str, pattern: re.Pattern, context: str
    ) -> None:
        fragments = _extract_group_fragments(regex_str)
        self._regex_wizard_state = RegexWizardState(
            original_regex=regex_str,
            pattern=pattern,
            group_fragments=fragments,
            context=context,
        )
        self._prompt_next_group_name()

    def _prompt_next_group_name(self: NlessApp) -> None:
        state = self._regex_wizard_state
        if state is None:
            return
        idx = len(state.group_names)
        fragment = state.group_fragments[idx]
        self._create_prompt(
            f"Name for group {idx + 1} ({fragment}):",
            "regex_wizard_name_input",
        )

    def _handle_regex_wizard_name_submitted(
        self: NlessApp, event: AutocompleteInput.Submitted
    ) -> None:
        state = self._regex_wizard_state
        if state is None:
            event.input.remove()
            return
        name = event.value.strip()
        if not name:
            event.input.remove()
            self._regex_wizard_state = None
            return
        if not name.isidentifier():
            self.notify(f"'{name}' is not a valid Python identifier", severity="error")
            self._reprompt_group_name(event.input)
            return
        if name in state.group_names:
            self.notify(f"'{name}' is already used", severity="error")
            self._reprompt_group_name(event.input)
            return
        state.group_names.append(name)
        if len(state.group_names) < len(state.group_fragments):
            self._reprompt_group_name(event.input)
        else:
            event.input.remove()
            self._apply_regex_wizard()

    def _reprompt_group_name(self: NlessApp, widget: AutocompleteInput) -> None:
        """Reuse the existing AutocompleteInput for the next group name prompt."""
        state = self._regex_wizard_state
        if state is None:
            return
        idx = len(state.group_names)
        fragment = state.group_fragments[idx]
        placeholder = f"Name for group {idx + 1} ({fragment}):"
        widget.placeholder = placeholder
        inner_input = widget.query_one("Input")
        inner_input.placeholder = placeholder
        inner_input.value = ""
        inner_input.focus()

    def _apply_regex_wizard(self: NlessApp) -> None:
        state = self._regex_wizard_state
        if state is None:
            return
        named_regex = _inject_group_names(state.original_regex, state.group_names)
        try:
            re.compile(named_regex)
        except re.error as e:
            self.notify(f"Regex error: {e}", severity="error")
            self._regex_wizard_state = None
            return
        context = state.context
        # Replace the raw unnamed-group regex in history with the named version
        history_id = (
            "column_delimiter_input"
            if context == "column_delimiter"
            else "delimiter_input"
        )
        raw_entry = {"id": history_id, "val": state.original_regex}
        if raw_entry in self.input_history:
            idx = self.input_history.index(raw_entry)
            self.input_history[idx] = {"id": history_id, "val": named_regex}
        self._regex_wizard_state = None
        if context == "column_delimiter":
            self._apply_column_delimiter(named_regex)
        else:
            self._get_current_buffer().switch_delimiter(named_regex)
