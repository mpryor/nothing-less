from importlib.metadata import version as pkg_version

from textual.containers import Center, Container
from textual.screen import ModalScreen
from textual.widgets import Markdown, Static

from nless.config import load_config, save_config


class GettingStartedScreen(ModalScreen):
    """A widget to display a getting started message."""

    DEFAULT_CSS = """
    GettingStartedScreen {
        align: center middle;
    }
    #getting_started {
        overflow: auto;
    }
    """

    BINDINGS = [
        ("q", "app.pop_screen", "Close Getting Started"),
        ("ctrl+c", "dismiss_getting_started", "Dismiss Getting Started"),
    ]

    def _help_text(self) -> str:
        try:
            brand = self.app.nless_theme.brand
        except AttributeError:
            brand = "green"
        return (
            f"Help: [{brand}]?[/{brand}] - keybindings | "
            f"[{brand}]q[/{brand}] - close this dialog | "
            f"[{brand}]<Ctrl+c>[/{brand}] - dismiss this dialog permanently"
        )

    def on_mount(self):
        try:
            accent = self.app.nless_theme.accent
            dialog = self.query_one("#dialog")
            dialog.styles.border = ("tall", accent)
        except Exception:
            pass

    def action_dismiss_getting_started(self):
        config = load_config()
        config.show_getting_started = False
        save_config(config)
        self.app.pop_screen()

    def compose(self):
        try:
            t = self.app.nless_theme
            accent = t.accent
            muted = t.muted
        except AttributeError:
            accent = "green"
            muted = "#888888"
        try:
            ver = pkg_version("nothing-less")
        except Exception:
            ver = "?"
        yield Container(
            Static("\n"),
            Static(
                f"[{accent}]"
                """           ░██
           ░██
░████████  ░██  ░███████   ░███████   ░███████
░██    ░██ ░██ ░██    ░██ ░██        ░██
░██    ░██ ░██ ░█████████  ░███████   ░███████
░██    ░██ ░██ ░██               ░██        ░██
░██    ░██ ░██  ░███████   ░███████   ░███████"""
                f"[/{accent}]"
                f"\n[{muted}]                    v{ver}[/{muted}]",
                classes="centered",
            ),
            Static(
                f"[{muted}]"
                f"[@click=app.open_link('https://github.com/mpryor/nothing-less')]GitHub[/]"
                f" · "
                f"[@click=app.open_link('https://mpryor.github.io/nothing-less/')]Docs[/]"
                f" · "
                f"[@click=app.open_link('https://github.com/mpryor/nothing-less/issues')]Report Issue[/]"
                f" · "
                f"[@click=app.open_link('https://mpryor.dev')]Author[/]"
                f"[/{muted}]",
                classes="centered",
            ),
            Center(
                Center(
                    Markdown(
                        """Nless is a TUI to explore and analyze data - filter, search, sort, group, and export it!
                """,
                        classes="centered",
                    ),
                ),
                Center(
                    Markdown(
                        """There are a few ways you can populate nless with data:  
- Pipe data into nless: `cat file.txt | nless`  
- Redirect a file into nless: `nless < file.txt`  
- Pass a file as an argument: `nless file.txt`  
- Use the `!` command to run a shell command and load its output into nless  
""",
                        classes="text-wrap",
                    ),
                    classes="overflow",
                ),
                Static(
                    self._help_text(),
                    classes="centered",
                ),
                id="dialog",
            ),
            id="getting_started",
        )
