from textual.containers import Center, Container
from textual.screen import ModalScreen
from textual.widgets import Markdown, Static

from nless.config import load_config, save_config


class GettingStartedScreen(ModalScreen):
    """A widget to display a getting started message."""

    BINDINGS = [
        ("q", "app.pop_screen", "Close Getting Started"),
        ("ctrl+c", "dismiss_getting_started", "Dismiss Getting Started"),
    ]

    def action_dismiss_getting_started(self):
        config = load_config()
        config.show_getting_started = False
        save_config(config)
        self.app.pop_screen()

    def compose(self):
        yield Container(
            Static("\n"),
            Static(
                """           ░██                                  
           ░██                                  
░████████  ░██  ░███████   ░███████   ░███████  
░██    ░██ ░██ ░██    ░██ ░██        ░██        
░██    ░██ ░██ ░█████████  ░███████   ░███████  
░██    ░██ ░██ ░██               ░██        ░██ 
░██    ░██ ░██  ░███████   ░███████   ░███████
""",
                classes="centered green",
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
                    "Help: [green]?[/green] - keybindings | [green]q[/green] - close this dialog | [green]<Ctrl+c>[/green] - dismiss this dialog permanently",
                    classes="centered",
                ),
                id="dialog",
            ),
            id="getting_started",
        )
