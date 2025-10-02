from textual.containers import Center, Grid, Vertical
from textual.screen import ModalScreen
from textual.widgets import Markdown, Static


class GettingStartedScreen(ModalScreen):
    """A widget to display a getting started message."""

    BINDINGS = [("q", "app.pop_screen", "Close Getting Started")]

    def compose(self):
        yield Grid(
            Static(
"""           ░██                                  
           ░██                                  
░████████  ░██  ░███████   ░███████   ░███████  
░██    ░██ ░██ ░██    ░██ ░██        ░██        
░██    ░██ ░██ ░█████████  ░███████   ░███████  
░██    ░██ ░██ ░██               ░██        ░██ 
░██    ░██ ░██  ░███████   ░███████   ░███████
""",
                classes="centered green"
            ),
            Center(Markdown(
                """This is a simple TUI to explore and analyze data:  
- convert it into a tabular format  
- filter it  
- pivot it  
- sort it  
- search it  
- and export it!  
                """,
                classes="centered"
            ),
            Static("Press [green][bold]'?'[/bold][/green] after closing this dialog to view the keybindings.", classes="centered"),
            Static("Press [green][bold]'q'[/bold][/green] to close this dialog.", classes="centered"
            ),
            id="dialog"
        )
