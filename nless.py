import sys
from typing import Optional
from threading import Thread

from textual.app import App, ComposeResult
from textual.widgets import Log


class NlessApp(App):
    """A Textual application for displaying streaming log lines."""
    
    def __init__(self):
        super().__init__()
        self.lines: list[str] = []  # Instance variable for thread safety

    def compose(self) -> ComposeResult:
        """Create and yield the Log widget."""
        yield Log()

    def on_mount(self) -> None:
        """Set up periodic updates when the app is mounted."""
        self.set_interval(.1, self.update)

    def update(self) -> None:
        """Update the log display with new lines."""
        try:
            log = self.query_one(Log)
            while self.lines:
                line = self.lines.pop(0)
                log.write_line(line)
        except Exception as e:
            self.exit(message=f"Update error: {e}")

    def add_log(self, log_line: str) -> None:
        """Add a log line to be displayed."""
        self.lines.append(log_line)


class InputConsumer:
    """Reads input from stdin and forwards to the application."""
    
    def __init__(self, app: NlessApp):
        self.app = app

    def run(self) -> None:
        """Read lines from stdin and add them to the app."""
        for line in sys.stdin:
            self.app.add_log(line.strip())


if __name__ == "__main__":
    app = NlessApp()
    t = Thread(target=InputConsumer(app).run, daemon=True)
    t.start()
    app.run()
