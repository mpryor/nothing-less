import sys
from typing import Optional
from threading import Thread

from textual.app import App, ComposeResult
from textual.widgets import Log


class NlessApp(App):
    lines = []

    def __init__(self):
        super().__init__()

    def compose(self) -> ComposeResult:
        yield Log()

    def on_mount(self) -> None:
        self.set_interval(.1, self.update)

    def update(self):
        log = self.query_one(Log)
        while self.lines:
            line = self.lines.pop(0)
            log.write_line(line)

    def add_log(self, log_line):
        self.lines.append(log_line)


class InputConsumer:
    def __init__(self, app: NlessApp):
        self.app = app

    def run(self):
        for line in sys.stdin:
            self.app.add_log(line.strip())


if __name__ == "__main__":
    app = NlessApp()
    t = Thread(target=InputConsumer(app).run, daemon=True)
    t.start()
    app.run()
