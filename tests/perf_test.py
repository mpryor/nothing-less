import cProfile
import csv
import time
from turtle import end_fill
from textual.app import App, ComposeResult
from nless.datatable import Datatable
from nless.nlesstable import NlessDataTable as OldDatatable

class TestAppOld(App):
    start_time = time.time_ns()
    end_time = 0

    def compose(self) -> ComposeResult:
        table = OldDatatable()
        with open('./example_data/people-1000000.csv') as f:
            lines = f.readlines()
            rows = []
            for i, line in enumerate(lines):
                if i == 0:
                    columns = next(csv.reader([line]))
                    table.add_columns(*(columns))
                else:
                    row = next(csv.reader([line]))
                    rows.append(row)
            table.add_rows(rows)
        yield table

    def on_ready(self) -> None:
        self.end_time = time.time_ns()
        self.exit()

def load_data():
    with open('./example_data/people-1000000.csv') as f:
        lines = f.readlines()
        rows = []
        for i, line in enumerate(lines):
            if i == 0:
                columns = next(csv.reader([line]))
            else:
                row = next(csv.reader([line]))
                rows.append(row)
    return columns, rows

class TestApp(App):

    def __init__(self, rows, columns):
        super().__init__()
        self.start_time = time.time_ns()
        self.end_time = 0
        self.rows = rows
        self.columns = columns

    def compose(self) -> ComposeResult:
        table = Datatable()
        table.add_columns(self.columns)
        table.add_rows(self.rows)
        yield table

    def on_ready(self) -> None:
        self.end_time = time.time_ns()
        self.exit()

if __name__ == "__main__":
    columns, rows = load_data()
    t = TestApp(rows=rows, columns=columns)
    t.run()
    print(f"Elapsed time with new datatable (seconds): {(t.end_time - t.start_time) / 1_000_000_000}")
