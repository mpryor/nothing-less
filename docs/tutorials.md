# Tutorials

These tutorials walk through real-world workflows with sample data you can copy and paste. Each tutorial builds on the basics and introduces progressively more advanced features.

!!! tip "Jumping to columns with `c`"
    In any tutorial, press `c` to open a column picker and jump directly to a column by name. This is much faster than scrolling with `h` / `l` when your data has many columns.

---

## 1. Terminology

Before diving in, here are the core concepts you'll encounter throughout nless.

### Buffers

A **buffer** is a single view of data. When you open a file, nless creates a buffer to display it. Buffers are like tabs — you can have several open at once and switch between them.

Buffers are created automatically when you:

- **Filter** (`f` / `F` / `e` / `E`) — a new buffer opens showing only the matching (or excluded) rows
- **Drill into a pivot** (++enter++ on a grouped row) — a new buffer opens with the detail rows behind that group
- **Create one manually** (`N`) — a fresh buffer from the original data

Switch between buffers with `L` (next) and `H` (previous), or press `1`–`9` to jump directly. Press `q` to close the current buffer. When the last buffer is closed, nless exits.

Each buffer maintains its own independent state — sort order, search position, column visibility, and scroll position. This means you can have one buffer sorted by price while another is filtered to a specific customer, without them interfering with each other.

### Buffer Groups

A **buffer group** is a collection of related buffers. When you first open a file, nless creates a group to hold its buffers. Groups let you keep separate data sources organized.

New groups are created when you:

- **Open a file** (`O`) — creates a group with a `📄` icon
- **Run a shell command** (`!`) — creates a group with a `⏵` icon indicating a streaming source
- **Start nless with a file argument** — the initial group

Switch between groups with `}` (next) and `{` (previous). Press `R` to rename a group for easy identification.

Within a group, buffers work as described above — filter, pivot, and create new buffers, all scoped to that group's data.

### Other Key Terms

| Term | Meaning |
|------|---------|
| **Delimiter** | The character or pattern used to split each line into columns. Auto-detected for CSV, TSV, JSON, and space-aligned formats. Change with `D`. |
| **Column delimiter** | A secondary delimiter applied to a single column to split it into sub-columns (`d`). |
| **Pivot / Unique key** | Mark columns with `U` to group rows by their values, adding a `count` column. Multiple `U` presses create composite keys. |
| **Filter** | A regex applied to a column (or all columns) to show only matching rows. |
| **Exclude filter** | The inverse — hides rows matching the pattern. |
| **Tail mode** | Keeps the cursor pinned to the bottom so you always see the latest data as it streams in (`t`). |
| **Highlights** | New rows from streaming sources appear highlighted in green. Press `x` to clear. |

---

## 2. Exploring a CSV

Create a file called `orders.csv`:

```csv
order_id,customer,product,quantity,price,status
1001,alice,widget,3,9.99,shipped
1002,bob,gadget,1,24.99,pending
1003,alice,widget,1,9.99,shipped
1004,carol,gizmo,2,14.99,cancelled
1005,bob,widget,5,9.99,shipped
1006,dave,gadget,2,24.99,pending
1007,alice,gizmo,1,14.99,shipped
1008,carol,widget,4,9.99,pending
1009,dave,gizmo,3,14.99,shipped
1010,bob,gadget,1,24.99,cancelled
```

Open it:

```bash
nless orders.csv
```

**Navigate the data:**

- `j` / `k` to move up and down
- `h` / `l` to move left and right
- `g` to jump to the first row, `G` to jump to the last
- `0` to jump to the first column, `$` to jump to the last
- `c` to open a column picker — select a column by name to jump straight to it

**Search for a value:**

1. Press `/`, type `alice`, press ++enter++
2. The first match is highlighted. Press `n` to jump to the next match, `p` to go back.

**Filter to a specific customer:**

1. Press `c` and select `customer` to jump to that column
2. Press `f`, type `bob`, press ++enter++
3. A new buffer opens showing only Bob's orders
4. Press `q` to close the filtered buffer and return to the original

**Quick filter by cell value:**

1. Move the cursor to a cell that says `shipped`
2. Press `F` — the column is instantly filtered to only `shipped` rows

**Sort a column:**

1. Press `c` and select `price` to jump to the price column
2. Press `s` to sort ascending (indicated by `▲`)
3. Press `s` again to sort descending (`▼`)
4. Press `s` once more to clear the sort

**Exclude rows:**

1. Press `c` and select `status`
2. Press `e`, type `cancelled`, press ++enter++
3. Cancelled orders are excluded from the view

---

## 3. Pivoting and Grouping

Using the same `orders.csv` from above:

```bash
nless orders.csv
```

**Group by a single column:**

1. Press `c` and select `status` to jump to it
2. Press `U` — the data is deduplicated by `status`, and a `count` column appears on the left
3. The view automatically focuses on just the key and count columns, hiding the rest so you can see the summary clearly

    You should see something like:

    | count | status    |
    |-------|-----------|
    | 5     | shipped   |
    | 3     | pending   |
    | 2     | cancelled |

    !!! tip "Streaming with pivots"
        If you're watching live data (e.g. `kubectl get pods -w | nless`), the hidden columns automatically reappear when new lines arrive, so you see the full row detail alongside updated counts.

**Drill into a group:**

1. With the cursor on the `shipped` row, press ++enter++
2. A new buffer opens showing all 5 shipped orders with full detail

**Composite keys — group by multiple columns:**

1. Start from the original data (press `q` to go back to earlier buffers)
2. Press `c` and select `customer`, then press `U`
3. Press `c` and select `status`, then press `U` again
4. Now data is grouped by the combination of `customer` + `status`

    | count | customer | status  |
    |-------|----------|---------|
    | 3     | alice    | shipped |
    | 1     | bob      | pending |
    | ...   | ...      | ...     |

This is equivalent to `SELECT customer, status, COUNT(*) FROM orders GROUP BY customer, status`.

---

## 4. Working with JSON Lines

Create a file called `events.jsonl`:

```json
{"ts":"2025-03-01T10:00:00Z","event":"login","user":{"id":1,"name":"alice"},"meta":{"ip":"10.0.0.1","browser":"firefox"}}
{"ts":"2025-03-01T10:05:00Z","event":"purchase","user":{"id":2,"name":"bob"},"meta":{"ip":"10.0.0.2","browser":"chrome"}}
{"ts":"2025-03-01T10:10:00Z","event":"login","user":{"id":3,"name":"carol"},"meta":{"ip":"10.0.0.3","browser":"safari"}}
{"ts":"2025-03-01T10:15:00Z","event":"logout","user":{"id":1,"name":"alice"},"meta":{"ip":"10.0.0.1","browser":"firefox"}}
{"ts":"2025-03-01T10:20:00Z","event":"purchase","user":{"id":2,"name":"bob"},"meta":{"ip":"10.0.0.2","browser":"chrome"}}
{"ts":"2025-03-01T10:25:00Z","event":"login","user":{"id":4,"name":"dave"},"meta":{"ip":"10.0.0.4","browser":"firefox"}}
```

Open it:

```bash
nless events.jsonl
```

nless auto-detects JSON and parses each line into columns: `ts`, `event`, `user`, `meta`.

**Extract nested fields with `J`:**

1. Press `c` and select `user` to jump to that column
2. Press `J` — a dropdown appears listing the nested keys
3. Select `user.name` — a new column is added with just the user's name
4. Press `c` and select `meta`, then press `J` and select `meta.ip`

You now have flat columns for `user.name` and `meta.ip` alongside the original nested data.

**Extract nested fields with column delimiter:**

1. Press `c` and select `user`
2. Press `d`, type `json`, press ++enter++
3. All keys inside `user` (`id`, `name`) are extracted as new columns at once

**Filter and group the extracted data:**

1. Press `c` and select `user.name`, then press `f`, type `bob`, press ++enter++ — filtered to Bob's events
2. Press `q` to return, then press `c` to select `event` and press `U` to see event counts per type

---

## 5. Parsing Logs with Regex Capture Groups

Regex named capture groups let you define column structure with a pattern. This is one of the most powerful features in nless.

Create a file called `access.log`:

```
2025-03-01 10:00:01 GET /api/users 200 45ms
2025-03-01 10:00:02 POST /api/orders 201 120ms
2025-03-01 10:00:03 GET /api/users/42 200 38ms
2025-03-01 10:00:04 DELETE /api/orders/99 403 12ms
2025-03-01 10:00:05 GET /api/health 200 5ms
2025-03-01 10:00:06 POST /api/users 400 67ms
2025-03-01 10:00:07 GET /api/orders 200 89ms
2025-03-01 10:00:08 PUT /api/users/42 200 55ms
2025-03-01 10:00:09 GET /api/orders/100 404 15ms
2025-03-01 10:00:10 POST /api/orders 500 230ms
```

Open and parse with a regex delimiter:

```bash
nless access.log
```

The default delimiter may split on spaces, but you can get structured columns using regex named capture groups:

1. Press `D` to change the delimiter
2. Enter this regex:

    ```
    (?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2}) (?P<method>\w+) (?P<path>\S+) (?P<status>\d+) (?P<duration>\d+)ms
    ```

3. Press ++enter++

The data is now parsed into clean columns: `date`, `time`, `method`, `path`, `status`, `duration`.

**Analyze the structured data:**

- Press `c` and select `status`, then press `f` and type `^[45]` to match 4xx and 5xx status codes
- Press `c` and select `duration`, then press `s` to sort by response time
- Press `c` and select `method`, then press `U` to see request counts per HTTP method

You can also set the regex delimiter directly from the CLI:

```bash
nless -d '(?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2}) (?P<method>\w+) (?P<path>\S+) (?P<status>\d+) (?P<duration>\d+)ms' access.log
```

---

## 6. Splitting Columns with Regex Capture Groups

Column delimiters (`d`) also support regex capture groups — useful for breaking apart a single column into structured sub-columns.

Create a file called `requests.csv`:

```csv
id,request,response_time
1,GET /api/users?page=1&limit=10,45ms
2,POST /api/orders?ref=abc,120ms
3,GET /api/users/42?fields=name,38ms
4,DELETE /api/sessions?token=xyz,12ms
5,PUT /api/users/42?role=admin&notify=true,55ms
```

```bash
nless requests.csv
```

**Split the request column with a regex:**

1. Press `c` and select `request`
2. Press `d` to apply a column delimiter
3. Enter the regex:

    ```
    (?P<method>\w+) (?P<path>[^?]+)\?(?P<query>.*)
    ```

4. Press ++enter++

The `request` column is now split into `method`, `path`, and `query` columns.

**Go further — split the query string:**

1. Press `c` and select the new `query` column
2. Press `d`, type `&`, press ++enter++
3. Each query parameter is split into its own column

---

## 7. Kubectl and Aligned Output

nless works well with space-aligned output from tools like `kubectl`, `docker`, and `ps`.

```bash
kubectl get pods -A | nless
```

Or simulate with this sample data — create a file called `pods.txt`:

```
NAMESPACE     NAME                        READY   STATUS    RESTARTS   AGE
default       nginx-7c5b4f6b9-abc12       1/1     Running   0          5d
default       redis-6d8f7a3c2-def34       1/1     Running   2          12d
kube-system   coredns-5c98db65d4-ghi56     1/1     Running   0          30d
kube-system   etcd-master                  1/1     Running   0          30d
monitoring    prometheus-8b7c6d5e4-jkl78   1/1     Running   1          7d
monitoring    grafana-9a8b7c6d5-mno90      0/1     Pending   0          1d
logging       fluentd-4e3d2c1b0-pqr12      1/1     Running   3          20d
logging       elasticsearch-sts-0          1/1     Running   0          20d
```

```bash
nless pods.txt
```

nless auto-detects the double-space-aligned format. If it doesn't, press `D` and enter two spaces (`  `) as the delimiter.

**Useful workflows:**

- Press `c` and select `NAMESPACE`, then press `f` and type `monitoring` to see only monitoring pods
- Press `c` and select `STATUS`, then press `U` to see a count of pods in each status
- Press `c` and select `RESTARTS`, then press `s` to find pods with the most restarts
- Press `c` and select `STATUS`, then press `e` and type `Running` to see non-running pods

---

## 8. Live Streaming

nless can ingest data in real-time from pipes and shell commands. As new lines arrive, they are **highlighted in green** so you can instantly distinguish fresh data from what was already on screen. Once you've reviewed the new data, press `x` to clear the green highlights and reset everything to normal.

### Streaming from a pipe

Pipe a long-running command directly into nless:

```bash
kubectl get events -w | nless
```

Or try it locally:

```bash
ping localhost | nless
```

New lines appear at the bottom highlighted in green. Press `t` to enable **tail mode** — the cursor stays pinned to the bottom so you always see the latest data as it arrives. When the green highlighting becomes distracting, press `x` to reset it — the next batch of new lines will be highlighted fresh.

### Streaming with `!` shell commands

You can also launch streaming commands from inside nless without leaving the app:

1. Open any file: `nless orders.csv`
2. Press `!` and type:

    ```
    tail -f /var/log/syslog
    ```

3. A new buffer group opens (indicated by `⏵` in the group name) and lines stream in, highlighted in green as they arrive
4. Press `t` to enable tail mode and follow the output
5. Press `x` to reset the green highlights once you've seen the new data
6. Press `}` / `{` to switch between buffer groups, or `L` / `H` to switch buffers within a group

### Monitoring a live log with structure

Stream a log and apply a regex delimiter to parse it on the fly:

```bash
tail -f /var/log/nginx/access.log | nless
```

1. Wait for a few lines to arrive (they appear in green)
2. Press `D` and enter a regex to structure the data:

    ```
    (?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] "(?P<method>\w+) (?P<path>\S+) \S+" (?P<status>\d+) (?P<bytes>\d+)
    ```

3. All existing and future lines are parsed into columns
4. Press `t` for tail mode — new lines continue arriving, now structured and highlighted in green
5. Press `x` to clear the highlights, then press `c` and select `status`, press `f` and type `^5` to filter to 5xx errors in real-time

### Watching Kubernetes pods

```bash
kubectl get pods -A -w | nless -d '  '
```

1. The initial pod list loads as normal text
2. As pods change state, new lines stream in highlighted in green
3. Press `x` to reset highlights after reviewing the changes
4. Press `c` and select `STATUS`, then press `U` to pivot by status — the view focuses on just `STATUS` and `count`
5. When a new line arrives, all columns reappear automatically with updated counts, and the new row is highlighted in green
6. Press `t` to tail and watch changes as they happen

### Running multiple streams side by side

You can open several streaming commands in separate buffers:

1. Start with: `kubectl get pods -w | nless`
2. Press `!` and type `kubectl get events -w` — a second buffer opens with the event stream
3. Press `!` and type `tail -f /var/log/app.log` — a third buffer opens
4. Each `!` command opens in its own buffer group — switch between groups with `}` / `{`
5. Each group streams independently, with new lines highlighted in green — press `x` in any buffer to reset its highlights

### Opening additional files with `O`

You can open more files without leaving nless:

1. Start with: `nless orders.csv`
2. Press `O` and type the path to another file (autocomplete suggests files in the current directory)
3. A new buffer group opens (indicated by `📄` in the group name)
4. Press `}` / `{` to switch between groups
5. Press `R` to rename a group for easy identification

### Streaming JSON logs

Many applications emit structured JSON logs. nless handles these in real-time:

```bash
docker logs -f my-app | nless
```

If each log line is a JSON object, nless auto-detects the format and parses fields into columns. As new JSON lines stream in:

1. They appear highlighted in green with fields already parsed
2. Press `c` and select `level` (or whatever your log level field is called)
3. Press `f` and type `error` to filter to errors — the filter applies to new lines as they arrive too
4. Press `J` on a nested field to extract it as a column

---

## 9. Reshaping Data with Column Visibility

Create a file called `employees.csv`:

```csv
id,first_name,last_name,email,department,title,salary,start_date,office,phone
1,alice,smith,alice@co.com,engineering,senior engineer,120000,2020-03-15,NYC,555-0101
2,bob,jones,bob@co.com,marketing,manager,95000,2019-07-01,SF,555-0102
3,carol,williams,carol@co.com,engineering,staff engineer,140000,2018-01-10,NYC,555-0103
4,dave,brown,dave@co.com,sales,account exec,85000,2021-06-20,CHI,555-0104
5,eve,davis,eve@co.com,engineering,junior engineer,90000,2023-01-05,NYC,555-0105
6,frank,miller,frank@co.com,marketing,director,130000,2017-04-12,SF,555-0106
7,grace,wilson,grace@co.com,sales,manager,100000,2020-11-30,CHI,555-0107
8,hank,moore,hank@co.com,engineering,manager,135000,2019-02-18,NYC,555-0108
```

```bash
nless employees.csv
```

**Filter columns to focus on what matters:**

With 10 columns, scrolling to find the right one is slow. Use `c` to jump directly:

1. Press `C` to filter columns
2. Type `name|department|salary` and press ++enter++
3. Only columns matching the regex are shown

To show all columns again, press `C` and type `all`.

**Reorder columns:**

1. Press `c` and select `salary` to jump straight to it
2. Press `<` to move it left, `>` to move it right
3. Rearrange columns to your preferred layout

**Combine with other features:**

1. Press `C` and type `department|title|salary` to focus
2. Press `c` and select `salary`, then press `s` to sort by compensation
3. Press `c` and select `department`, then press `U` to see employee counts per department
4. Press ++enter++ on `engineering` to see all engineers

---

## 10. Exporting Results

After filtering, sorting, and reshaping data, you can export the current view.

```bash
nless orders.csv
```

1. Press `c` and select `status`, then press `f` and type `shipped`
2. Press `c` and select `quantity`, then press `s` to sort
3. Press `W`, type `shipped-orders.csv`, press ++enter++

The file is written as CSV with only the visible columns and filtered rows.

**Copy a single cell:**

Move the cursor to any cell and press `y` to copy its contents to the clipboard.

**Write to stdout:**

Press `W` and type `-` to write to stdout and exit — useful for piping nless output to other tools:

```bash
nless data.csv  # filter/sort interactively, then W and -
```

---

## 11. Live Debugging a Web Server

This tutorial combines live streaming, regex parsing, and interactive analysis. Start a log stream in one terminal:

```bash
# Simulate a live access log (or use a real one)
while true; do
  echo "$(date '+%Y-%m-%d %H:%M:%S') $(shuf -n1 -e GET POST PUT DELETE) /api/$(shuf -n1 -e users orders health sessions) $(shuf -n1 -e 200 200 200 201 400 404 500) $(shuf -n1 -e 5 12 45 89 120 230)ms"
  sleep 1
done > /tmp/live-access.log &
```

Now open it with nless:

```bash
tail -f /tmp/live-access.log | nless
```

1. Lines stream in and are **highlighted in green** as they arrive
2. Press `D` and enter the regex to structure the data:

    ```
    (?P<date>\S+) (?P<time>\S+) (?P<method>\w+) (?P<path>\S+) (?P<status>\d+) (?P<duration>\d+)ms
    ```

3. Press `t` to enable tail mode — you're now watching structured data scroll by in real-time
4. Press `c` and select `status`, then press `f` and type `^[45]` — you're filtering to errors live
5. New lines still stream in (highlighted in green), but only errors pass the filter
6. Press `c` and select `path`, then press `U` — the view focuses on `path` and `count` so you can see which endpoints are failing most
7. As new errors stream in, all columns reappear with updated counts and the new rows highlighted in green
8. Press ++enter++ on a path to drill into the specific errors for that endpoint
9. Press `W` and type `errors.csv` to snapshot the current errors to a file

---

## 12. Putting It All Together

This tutorial ties together regex parsing, filtering, pivoting, unparsed log handling, and export into a single investigation workflow. Create a file called `app.log`:

```
2025-03-01 08:00:01 INFO  server started on port 8080
2025-03-01 08:00:15 INFO  GET /api/health 200 user=system ip=10.0.0.1
2025-03-01 08:01:22 WARN  GET /api/users 429 user=alice ip=10.0.0.50
2025-03-01 08:01:45 ERROR POST /api/orders 500 user=bob ip=10.0.0.51
2025-03-01 08:02:10 INFO  GET /api/users/1 200 user=alice ip=10.0.0.50
2025-03-01 08:02:33 ERROR GET /api/orders/99 500 user=carol ip=10.0.0.52
2025-03-01 08:03:01 WARN  POST /api/users 400 user=dave ip=10.0.0.53
2025-03-01 08:03:15 INFO  GET /api/health 200 user=system ip=10.0.0.1
2025-03-01 08:04:00 ERROR DELETE /api/users/5 403 user=eve ip=10.0.0.54
2025-03-01 08:04:22 INFO  PUT /api/users/1 200 user=alice ip=10.0.0.50
2025-03-01 08:05:10 ERROR POST /api/orders 500 user=bob ip=10.0.0.51
2025-03-01 08:05:45 INFO  GET /api/orders 200 user=frank ip=10.0.0.55
```

**Step 1 — Parse with regex capture groups:**

```bash
nless app.log
```

Press `D` and enter:

```
(?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\S+) (?P<level>\w+)\s+(?P<method>\w+) (?P<path>\S+) (?P<status>\d+) user=(?P<user>\w+) ip=(?P<ip>\S+)
```

**Step 2 — Investigate errors:**

1. Press `c` and select `level`, then press `f`, type `ERROR`, press ++enter++
2. You see only error lines with structured columns

**Step 3 — Find repeat offenders:**

1. Press `c` and select `user`, then press `U` to group by user
2. Bob appears twice — press ++enter++ on his row to see his specific errors

**Step 4 — Check unparsed lines:**

1. Press `q` to go back to the original regex-parsed buffer
2. Press `~` to see lines that didn't match the regex pattern
3. The `server started` line appears here (it has no method/path/status/user/ip)

**Step 5 — Export findings:**

1. Navigate back to the error-filtered buffer (press `L` / `H` to switch buffers)
2. Press `W`, type `errors.csv`, press ++enter++

---

## 13. Themes and Keymaps

nless is fully customizable — you can switch color themes and keybinding presets on the fly, or create your own from scratch.

### Switching Themes

Press `T` inside nless to open the theme selector. Pick any built-in theme and it applies immediately. Your choice is saved to `~/.config/nless/config.json` so it persists across sessions.

You can also set a theme from the command line:

```bash
nless --theme dracula file.csv
nless -t nord file.csv
```

The CLI flag takes priority over the saved config, so you can try a theme without changing your default.

**Built-in themes:**

| Theme | Style |
|-------|-------|
| `default` | Green accents on dark background |
| `dracula` | Dracula color scheme |
| `monokai` | Monokai Pro colors |
| `nord` | Cool blue/teal palette |
| `solarized-dark` | Solarized dark mode |
| `solarized-light` | Solarized light mode |
| `gruvbox` | Warm retro groove colors |
| `tokyo-night` | Modern dark blues/purples |
| `catppuccin-mocha` | Dark mode with pastels |
| `catppuccin-latte` | Light mode with pastels |

### Creating a Custom Theme

Create a JSON file in `~/.config/nless/themes/`:

```bash
mkdir -p ~/.config/nless/themes
```

```json title="~/.config/nless/themes/ocean.json"
{
    "name": "ocean",
    "cursor_bg": "#264f78",
    "cursor_fg": "#e0e0e0",
    "header_bg": "#1b3a5c",
    "header_fg": "#c8dce8",
    "row_odd_bg": "#0d1b2a",
    "row_even_bg": "#1b2838",
    "highlight": "#00d4aa",
    "accent": "#5dadec",
    "border": "#5dadec",
    "brand": "#5dadec"
}
```

Only the `name` key is required — any color slots you omit inherit from the default theme. Your custom theme appears in the `T` selector immediately.

**Available color slots:**

| Slot | Controls |
|------|----------|
| `cursor_bg` / `cursor_fg` | Selected row background and text |
| `header_bg` / `header_fg` | Column header bar |
| `fixed_column_bg` | Pinned column background (e.g. `count` in pivots) |
| `row_odd_bg` / `row_even_bg` | Alternating row backgrounds |
| `col_odd_fg` / `col_even_fg` | Alternating column text colors |
| `scrollbar_bg` / `scrollbar_fg` | Scrollbar track and thumb |
| `search_match_bg` / `search_match_fg` | Search result highlighting |
| `highlight` | New-line highlighting color (streaming) |
| `accent` | UI accents (borders, active indicators) |
| `status_tailing` | "Tailing" indicator in the status bar |
| `status_loading` | Loading/filtering indicator in the status bar |
| `muted` | De-emphasized text (separators, inactive elements) |
| `border` | Border color for UI elements |
| `brand` | Brand accent color |

### Switching Keymaps

Press `K` inside nless to open the keymap selector. Like themes, your choice is saved automatically.

From the command line:

```bash
nless --keymap less file.csv
nless -k emacs file.csv
```

**Built-in keymaps:**

| Keymap | Style |
|--------|-------|
| `vim` | Vi-like keybindings (default) — `h`/`j`/`k`/`l` navigation, `/` search, `f` filter |
| `less` | Matches less(1) conventions — `space` pages down, `b` pages up, `h` opens help |
| `emacs` | Ctrl/Alt-based — `ctrl+n`/`ctrl+p` navigation, `ctrl+s` search, `alt+f` filter |

### Creating a Custom Keymap

Create a JSON file in `~/.config/nless/keymaps/`:

```bash
mkdir -p ~/.config/nless/keymaps
```

```json title="~/.config/nless/keymaps/custom.json"
{
    "name": "custom",
    "extends": "vim",
    "bindings": {
        "app.search": "ctrl+slash",
        "table.page_down": "space",
        "table.page_up": "shift+space",
        "app.filter": "ctrl+f"
    }
}
```

- `name` — required, appears in the `K` selector
- `extends` — base preset to inherit from (`vim`, `less`, or `emacs`). Defaults to `vim`. You only need to specify the bindings you want to change.
- `bindings` — maps binding IDs to key strings. Use commas to bind multiple keys: `"space,f"`.

**Binding IDs reference:**

| ID | Default (vim) | Action |
|----|---------------|--------|
| `table.cursor_down` | `j` / `down` | Move cursor down |
| `table.cursor_up` | `k` / `up` | Move cursor up |
| `table.cursor_right` | `l` / `w` | Move cursor right |
| `table.cursor_left` | `h` / `b` / `B` | Move cursor left |
| `table.page_down` | `ctrl+d` | Page down |
| `table.page_up` | `ctrl+u` | Page up |
| `table.scroll_top` | `g` | Jump to first row |
| `table.scroll_bottom` | `G` | Jump to last row |
| `table.scroll_to_beginning` | `0` | Jump to first column |
| `table.scroll_to_end` | `$` | Jump to last column |
| `app.search` | `/` | Search |
| `app.filter` | `f` | Filter column |
| `app.filter_cursor_word` | `F` | Filter by cursor word |
| `app.exclude_filter` | `e` | Exclude from column |
| `app.exclude_filter_cursor_word` | `E` | Exclude by cursor word |
| `app.filter_any` | `\|` | Filter all columns |
| `app.search_to_filter` | `&` | Search to filter |
| `app.filter_columns` | `C` | Show/hide columns |
| `app.mark_unique` | `U` | Mark column as pivot key |
| `app.delimiter` | `D` | Change delimiter |
| `app.column_delimiter` | `d` | Split column |
| `app.json_header` | `J` | Extract JSON key |
| `app.write_to_file` | `W` | Write to file |
| `app.run_command` | `!` | Run shell command |
| `app.select_theme` | `T` | Select theme |
| `app.select_keymap` | `K` | Select keymap |
| `app.help` | `?` | Show help |
| `app.add_buffer` | `N` | New buffer |
| `app.show_tab_next` | `L` | Next buffer |
| `app.show_tab_previous` | `H` | Previous buffer |
| `app.close_active_buffer` | `q` | Close buffer / quit |
| `app.rename_buffer` | `r` | Rename buffer |
| `app.show_group_next` | `}` | Next group |
| `app.show_group_previous` | `{` | Previous group |
| `app.rename_group` | `R` | Rename group |
| `app.open_file` | `O` | Open file |
| `buffer.sort` | `s` | Sort column |
| `buffer.next_search` | `n` | Next search match |
| `buffer.previous_search` | `p` | Previous search match |
| `buffer.search_cursor_word` | `*` | Search cursor word |
| `buffer.copy` | `y` | Copy cell |
| `buffer.jump_columns` | `c` | Jump to column |
| `buffer.move_column_right` | `>` | Move column right |
| `buffer.move_column_left` | `<` | Move column left |
| `buffer.toggle_tail` | `t` | Toggle tail mode |
| `buffer.reset_highlights` | `x` | Reset new-line highlights |
| `buffer.view_unparsed_logs` | `~` | View unparsed lines |
