# Nothing-less (nless)

<img src="./docs/assets/nless-logo.png" width="200px"/>  

**Nless** is a TUI paging application (based on the awesome [Textual](https://textual.textualize.io/) library) with vi-like keybindings.
Nless has enhanced functionality for parsing tabular data:
- inferring file delimiters
- delimiter swapping on the fly
- regex-based parsing of raw logs into tabular data using Python's regex engine
- filtering
- sorting
- searching
- real-time event parsing.


## Getting started
### Dependencies
- python>=3.13
### Installation
`pip install nothing-less`
### Usage
- pipe the output of a command to nless to parse the output `$COMMAND | nless`
- read a file with nless `nless $FILE_NAME`
- redirect a file into nless `nless < $FILE_NAME`
- Once output is loaded, press `?` to view the keybindings

## Demos
### Basic functionality
The below demo shows basic functionality:
- starting with a search `/`
- applying that search `&`
- filtering the selected column by the value within the selected cell `F`
- swapping the delimiter `D` (`raw` and `,`)
  
[![asciicast](https://asciinema.org/a/k8MOUx01XxnK7Lo9iTcM9QOpg.svg)](https://asciinema.org/a/k8MOUx01XxnK7Lo9iTcM9QOpg)  
  
### Streaming functionality
The below demo showcases some of nless's features for handling streaming input, and interacting with unknown delimitation:
- The nless view stays up-to-date as new log lines arrive on stdin (allows pipeline commands, or redirecting a file into nless)
- Showcases using a custom (Python engine) regex, example - `{(?P<severity>.*)}\((?P<user>.*)\) - (?P<message>.*)` - to parse raw logs into tabular fields.
- Sorts, filters, and searches on those fields.
- Flips the delimiter back to raw, sorts, searches, and filters on the raw logs
  
[![asciicast](https://asciinema.org/a/IeHSjycb9obCYTVxu7ZDH8WO5.svg)](https://asciinema.org/a/IeHSjycb9obCYTVxu7ZDH8WO5)  
  
## Features & Functionality
**Navigation**:
- `h` - move cursor left
- `l` - move cursor right
- `j` - move cursor down
- `k` - move cursor up
- `0` - jump to first column
- `$` - jump to final column
- `g` - jump to first row
- `G` - jump to final row
- `w` - move cursor right
- `b` - move cursor left

**Filtering**:
- `f` - will filter the current column and prompt for a filter
- `F` - will filter the current column by the highlighted cell
- `|` - will filter ALL columns and prompt for a filter
- `&` - applies the current search as a filter across all columns

**Searching**:
- `/` - will prompt for a search value and jump to the first match
- `*` - will search all columns for the current highglighted cell value
- `n` - jump to the next match
- `N` - jump to previous match
- `p` - jump to previous match

**Sorting**:
- `s` - toggles ascending/descending sort on the current column

**Delimiter/file parsing**:
- By default, `nless` will attempt to infer a file delimiter from the first few rows sent through stdin. It uses common delimiters to start - `,`, ` `, `|`, `\t`, etc.
- `D` - you can use `D` to explicitly swap the delimiter on the fly. Just type in one of the common delimiters above, and the rows will be re-parsed into a tabular format.
- `D` - alternatively, you can pass in a regex with named capture groups. Those named groups will become the tabular columns, and each row will be parsed and split across those groups. Example `{(?P<severity>.*)}\((?P<user>.*)\) - (?P<message>.*)`
- `D` - additionally you can just pass the word `raw` to see the raw lines behind the data. You can still sort, filter, and sarch the raw lines.
- `D` - last, you can pass a delimiter value of `  ` (two spaces). This will parse text that has been delimited utilizing multiple spaces, while preserving values that have a single space. This is most commonly useful for parsing kubernetes output (`kubectl get pods -w`), for example.
