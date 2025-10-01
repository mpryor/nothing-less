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

## Why?
As a kubernetes engineer, I frequently need to interact with streaming tabular data. `k get pods -w`, `k get events -w`, etc. I want a TUI tool to quickly dissect and analyze this data - and none of the existing alternatives had exactly what I wanted:
- streaming support
- delimiter inference - I don't want to do a bunch of work to tell the program what type of data it's viewing, I want it to infer it if possible
- vi-like keybindings
So I decided to build my own tool, integrating some of my favorite features that I've seen in other similar tools.

## Goals
This project is not meant to be a replacement/competitor for any of the tools mentioned in the alternatives section at the end. Instead, it's meant to bring its own unique set of features to compliment your workflow.
- UX:
  - vi-like keybindings, familiar to any VIM user
  - minimize the number of keypresses to analyze a dataset
- Kubernetes support:
  - support for K8s usecases out of the box - such as parsing data streams from kubectl
- Tabular data toolkit:
  - broad support for a variety of use-cases analyzing,filtering,sorting, and searching tabular data
  - converting data streams *into* tabular data, such as JSON log parsing

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
**Buffers**:
- All mutating actions will apply the action by replicating the current "buffer". This allows you to jump up and down the stack to see how you've analyzed your data.
- `[1-9]` - will select the buffer at the index corresponding to the input number
- `L` - selects the next buffer
- `H` - select the previous buffer
- `q` - closes the current active buffer, or the program if all buffers are closed
- `N` - creates a new buffer from the original data

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
- `ctrl+u` - page up
- `ctrl+d` - page down
- `c` - to select a column to jump the cursor to

**Column visibility**
- `C` - will prompt for a regex filter to selectively display columns, or `all` to see all columns. TIP: use a non-existing column (`none`, for example) to only see the current pivots/count
- `>` - will move the current column one to the right
- `<` - will move the current column one to the left

**Pivoting**
- `U` - will mark the selected column as part of a composite key to group records by, adding a `count` column pinned to the left
  - `enter` - pressing enter while the cursor is over one of the composite key columns will "dive in" to the data set behind the pivot - applying the composite key as a filter in a new buffer 

**Filtering**:
- `f` - will filter the current column and prompt for a filter
- `F` - will filter the current column by the highlighted cell
- `|` - will filter ALL columns and prompt for a filter
- `&` - applies the current search as a filter across all columns

**Searching**:
- `/` - will prompt for a search value and jump to the first match
- `*` - will search all columns for the current highglighted cell value
- `n` - jump to the next match
- `p` - jump to previous match

**Output**:
- `W` - will prompt for a file to write the current buffer to. `-` can be used to write to `stdout`, allowing you to use `nless` inside of a command chain `cat $MY_FILE.txt | nless | grep -i active` for example.

**Sorting**:
- `s` - toggles ascending/descending sort on the current column

**json**:
- in addition to the `json` delimiter that can be set per session or per column, there's also support for json actions:
- `J` - will prompt you to select a json field, under the current cell, to add as a column for further filtering/sorting/etc

**Delimiter/file parsing**:
- By default, `nless` will attempt to infer a file delimiter from the first few rows sent through stdin. It uses common delimiters to start - `,`, ` `, `|`, `\t`, etc.
- `D` - you can use `D` to explicitly swap the delimiter on the fly. Just type in one of the common delimiters above, and the rows will be re-parsed into a tabular format.
- `D` - alternatively, you can pass in a regex with named capture groups. Those named groups will become the tabular columns, and each row will be parsed and split across those groups. Example `{(?P<severity>.*)}\((?P<user>.*)\) - (?P<message>.*)`
- `D` - additionally you can just pass the word `raw` to see the raw lines behind the data. You can still sort, filter, and sarch the raw lines.
- `D` - pass the word `json` to parse the first set of keys from each JSON line (or read the whole buffer in as a JSON object/list)
- `D` - last, you can pass a delimiter value of `  ` (two spaces). This will parse text that has been delimited utilizing multiple spaces, while preserving values that have a single space. This is most commonly useful for parsing kubernetes output (`kubectl get pods -w`), for example.

- `d` - transforms a column into more columns using a columnar delimiter (currently `json` is the only delimiter supported)

## Contributing
Contributions are welcome! Please open an issue or a pull request - check out the [contributing guidelines](CONTRIBUTING.md) for more information.

## Alternatives
Shout-outs to all of the below wonderful tools! If my tool doesn't have what you need, they likely will:
- [visidata](https://www.visidata.org/)
- [csvlens](https://github.com/YS-L/csvlens)
- [lnav](https://github.com/tstack/lnav)
- [toolong](https://github.com/Textualize/toolong)
