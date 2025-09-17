# Nothing-less (nless)

Nless is a TUI paging application that has enhanced support for tabular data - such as inferring file delimiters, delimiter swapping on the fly, filtering, sorting, searching, and real-time event parsing

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
- Showcases using a custom (Python engine) regex to parse raw logs into tabular fields.
- Sorts, filters, and searches on those fields.
- Flips the delimiter back to raw, sorts, searches, and filters on the raw logs
[![asciicast](https://asciinema.org/a/IeHSjycb9obCYTVxu7ZDH8WO5.svg)](https://asciinema.org/a/IeHSjycb9obCYTVxu7ZDH8WO5)
