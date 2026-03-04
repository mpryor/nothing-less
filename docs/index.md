# nless

<p align="center">
  <img src="assets/nless-logo.png" width="600px" alt="nless logo"/>
</p>

<p align="center">
  <a href="https://pypi.org/project/nothing-less/"><img src="https://img.shields.io/pypi/v/nothing-less" alt="PyPI"></a>
  <a href="https://pypi.org/project/nothing-less/"><img src="https://img.shields.io/pypi/pyversions/nothing-less" alt="Python"></a>
  <a href="https://github.com/mpryor/nothing-less/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
  <a href="https://github.com/mpryor/nothing-less/actions/workflows/ci.yml"><img src="https://github.com/mpryor/nothing-less/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
</p>

**nless** is a TUI paging application (built on [Textual](https://textual.textualize.io/)) with vi-like keybindings and enhanced support for tabular data and real-time streaming.

## Features

- **Streaming support** — stay up-to-date as new data arrives on stdin
- **Delimiter inference** — no configuration needed; nless infers the delimiter from your data
- **Vi-like keybindings** — familiar to any Vim user, minimize keypresses to analyze a dataset
- **Kubernetes-friendly** — built for K8s use-cases like parsing streams from kubectl
- **Tabular data toolkit** — filter, sort, search, pivot, and reshape data on the fly
- **JSON & log parsing** — convert unstructured data streams into tabular data
- **Buffers** — mutating actions create a new buffer, letting you jump up and down your analysis history
- **Delimiter swapping** — swap between CSV, TSV, space-aligned, JSON, regex with named capture groups, and raw mode on the fly
- **Column delimiters** — split a column into more columns using JSON, regex, or string delimiters
- **Pivoting** — group records by composite key, dive into grouped data

## Demos

### Basic functionality

Starting with a search `/`, applying that search `&`, filtering the selected column by the value within the selected cell `F`, and swapping the delimiter `D` (`raw` and `,`).

[![asciicast](https://asciinema.org/a/k8MOUx01XxnK7Lo9iTcM9QOpg.svg)](https://asciinema.org/a/k8MOUx01XxnK7Lo9iTcM9QOpg)

### Streaming functionality

Showcases nless's features for handling streaming input and interacting with unknown delimitation:

- The view stays up-to-date as new log lines arrive on stdin
- Using a custom Python regex — `{(?P<severity>.*)}\((?P<user>.*)\) - (?P<message>.*)` — to parse raw logs into tabular fields
- Sorts, filters, and searches on those fields
- Flips the delimiter back to raw, sorts, searches, and filters on the raw logs

[![asciicast](https://asciinema.org/a/IeHSjycb9obCYTVxu7ZDH8WO5.svg)](https://asciinema.org/a/IeHSjycb9obCYTVxu7ZDH8WO5)

## Why nless?

As a kubernetes engineer, I frequently need to interact with streaming tabular data — `k get pods -w`, `k get events -w`, etc. I wanted a TUI tool to quickly dissect and analyze this data, and none of the existing alternatives had exactly what I wanted. So I decided to build my own tool, integrating some of my favorite features from other similar tools.

This project is not meant to replace any of the tools mentioned below. Instead, it brings its own unique set of features to complement your workflow.

## Alternatives

Shout-outs to all of the below wonderful tools! If nless doesn't have what you need, they likely will:

- [visidata](https://www.visidata.org/)
- [csvlens](https://github.com/YS-L/csvlens)
- [lnav](https://github.com/tstack/lnav)
- [toolong](https://github.com/Textualize/toolong)
