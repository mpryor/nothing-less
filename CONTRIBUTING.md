# Contributing to This Project
## Dependencies
- This project contains a `~/.tool-versions` to manage Python versions.
- Make sure you have the correct Python version installed. You can use `mise` to manage multiple Python versions.

## Poetry
This project uses `poetry` for dependency management.
- Install poetry if you haven't already. You can find the installation instructions [here](https://python-poetry.org/docs/#installation).
- To install the dependencies, run the following command:
```bash
poetry install
```

## Semantic Commit Messages
Make sure to follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification when making commits.

## Pre-commit hooks
This project uses `pre-commit` to manage and maintain code quality. The pre-commit hooks are defined in the `.pre-commit-config.yaml` file.

This sets up `ruff` for linting and code formatting.

### Install the pre-commit hooks
To install the pre-commit hooks, run the following command:

```bash
poetry run pre-commit install
```

This will set up the pre-commit hooks to run automatically before each commit.

### Run the pre-commit hooks manually
You can also run the pre-commit hooks manually on all files by executing:
```bash
poetry run pre-commit run --all-files
```
