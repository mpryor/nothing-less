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

## Tests
Run the full test suite:
```bash
poetry run pytest
```

### Performance tests
The suite includes performance regression tests (`tests/test_perf.py`) marked with `@pytest.mark.perf`. These load 50K rows and assert that key operations complete within a threshold derived from measured baselines.

```bash
# Run only perf tests
poetry run pytest -m perf

# Skip perf tests for a faster feedback loop
poetry run pytest -m "not perf"
```

If you make changes that affect data processing performance, run the perf tests to verify there are no regressions. If a legitimate change makes an operation slower, update the corresponding baseline in `_BASELINES` inside `tests/test_perf.py` with a new measured value.
