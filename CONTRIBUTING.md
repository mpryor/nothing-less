## Contributing to This Project
This project uses `pre-commit` to manage and maintain code quality. The pre-commit hooks are defined in the `.pre-commit-config.yaml` file.

This sets up `ruff` for linting and code formatting.

## Install the pre-commit hooks
To install the pre-commit hooks, run the following command:

```bash
poetry run pre-commit install
```

This will set up the pre-commit hooks to run automatically before each commit.

## Run the pre-commit hooks manually
You can also run the pre-commit hooks manually on all files by executing:
```bash
poetry run pre-commit run --all-files
```
