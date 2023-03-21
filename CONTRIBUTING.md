# Install in dev mode

1. Create a virtual environment

```
python -m venv .venv
```

2. Activate the venv.

On Linux:
```
python3 -m venv .venv
. .venv/bin/activate
```

On Windows:
```
.venv\Scripts\activate
```

3. Install dev dependencies and then the package in editable mode:
```
pip install -r requirements/dev.txt && pip install -e .
```

4. Install pre-commit hooks
```
pre-commit install --install-hooks
```

# Run tests

To run the test suite, do

```
pytest
```

To run the whole tox testing party, do

```
tox
```

The `.python-version` file specifies Python versions used for testing; this is intended for use with [`pyenv`](https://github.com/pyenv/pyenv).


# Specifying dev and testing requirements using  `pip-compile`

`pip-compile` is used to transform `requirements/*.in` to requirement lists `requirements/*.txt`. `pip-compile` is part of pip-tools: <https://github.com/jazzband/pip-tools>

To add a dev or testing requirement, add the requirement to the `.in` file and then do
```
pip-compile requirements/{dev|test|whatever}.in
```


# Build

```
python -m build
```

# Release on PyPI

```
python -m twine upload --repository pypi dist/*
```

See also <https://packaging.python.org/tutorials/packaging-projects/>.


# Explanations of some choices

## `.gitattributes`

tl;dr: `*	text=auto` because Windows

Slightly longer explanation: We don't want to depend on each contributor having the right settings in their git config. `.gitattributes` can be used to define what to do about line endings on repository level.

More advanced settings may be useful to add depending on your needs: https://git-scm.com/docs/gitattributes
