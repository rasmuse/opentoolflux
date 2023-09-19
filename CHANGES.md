# 0.3.0 (unreleased)

- Fix failing tests due to type mismatch `datetime64[ns]` vs `datetime64[ms]`.
- Fix bug that appeared with pydantic v2.x, by pinning to pydantic 1.x.
- Assume ISO8601 strings in import.

# 0.2.3 (2023-03-21)

- Fix link to example in README.

# 0.2.2 (2023-03-21)

- Test on Python 3.11, 3.10, 3.9, 3.8

# 0.2.1 (2023-03-21)

- Improve formatting of log files to accomodate longest module names.

# 0.2.0 (2023-03-21)

- Add `--version` flag to CLI.
- Remove pre-existing figure files with `plot` commands.
- Write docs for the CLI commands.
- Enable running commands without config file.
- Improve README, including a full configuration example.

# 0.1.0 (2023-03-20)

- First release
