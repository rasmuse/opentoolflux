# Todo

- Adjust verbosity to show less output on screen by default
- Add a `--log-level` option to the CLI to control verbosity
- Add more plots (time series, boxplots, more?)
  - When doing that, add another example with more data
- Add CONTRIBUTING instructions for development
- Prepare a full example (config and data)
- Make `picarrito` installable using `pip`
- Use the `tomli` TOML parser library instead of `toml`
- Add notes on units of flux estimates
- Add notes on dimensions of flux estimates (volumetric and molar
- Add friendly error message when trying to import data with columns/dtypes mismatch between existing database and config file
- Add friendly error message when trying to run, e.g., `picarrito fluxes` without having a database
- Allow nullable ints, uints, bools?
- Add check that chamber labels is a one-to-one mapping
- Autogenerate chamber labels when missing
