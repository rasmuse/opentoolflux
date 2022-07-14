# Todo

- Adjust verbosity to notice on screen by default
- Add --log-level option
- Make it `pip` installable
- Add more plots
- Add CONTRIBUTING instructions
- Prepare a full example






# Todo

- Allow ISO8601 string timestamps (UTC) in input data
- Use pd.to_timedelta() to convert config file input data
- Use numpy.ndarray.astype() to convert filter settings
- Possibly allow localized time in plot outputs

# Functionality

## `picarrito init`
- Create config file with default settings

## `picarrito import`
- Read files into the database
  - Settings
    - file glob(s) to search
    - columns to import and their data types
    - column to use as time stamp (primary key in the database)
- Assumptions (subject to change)
  - time stamp input data is a floating-point number Unix timestamp in seconds
  - no null values

## `picarrito measurements`
1. Filter data on row level (by values)
2. Identify measurements
  - Split by valve column and/or long time gap
  - Filter by duration
- Settings
  - `valve_column`
  - `min_duration`
  - `max_duration`
  - row filters: `min_value`, `max_value`, `allow_only`, `disallow`
- Assumptions (subject to change)
  - The valves are integers

## `picarrito fluxes`
- Estimate fluxes from previously identified measurements
- Join valve numbers with valve labels (see below)
- Export a tidy data file
- Settings
  - `gases` (name of columns)
  - `t0_delay`
  - `t0_margin`
  - `A` (area), `Q` (gas flow), `V` (volume), `P` (pressure), `T` (temperature)
- Assumptions (subject to change)
  - `A`, `Q`, `V`, `P`, `T` are constants
    - Possible extension: `T` and `P` could be data columns; `A`, `Q`, `V` could be valve dependent

## `picarrito plot`
- Subcommands for different plots
  - `flux-fits`
  - `boxplots`
  - `time-series`
- All these should use `valve_labels` if available (see below) and also use the order specified there

## Common settings
- `valve_labels` (optional one-to-one mapping valve labels to valve numbers)


# Internal database

## Example of columns

- time stamp: 8 bytes (64-bit integer; Unix timestamp in ms)
- valve: 1 byte (uint8)
- alarm: 2 bytes (int16)
- gases (e.g., 3 gases): 3*8 bytes (float64)

This implies a data size of ~35 bytes per row; assuming 1 Hz measurements that means roughly 1 GB per year.
