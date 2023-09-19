# Contents

- [Contents](#contents)
- [Overview](#overview)
- [How the flux estimation works](#how-the-flux-estimation-works)
- [Installation](#installation)
- [Basic usage](#basic-usage)
  - [Run an example](#run-an-example)
  - [Use the built-in documentation](#use-the-built-in-documentation)
  - [Configure OpenToolFlux using a configuration file](#configure-opentoolflux-using-a-configuration-file)
  - [Import data from source files](#import-data-from-source-files)
  - [Other ways to get a database](#other-ways-to-get-a-database)
  - [Show info about the database](#show-info-about-the-database)
  - [Estimate gas fluxes](#estimate-gas-fluxes)
  - [Plot results](#plot-results)
    - [`flux-fits`: flux estimation diagnostics](#flux-fits-flux-estimation-diagnostics)
    - [`flux-time-series`: fluxes over time](#flux-time-series-fluxes-over-time)
- [Notes on dimensions and units](#notes-on-dimensions-and-units)
- [The database file](#the-database-file)
  - [On data types and database size](#on-data-types-and-database-size)
- [Source data file format](#source-data-file-format)
  - [Notes on timestamps](#notes-on-timestamps)
- [Configuration options](#configuration-options)

# Overview

OpenToolFlux is a software to estimate gas fluxes from soil using data from automatic chambers.

The software is used through a command-line interface (CLI) which allows the user to:

- Import data from one or more source data files into a database file.
- Filter the database based on time period, alarm status values, and other criteria.
- Identify segments of the database corresponding to closures of chambers.
- Estimate gas fluxes from the concentration time series during closure, and export these to a [tidy data](https://doi.org/10.18637/jss.v059.i10) file.
- Generate diagrams for diagnostics.

# How the flux estimation works

The software analyzes time series of gas concentrations in gas collected from multiple automated chambers connected to a single gas analyzer. The multiple chambers sequentially close (e.g. in 20-minute intervals) and gas is continuously pumped into the analyzer from the currently closed chamber. The profile of the concentration change during each chamber closure is used to estimate the flux from the soil under that chamber.

A detailed explanation of the flux estimation and its underlying assumptions is to be published in a paper by Galea et al. When published, a reference to the paper will be added here.

# Installation

OpenToolFlux is written in Python and works with Python 3.8+.

This installation guide assumes that you already have Python 3.8+ and that you know how to use a terminal and install Python packages using `pip`. If you are unsure about any of these points, you might find it helpful to follow the instructions given by any of these sources:

- Instructions from RealPython on [installing Python](https://realpython.com/installing-python/) and [using `pip`](https://realpython.com/what-is-pip/)
- Instructions from [Python.org on getting started with Python](https://www.python.org/about/gettingstarted/)
- Instructions from PyPA on [using pip and virtual environments](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#creating-a-virtual-environment)
- Instructions [_Python for Data Analysis_, 3rd Edition](https://wesmckinney.com/book/preliminaries.html#installation_and_setup) using [miniconda](https://docs.conda.io/)

How to install:

- Optionally, create and activate a virtual environment.
- Install using `pip install opentoolflux`
- Verify that the installation was successful by running the command `opentoolflux --help`. If the installation has succeeded, this will show a list of the available commands.

If installation according to these instructions fail, please submit a bug report [using the issue tracker](issues).

# Basic usage

The command-line interface consists of a command `opentoolflux` with subcommands such as `opentoolflux import` and `opentoolflux fluxes`. Here is a quick introduction to basic usage of the command-line interface.

## Run an example

You might find it instructive to get started using an example. An example of a configuration file, input data, and corresponding output data can be downloaded from here: [https://github.com/rasmuse/flux-estimation-example](https://github.com/rasmuse/flux-estimation-example).

## Use the built-in documentation

The command-line interface has built-in documentation which is accessed by calling commands such as:

```
opentoolflux --help
opentoolflux import --help
```

When adding `--help` to a command, the command will do nothing except print an information message.

We recommend exploring the software by reading the built-in help and experimenting with commands on some test data. OpenToolFlux will never change or delete your source data files, so you can safely play around. (And in any case, you do have a backup of your important research data, _right_?)

## Configure OpenToolFlux using a configuration file

Most configuration is made in a configuration file written in [TOML language](https://toml.io/en/). All the configuration options are listed and explained [at the end of this document](#configuration-options).

A small number of configuration options can be made on the command line. These are listed and explained [in the built-in documentation](#use-the-built-in-documentation).

OpenToolFlux by default looks for a configuration file named `opentoolflux.toml` in the working directory. This default can be overridden using the `--config` flag as follows:

```
opentoolflux --config my_config.toml [command]
```

## Import data from source files

When the configuration file is in place, import data from source files using the following command:

```
opentoolflux import
```

This will create a new database, or add data to an existing one, located at `opentoolflux/database.feather`. The data files to read are specified in the `import` section of the [config file](#configuration-options). Read more below about [the source data file format](#source-data-file-format).

Note: `opentoolflux` will never change or remove the source data files, so you can safely try commands to see what happens. If you want to start over from zero, simply remove the `database.feather` file and run `opentoolflux import` again.

## Other ways to get a database

It is also possible to copy the `opentoolflux/database.feather` file between computers. To "export" the database, simply copy the `database.feather` file and save it on a USB stick or network drive, or even send it by email if it's not too large.

You can also create the database file in any other way you like, following the [technical specification of the database below](#the-database-file).

## Show info about the database

To show some basic info about the database file, run the following command:

```
opentoolflux info
```

## Estimate gas fluxes

When the `database.feather` file is in place, estimate gas fluxes using the following command:

```
opentoolflux fluxes
```

This will do several things:

- Optionally, filter the database as specified in the `filters` section of the [config file](#configuration-options).
- Split the remaining data into segments by chamber as specified in the `measurements` section of the [config file](#configuration-options).
- Ignore any segments that are too short or too long, following the `measurements` section of the [config file](#configuration-options). Unexpectedly short segments can be created, for example, if the equipment is shut down or restarted.
- For each of the remaining measurements, estimate the flux as specified in the `fluxes` section of the [config file](#configuration-options). (Read more about [how the flux estimation works](#how-the-flux-estimation-works) below.)
- Save all the results to a file `opentoolflux/fluxes.csv`.

## Plot results

The command group `opentoolflux plot` can be used to generate the following figures:

### `flux-fits`: flux estimation diagnostics

To visualize the gas flux estimation and identify potential problems, run the following command:

```
opentoolflux plot flux-fits
```

This command estimates gas fluxes following [the same steps as the `opentoolflux fluxes` command](#estimating-gas-fluxes), but instead of a results table it outputs one figure for each measurement in the folder `opentoolflux/plots/flux-fits`. Each figure shows the gas concentration(s) over time during a chamber closure, and the curve that has been fit to estimate the gas flux(es). The `t0_delay` and `t0_margin` parameters are also shown in this figure.

### `flux-time-series`: fluxes over time

To generate figures showing the fluxes over time in each chamber, run:

```
opentoolflux plot flux-time-serie
```

This command estimates gas fluxes following [the same steps as the `opentoolflux fluxes` command](#estimating-gas-fluxes) and then creates a figure for each chamber, showing the flux estimates of all gases over time.

# Notes on dimensions and units

The software expects volumetric concentration data and produces volumetric flux estimates, i.e., fluxes in dimensions of length / time (= volume / area / time). The units can be, e.g., ppmv, ppbv, or simply fractions (i.e., m3/m3). The "prefix" implicit in the unit (e.g., 10^-6 for ppmv) also affects the flux estimates. For example, if concentration indata are in ppmv, and all other parameters given in SI units, then the flux estimates are given in 10^-6 m/s (micrometers per second).

As explained in [our paper on the estimation procedure](#how-the-flux-estimation-works), the flux estimates also depend on the chambers' footprint area A, volume V, and the sample flow rate Q. The configuration file (see below) requires specification of A, V, and Q. Technically, the only requirement is that Q/V has unit second, but **we strongly recommend using SI units for A, V, and Q (i.e., specify A in m2, V in m3, and Q in m3/s)**. See the example below.

The output flux estimates will have the units of [c * V / A] per second, where c is the concentration. E.g., if concentrations are in ppmv, V in m3 and A in m2, then flux estimates are in 10^-6 m/s (micrometer per second).

# The database file

OpenToolFlux uses a database file which is just a table stored as a [Feather file](https://arrow.apache.org/docs/python/feather.html). The default file path to the database is `database.feather` stored in the output directory.

The database has one row per sample and normally contains the following columns:
- `__TIMESTAMP__`: a timestamp of the sample, in [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time). This column is used as primary key in the database, so the timestamps must be unique. The table must be sorted by timestamp in ascending order. The `__TIMESTAMP__` column is the only mandatory column in the database (although a database with only timestamps is not really useful).
- One column identifying the current chamber. With the Picarro data, this is a number, but any data type will work.
- One column giving sample concentration of each gas to analyze, e.g., `CO2`, `CH4_dry` and/or `N2O_dry`.
- Optionally, additional columns used to filter out samples. For example `ALARM_STATUS` in the case of Picarro data files.

The command `opentoolflux import` ([see above](#import-data-from-source-files)) can be used to create the database from Picarro (or other) data files. It is also possible to create the database in any way (e.g., using a custom Python or R script). The database file can also be copied between folders or computers without any problem.

## On data types and database size

The database can contain columns of different data types: unsigned (nonnegative) integers (`uint`), signed integers (`int`), floating-point numbers (`float`), booleans (`bool`), and strings (`str`). The numeric datatypes (`uint`, `int`, and `float`) come in different precisions/sizes:

- `uint8`: Integers 0 to 255
- `uint16`: Integers 0 to 65,535
- `uint32`: Integers 0 to 4,294,967,295
- `uint64`: Integers 0 to 18,446,744,073,709,551,615
- `int8`: Integers -128 to 127
- `int16`: Integers -32,768 to 32,767
- `int32`: Integers -2,147,483,648 to 2,147,483,647
- `int64`: Integers -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
- `float16`: 16-bit [floating point](https://en.wikipedia.org/wiki/Floating-point_arithmetic) (a.k.a. "half precision")
- `float32`: 32-bit [floating point](https://en.wikipedia.org/wiki/Floating-point_arithmetic) (a.k.a. "single precision")
- `float64`: 64-bit [floating point](https://en.wikipedia.org/wiki/Floating-point_arithmetic) (a.k.a. "double precision")

Optimizing only for data types capacity and precision, it makes sense to always choose `uint64`, `int64` and `float64`. However, for large databases (long time series), the database can grow fairly large, and it may make sense to choose a more restrictive data type.

For example, if the chambers are encoded as integers 1-12, or even 1-100, it is more than enough to use an `int8` or `uint8`, which takes only 1/8 of the space compared to an `int64`.

For the `float` data types, the choice is not as obvious because there is a loss of precision going from `float64` to `float32` or `float16`. But practically speaking, even a `float16` in many cases will be sufficient. In technical terms, a [`float16` significand has 3-4 significant decimal digits](https://en.wikipedia.org/wiki/Half-precision_floating-point_format#IEEE_754_half-precision_binary_floating-point_format:_binary16).

Therefore, for example, when we work with Picarro data on N2O concentrations in ppmv (roughly 0.3 ppmv N2O), a `float16` can encode the difference beteween 0.300 ppmv and 0.301 ppmv without problem (a difference of 1 ppbv). This precision is much better than the [second-to-second noise in the Picarro concentration data](#source-data-file-format). This example shows that for many purposes, using `float16` instead of `float64` for gas concentrations will practically make very little difference for results. (By the way, the Picarro software converts all gas concentrations to `float64` before doing the flux estimate calculation, so only the value stored in the database is limited by the `float16` encoding.)

**If in doubt about `float` data types, we suggest to use `float32` which has [at least 6 decimal digits of precision in the significand](https://en.wikipedia.org/wiki/Single-precision_floating-point_format#IEEE_754_standard:_binary32) and thus should be far more precise than practically speaking any gas analyzer out there.**

How much space can be saved by choosing smaller data types? As an example, consider a database with the following data columns:

- Timestamp (always 64 bits in a Feather file)
- Chamber number (`uint8` or `uint64`)
- Alarm status (`int8` or `int64`)
- Five gas concentrations: N2O, NO2, CH4, CO2, H2O (`float16` or `float64`)

With the smaller data types, each row will take 64 + 2 * 8 + 5 * 16 = 160 bits = 20 bytes.

With the larger data types, each row will take 64 + 2 * 64 + 3 * 64 = 384 bits = 64 bytes, or 3.2 times as much.

If we collect one data row per second during one year, the resulting database sizes will be either 601 or 1,925 megabytes (MiB). If your computer has less than 4 gigabytes of RAM memory, the program might get slow or even crash with the larger database, and in any case you might care about the difference in space on disk.

This example also shows that if your dataset is perhaps only a few weeks long with frequency 1 second, or maybe one year with frequency 1 minute, the database file will anyway be so small that there are probably very few reasons to worry about database size.

A final related note is that floating-point data to be converted to timestamps in the `opentoolflux import` command should always be `float64`. The conversion of floating-point Unix timestamps (in seconds) is designed to preserve 6 decimal places (microseconds), something which requires `float64`. The `__TIMESTAMP__` column in the end is always encoded using 64 bits anyway, so there is no space to be saved by parsing the timestamp column as a `float32`. Failure to specify `float64` as data type for the timestamp column raises a helpful error message.

# Source data file format

Here is an example of what the default source file format looks like:

```
EPOCH_TIME      ALARM_STATUS   solenoid_valves     N2O_dry
1620345675.170  0              5.0000000000E+00    3.3926340875E-01
1620345675.991  0              5.0000000000E+00    3.3928078030E-01
1620345676.605  2              5.0000000000E+00    3.5087647532E-01
1620345677.312  0              6.0000000000E+00    3.3491837412E-01
```

The data files from our Picarro equipment look like this, but with many more columns containing various information. A full example is given here: [example/indata](example/indata).

Source data files must be be [delimited text files](https://en.wikipedia.org/wiki/Delimiter-separated_values). The default delimiter, following the data files we get from our Picarro equipment, is **one or more** whitespace characters (`sep = '\s+'`), but other delimiters can be specified using the `sep` setting (e.g., `sep = ","` for standard csv files).

For example, setting `sep = ","` in the configuration file, an example input data file would be

```
EPOCH_TIME,ALARM_STATUS,solenoid_valves,N2O_dry
1620345675.170,0,5.0000000000E+00,3.3926340875E-01
1620345675.991,0,5.0000000000E+00,3.3928078030E-01
1620345676.605,2,5.0000000000E+00,3.5087647532E-01
1620345677.312,0,6.0000000000E+00,3.3491837412E-01
```

(Technical note: The Picarro source files, following roughly the format shown above, can also be seen as [fixed-width files](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_fwf.html), but since the data fields do not contain whitespace, they can also be parsed as delimited files as described here.)

Each source data file must have a one-line header specifying the column names.

## Notes on timestamps

One of the columns must contain timestamps of the measurements (in the example above, the colum `EPOCH_TIME`). The timestamps can be encoded as:

- Numeric values, which are interpreted to be [Unix timestamps](https://en.wikipedia.org/wiki/Unix_time) expressed in seconds. In Picarro data files, the `EPOCH_TIME` column is a Unix timestamp in seconds (with three decimals, giving millisecond resolution).
- String values, which are parsed using [`pandas.to_datetime(values, format="ISO8601", utc=True)`](https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html) and then converted to UTC timestamps. This means that timestamp strings can be expressed
  - in UTC using a string such as `"2021-12-07T11:00:24.123Z"`,
  - in any other timezone, e.g., `"2021-12-07 13:00:24.123+0200"`, or
  - without timezone, e.g., `"2021-12-07 13:00:24"`, which will be interpreted as UTC.

When running the [`opentoolflux import` command](#import-data-from-source-files), the timestamp source column is converted to UTC timestamps following the above rules, and renamed to `__TIMESTAMP__`.

# Configuration options

Below is an example configuration file listing all the configuration options.

```toml
# The [general] section is optional.
[general]
# `outdir` is the output directory path, relative to the configuration file.
# Optional. Default "opentoolflux".
outdir = "outdata"

# The [import] section is optional. If you already have a Feather database
# file, this is not needed.
[import]
# src lists the path(s) to input data files which should be delimited text
# files as explained above. All paths are relative to the configuration file.
# When running the `opentoolflux import` command, these paths are searched.
src = [
    "path/to/filename.dat",  # Path to a specific file
    "indata/2022/06/01/*.dat",  # Unix-style glob patterns using *
    "indata/2022/**/*.dat",  # Also ** globs are supported.
]

# sep specifies the delimiter used in the data files..
sep = "\\s+"  # One or more whitespace characters (the default).
# sep = ","  # Alternative for comma-separated files
# sep = "\\t,"  # Alternative for tab-separated files

# timestamp_col gives the name of the column containing sample timestamps.
# See the README section on timestamps for details on formats.
timestamp_col = "EPOCH_TIME"

# [import.columns] gives the names of the columns to import and their data types.
# See the opentoolflux README section on data types for more details.
[import.columns]
EPOCH_TIME = "float64"
valve_number = "float16"
ALARM_STATUS = "uint8"
CO2 = "float32"
CH4_dry = "float32"
N2O_dry = "float32"


# Filters are optional.
#
# A [filters.x] section specifies the filtering to be done on column x.
# Filters can specify `allow_only`, `disallow` and `min` and `max`
# as shown in examples below. The data matched by a filter is treated
# as if it was not present in the database; i.e., data "removed" by
# filters do not contribute at all to flux estimates and are not shown
# in plots generated.

[filters.ALARM_STATUS]
disallow = [4]

# __TIMESTAMP__ is a special column created in the database to store
# the timestamps. This has datetime data type and any values specified in the
# filter settings are automatically converted to datetime too.
[filters.__TIMESTAMP__]
min_value = "2022-05-09"
max_value = "2022-05-10 06:00:00"

# Advanced note on data types:
# Filtering is done after import, i.e., using the data types specified in
# the [import.columns] section above. Values specified in filter settings
# are converted to the data types of the  corresponding column; therefore,
# in the `valve_number` example below, the `allow_only` values specified
# as integers will be converted to `float16`, the specified data type for
# that column, before the filter is applied.
[filters.valve_number]
allow_only = [2, 3, 6, 7]

[filters.CO2]
min = 0
max = 2000

# The [measurements] section specifies how to split the database into segments
# corresponding to chamber closures.
[measurements]
# chamber_col is the name of the database column indicating the current chamber.
# A new segment starts when the value in this column changes.
chamber_col = "valve_number"

# The time values below (max_gap, min_duration, and max_duration) should be set
# to values corresponding to the setup of your experiment.
#
# In our example, the input data have ca 1 Hz sampling frequency, so a gap
# of 10 seconds indicates a problem/restart which we treat as a "new" segment;
# and the segment length normally should be 20 minutes, so we discard any
# segments deviating more than 30 seconds from that.
max_gap = "00:00:10"  # Max 10 seconds between samples.
min_duration = "00:19:30"  # Segment length minimum 19 minutes 30 seconds.
max_duration = "00:20:30"  # Segment length maximum 20 minutes 30 seconds.

# The [fluxes] section specifies columns containing concentration data
# to be analyzed.
[fluxes]
gases = ["N2O_dry", "CH4_dry", "CO2"]  # the list of gas-concentration columns

# Chamber dimensions and sampled gas flow
#
# See notes on units in the README. In summary:
# - Concentrations can be in any unit (e.g., ppmv or simple fractions m3/m3).
# - V / Q must have unit second. (E.g., if Q is m3/s, then must be in m3.)
# - A can have any unit.
#
# The resulting (volumetric) flux estimates will have the units of
# [c * V / A] per second, where c is the concentration.
# E.g., if concentrations are in ppmv, V in m3 and A in m2, then flux
# estimates are in 10^-6 m/s (micrometer per second).
#
# We recommend using straight SI units throughout to minimize confusion.
A = 0.25  # Soil area covered by the chamber, here 0.25 m2
V = 50e-3  # Volume of chamber, here 50 liters in m3
Q = 4.16e-6  # Gas flow drawn from chamber, here 0.25 liters/minute in m3/s

# t0_delay is the time delay from chamber closure until the first gas arrives
# in the gas analyzer. This delay depends on the tube length which may be
# different between chambers.

# Optionally, the same t0_delay can be set for all chambers:
# t0_delay = "00:06:00"  # 6 minutes

# t0_margin specifies an extra duration of data to discard after t0_delay,
# to avoid estimation errors if the actual delay is longer which leads to "old"
# gas concentrations being erroneously included in the curve fit.
# Calculations are made using t0 equal to the closure start plus t0_delay.
# See figures generated by `opentoolflux plot flux-fits` for an illustration.
t0_margin = "00:02:00"

# The `t0_delay` can be set to one value, e.g., t0_delay = "00:06:00" as above,
# or as a table with different t0_delay for each chamber, as follows.
# This is the preferred solution if the delay is substantially different
# between chambers.
# The left-hand side of these assignments is a string representation
# of the chamber value, i.e., in this example the value found in the
# `valve_number` column.
[fluxes.t0_delay]
2 = "00:04:00"  # 4 minutes
3 = "00:06:00"  # 6 minutes
6 = "00:05:30"  # 5 minutes 30 seconds
7 = "00:06:30"  # 6 minutes 30 seconds

# The [chamber_labels] section is optional. The labels may correspond to
# labels used in the experimental design. If specified, these labels
# are used in output file names and printed in figures.
[chamber_labels]
2 = "A1"
3 = "A2"
6 = "B1"
7 = "B2"
```
