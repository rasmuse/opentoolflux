# Contents

- [Contents](#contents)
- [Overview](#overview)
- [Installation](#installation)
- [Basic usage](#basic-usage)
  - [Run an example](#run-an-example)
  - [Use the built-in documentation](#use-the-built-in-documentation)
  - [Configure Picarrito using a configuration file](#configure-picarrito-using-a-configuration-file)
  - [Import data from source files](#import-data-from-source-files)
  - [Other ways to get a database](#other-ways-to-get-a-database)
  - [Estimate gas fluxes](#estimate-gas-fluxes)
  - [Plot results](#plot-results)
    - [`flux-fits`: flux estimation diagnostics](#flux-fits-flux-estimation-diagnostics)
- [The database file](#the-database-file)
  - [On data types and database size](#on-data-types-and-database-size)
- [Source data file format](#source-data-file-format)
  - [Notes on timestamps](#notes-on-timestamps)
- [How the flux estimation works](#how-the-flux-estimation-works)
- [Known limitations](#known-limitations)

# Overview

Picarrito is a software to estimate gas fluxes from soil using data from automatic chambers. The software is built for data from the [Picarro](https://www.picarro.com) brand of equipment but in principle could work with, or be adapted to, data from other similar equipment.

The software analyzes time series of gas concentrations in gas collected from multiple automated chambers connected to a single gas analyzer. The multiple chambers sequentially close (e.g. in 20-minute intervals) and gas is continuously pumped into the analyzer from the currently closed chamber. The profile of the concentration change during each chamber closure is used to estimate the flux from the soil under that chamber. [A separate section below describes in further detail how the calculation works and what it assumes.](#how-the-flux-estimation-works)

The software is used through a command-line interface (CLI) which allows the user to:

- Import data from one or more source data files into a database file.
- Filter the database based on time period, alarm status values, and other criteria.
- Identify segments of the  database corresponding to closures of chambers.
- Estimate gas fluxes from the concentration time series during closure, and export these to a [tidy data](https://doi.org/10.18637/jss.v059.i10) file.
- Generate diagrams for diagnostics.

# Installation

Picarrito is written in Python and works with Python 3.8+.

This installation guide assumes that you already have Python 3.8+ and that you know how to use a terminal and install Python packages using `pip`. If you are unsure about any of these points, you might find it helpful to follow the instructions given by any of these sources:

- Instructions from RealPython on [installing Python](https://realpython.com/installing-python/) and [using `pip`](https://realpython.com/what-is-pip/)
- Instructions from [Python.org on getting started with Python](https://www.python.org/about/gettingstarted/)
- Instructions from PyPA on [using pip and virtual environments](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#creating-a-virtual-environment)
- Instructions [_Python for Data Analysis_, 3rd Edition](https://wesmckinney.com/book/preliminaries.html#installation_and_setup) using [miniconda](https://docs.conda.io/)

How to install:

- Optionally, create and activate a virtual environment.
- Install using `pip install picarrito`
- Verify that the installation was successful by running the command `picarrito --help`. If the installation has succeeded, this will show a list of the available commands.

If installation according to these instructions fail, please submit a bug report [using the issue tracker](issues).

# Basic usage

The command-line interface consists of a command `picarrito` with a number of subcommands such as `picarrito import` and `picarrito fluxes`. Here is a quick introduction to basic usage of the command-line interface.

## Run an example

You might find it instructive to get started using an example. An example of a configuration file and input data can be downloaded from here: [example](example).

## Use the built-in documentation

The command-line interface has built-in documentation which is accessed by calling commands such as:

```
picarrito --help
picarrito import --help
```

When adding `--help` to a command, the command will do nothing except print an information message.

We recommend exploring the software by reading the built-in help and experimenting with commands on some test data. Picarrito will never change or delete your source data files, so you can safely play around. (And in any case, you do have a backup of your important research data, _right_?)

## Configure Picarrito using a configuration file

Most configuration is made in a configuration file written in [TOML language](https://toml.io/en/). All the configuration options are listed and explained in the example file found here: [example/picarrito.toml](example/picarrito.toml).

A small number of configuration options can be made on the command line. These are listed and explained [in the built-in documentation](#use-the-built-in-documentation).

Picarrito by default looks for a configuration file named `picarrito.toml` in the working directory. This default can be overridden using the `--config` flag as follows:

```
picarrito --config my_config.toml [command]
```

## Import data from source files

When the configuration file is in place, import data from source files using the following command:

```
picarrito import
```

This will create a new database, or add data to an existing one, located at `picarrito/database.feather`. The data files to read are specified in the `import` section of the [config file](#configuration-options). Read more below about [the source data file format](#source-data-file-format).

Note: `picarrito` will never change or remove the source data files, so you can safely try commands to see what happens. If you want to start over from zero, simply remove the `database.feather` file and run `picarrito import` again.

## Other ways to get a database

It is also possible to copy the `picarrito/database.feather` file between computers. To "export" the database, simply copy the `database.feather` file and save it on a USB stick or network drive, or even send it by email if it's not too large.

You can also create the database file in any other way you like, following the [technical specification of the database below](#the-database-file).

## Estimate gas fluxes

When the `database.feather` file is in place, estimate gas fluxes using the following command:

```
picarrito fluxes
```

This will do several things:

- Optionally, filter the database as specified in the `filters` section of the [config file](#configuration-options).
- Split the remaining data into segments by chamber as specified in the `measurements` section of the [config file](#configuration-options).
- Ignore any segments that are too short or too long, following the `measurements` section of the [config file](#configuration-options). Unexpectedly short segments can be created, for example, if the equipment is shut down or restarted.
- For each of the remaining measurements, estimate the flux as specified in the `fluxes` section of the [config file](#configuration-options). (Read more about [how the flux estimation works](#how-the-flux-estimation-works) below.)
- Save all the results to a file `picarrito/fluxes.csv`.

## Plot results

The command group `picarrito plot` can be used to generate the following figures:

### `flux-fits`: flux estimation diagnostics

To visualize the gas flux estimation and identify potential problems, run the following command:

```
picarrito plot flux-fits
```

This command estimates gas fluxes following [the same steps as the `picarrito fluxes` command](#estimating-gas-fluxes), but instead of a results table it outputs one figure for each measurement in the folder `picarrito/plots/flux-fits`. Each figure shows the gas concentration(s) over time during a chamber closure, and the curve that has been fit to estimate the gas flux(es).

# The database file

Picarrito uses a database file which is just a table stored as a [Feather file](https://arrow.apache.org/docs/python/feather.html). The default file path to the database is `picarrito_db.feather` stored in the same directory as the configuration file.

The database has one row per sample and normally contains the following columns:
- `__TIMESTAMP__`: a timestamp of the sample, in [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time). This column is used as primary key in the database, so the timestamps must be unique. The table must be sorted by timestamp in ascending order. The `__TIMESTAMP__` column is the only mandatory column in the database (although a database with only timestamps is not really useful).
- One column identifying the current chamber. With the Picarro data, this is a number, but any data type will work.
- One column giving sample concentration of each gas to analyze, e.g., `CO2`, `CH4_dry` and/or `N2O_dry`.
- Optionally, additional columns used to filter out samples. For example `ALARM_STATUS` in the case of Picarro data files.

The command `picarrito import` ([see above](#import-data-from-source-files)) can be used to create the database from Picarro (or other) data files. It is also possible to create the database in any way (e.g., using a custom Python or R script). The database file can also be copied between folders or computers without any problem.

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

If in doubt about `float` data types, we suggest to use `float32` which has [at least 6 decimal digits of precision in the significand](https://en.wikipedia.org/wiki/Single-precision_floating-point_format#IEEE_754_standard:_binary32) and thus should be far more precise than practically speaking any gas analyzer out there.

How much space can be saved by choosing smaller data types? As an example, consider a database with the following data columns:

- Timestamp (always 64 bits in a Feather file)
- Chamber number (`uint8` or `uint64`)
- Alarm status (`int8` or `int64`)
- Five gas concentrations: N2O, NO2, CH4, CO2, H2O (`float16` or `float64`)

With the smaller data types, each row will take 64 + 2 * 8 + 5 * 16 = 160 bits = 20 bytes.

With the larger data types, each row will take 64 + 2 * 64 + 3 * 64 = 384 bits = 64 bytes, or 3.2 times as much.

If we collect one data row per second during one year, the resulting database sizes will be either 601 or 1,925 megabytes (MiB). If your computer has less than 4 gigabytes of RAM memory, the program might get slow or even crash with the larger database, and in any case you might care about the difference in space on disk.

This example also shows that if your dataset is perhaps only a few weeks long with frequency 1 second, or maybe one year with frequency 1 minute, the file will be 1-2 orders of magnitude smaller and there is not much point in worrying about database size.

A final related note is that floating-point data to be converted to timestamps in the `picarrito import` command should always be `float64`. The conversion of floating-point Unix timestamps (in seconds) is designed to preserve 6 decimal places (microseconds), something which requires `float64`. The `__TIMESTAMP__` column in the end is always encoded using 64 bits anyway, so there is no space to be saved by parsing the timestamp column as a `float32`. Failure to specify `float64` as data type for the timestamp column raises a helpful error message.

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

Source data files must be be [delimited text files](https://en.wikipedia.org/wiki/Delimiter-separated_values). The default delimiter, following the data files we get from our Picarro equipment, is **one or more** whitespace characters (`sep = '\s+'`), but other delimiters can be specified using the `sep` setting (e.g., `sep = ','` for standard csv files).

(Technical note: The Picarro source files, following roughly the format shown above, can also be seen as [fixed-width files](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_fwf.html), but since the data fields do not contain whitespace, they can also be parsed as delimited files as described here.)

Each source data file must have a one-line header specifying the column names.

## Notes on timestamps

One of the columns must contain timestamps of the measurements. The timestamps can be encoded as:

- Numeric values, which are interpreted to be [Unix timestamps](https://en.wikipedia.org/wiki/Unix_time) expressed in seconds. In Picarro data files, the `EPOCH_TIME` column is a Unix timestamp in seconds (with three decimals, giving millisecond resolution).
- String values, which are parsed using [`pandas.to_datetime()`](https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html) and then converted to UTC timestamps. This means that timestamp strings can be expressed
  - in UTC using a string such as `"2021-12-07T11:00:24.123Z"`,
  - in any other timezone, e.g., `"2021-12-07 13:00:24.123+0200"`, or
  - without timezone, e.g., `"2021-12-07 13:00:24"`, which will be interpreted as UTC.

When running the [`picarrito import` command](#import-data-from-source-files), the timestamp source column is converted to UTC timestamps following the above rules, and renamed to `__TIMESTAMP__`.

# How the flux estimation works

This section will explain
- Assumptions about chambers, closure, no recirculation
- Derivation and solution of differential equation
- How to estimate flux using linear regression on transformed data
- Complication caused by delay from chamber closure to arrival of gas; parameters `t0_delay` and `t0_margin`.
- Sensitivity to parameter errors

# Known limitations
- Only supports setup with no recirculation
- All the settings for measurements and fluxes are the same for all chambers
- Temperature & pressure are constant
