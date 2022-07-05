# Source file format

Here is an example of the default source file format:

```
EPOCH_TIME      ALARM_STATUS   solenoid_valves     N2O_dry
1620345675.170  0              5.0000000000E+00    3.3926340875E-01
1620345675.991  0              5.0000000000E+00    3.3928078030E-01
1620345676.605  2              5.0000000000E+00    3.5087647532E-01
```

Source data files are expected to be delimited text files. The default delimiter, following the data files we get from our Picarro equipment, is one or more whitespace characters (`r"\s+"`), but other delimiters can be specified using the `sep` setting (e.g., `sep = ","` for standard csv files).

Rows with any null values are dropped.
