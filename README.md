# QuackIO.jl

If it quacks like a tabular IO package, then it is a tabular IO package _(powered by `DuckDB`)_.

`QuackIO` provides a native Julia interface to DuckDB read/write functions:
```julia
using QuackIO

# write_table() arguments are forwarded to DuckDB's `copy tbl to file`:
write_table("my_file.csv", tbl)
write_table("my_file.pq", tbl, format=:parquet)

using Tables, StructArrays

# read_* call DuckDB functions with corresponding names, and support any Julia table format:
tbl = read_csv(columntable, "my_file.csv", delim=";")
tbl = read_parquet(StructArray, "my_file.pq")
```
Thanks to DuckDB and its Julia integration, `QuackIO` functions are performant, and can even be faster than native Julia readers for these formats.
