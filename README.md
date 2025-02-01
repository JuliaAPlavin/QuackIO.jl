# QuackIO.jl 🐣🦆

If it quacks like a tabular IO package, then it is a tabular IO package _(powered by `DuckDB`)_.

`QuackIO` provides a native Julia interface to DuckDB read/write functions. They support all Julia table types that follow the `Tables.jl` interface. Reading and writing is most efficient with columnar table types, such as `StructArray` or `columntable`.

- `write_table(filename, table; options...)`\
Executes `COPY table TO file (options)` in DuckDB. Here, `table` is an arbitrary Julia table object:
```julia
using QuackIO

write_table("my_file.csv", tbl)
write_table("my_file.pq", tbl, format=:parquet)
```
- `read_*(tabletype, filename; options...)` where `*` is `csv`, `parquet`, or `json`\
Calls the corresponding DuckDB function and converts the result to the specified Julia table type.
```julia
using QuackIO, Tables, StructArrays

# read_* call DuckDB functions with corresponding names
# and support any Julia table format:
tbl = read_csv(columntable, "my_file.csv", delim=";")
tbl = read_parquet(StructArray, "my_file.pq")
tbl = read_json(rowtable, "my_file.json")
```

Thanks to DuckDB and its Julia integration, `QuackIO` functions are performant. They can even be faster than native Julia readers for these formats.

## Querying, row and column selection

`QuackIO` is based on DuckDB – a fully-featured SQL database. Such a backend makes it straightforward to do basic data manipulation on the fly, without materializing the whole table in memory.\
Thanks to the [SQLCollections.jl](https://github.com/JuliaAPlavin/SQLCollections.jl) integration, even the syntax for performing manipulations in SQL is basically the same as for in-memory Julia datasets. Compare:
```julia
using QuackIO
using Tables
using DataPipes

# basic:
# load everything into memory, then filter and select columns:
@p read_csv(rowtable, "https://duckdb.org/data/duckdb-releases.csv") |>
   filter(startswith(_.version_number, "0.10.")) |>
   map((;_.version_number, _.release_date)) |>
   first(__, 3)


using SQLCollections

# with SQLCollections:
# filter and select columns on the fly in DuckDB,
# load only the small final table into memory
@p read_csv(SQLCollection, "https://duckdb.org/data/duckdb-releases.csv") |>
   filter(@o startswith(_.version_number, "0.10.")) |>
   map(@o (;_.version_number, _.release_date)) |>
   first(__, 3) |>
   collect
```

---
_Experimental:_
Very common tasks, such as column selection, are also supported through dedicated keyword arguments in `read_*` functions. For example, `read_csv(...; select=["version_number", "release_date"], limit=3)`; see docstrings for more details.
