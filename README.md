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
