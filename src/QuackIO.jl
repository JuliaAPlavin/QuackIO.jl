module QuackIO

using DuckDB
using Tables

export write_table, read_csv, read_parquet, read_json

function write_table(file, tbl; kwargs...)
    con = DBInterface.connect(DuckDB.DB)
	DuckDB.register_table(con, tbl, "my_tbl")
    qstr = "copy my_tbl to '$(escape_sql_string(file))' $(kwargs_to_db_brackets(kwargs))"
    @debug "write_table query" qstr
	DBInterface.execute(con, qstr)
end

read_csv(fmt, file; kwargs...) = _read_file(fmt, file, "read_csv"; kwargs...)
read_parquet(fmt, file; kwargs...) = _read_file(fmt, file, "read_parquet"; kwargs...)
read_json(fmt, file; kwargs...) = _read_file(fmt, file, "read_json"; kwargs...)

function _read_file(fmt, file, duckdb_func::String; kwargs...)
    qstr = "select * from $duckdb_func($(kwarg_val_to_db_incomma(file)) $(kwargs_to_db_comma(kwargs)))"
    @debug "$duckdb_func query" qstr
    matf = fmt isa Function ? fmt : Tables.materializer(fmt)
    DBInterface.execute(DBInterface.connect(DuckDB.DB), qstr) |> matf
end

kwargs_to_db_brackets(kwargs) = isempty(kwargs) ? "" : "($(kwargs_to_db(kwargs, " ", kwarg_val_to_db_inbrackets)))"
kwargs_to_db_comma(kwargs) = isempty(kwargs) ? "" : ", $(kwargs_to_db(kwargs, "=", kwarg_val_to_db_incomma))"

kwargs_to_db(kwargs, sep, val_to_db) = join(["$k $sep $(val_to_db(v))" for (k,v) in pairs(kwargs)], ", ")

kwarg_val_to_db(x::AbstractString) = "'$(escape_sql_string(x))'"
kwarg_val_to_db(x::Symbol) = "$x"
kwarg_val_to_db(x::Number) = "$x"
kwarg_val_to_db(x::Union{NamedTuple,AbstractDict}) = "{" * join(("$(kwarg_val_to_db(k)) : $(kwarg_val_to_db(v))" for (k, v) in pairs(x)), ", ") * "}"
kwarg_val_to_db(x::AbstractVector) = join(kwarg_val_to_db.(x), ", ")

kwarg_val_to_db_inbrackets(x) = kwarg_val_to_db(x)
kwarg_val_to_db_incomma(x) = kwarg_val_to_db(x)

kwarg_val_to_db_inbrackets(x::AbstractVector) = "(" * kwarg_val_to_db(x) * ")"
kwarg_val_to_db_incomma(x::AbstractVector) = "[" * kwarg_val_to_db(x) * "]"

escape_sql_string(x::AbstractString) = replace(x, "'" => "''")

end
