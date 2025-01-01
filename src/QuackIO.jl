module QuackIO

using DuckDB
using Tables

export write_table, read_csv, read_parquet

function write_table(file, tbl; kwargs...)
    con = DBInterface.connect(DuckDB.DB)
	DuckDB.register_table(con, tbl, "my_tbl")
    qstr = "copy my_tbl to '$file' $(kwargs_to_db_brackets(kwargs))"
    @debug "write_table query" qstr
	DBInterface.execute(con, qstr)
end

function read_csv(fmt, file; kwargs...)
    qstr = "select * from read_csv('$file' $(kwargs_to_db_comma(kwargs)))"
    @debug "read_csv query" qstr
    matf = fmt isa Function ? fmt : Tables.materializer(fmt)
    DBInterface.execute(DBInterface.connect(DuckDB.DB), qstr) |> matf
end

function read_parquet(fmt, file; kwargs...)
    qstr = "select * from read_parquet('$file' $(kwargs_to_db_comma(kwargs)))"
    @debug "read_csv query" qstr
    matf = fmt isa Function ? fmt : Tables.materializer(fmt)
    DBInterface.execute(DBInterface.connect(DuckDB.DB), qstr) |> matf
end

kwargs_to_db_brackets(kwargs) = isempty(kwargs) ? "" : "($(kwargs_to_db(kwargs, " ")))"
kwargs_to_db_comma(kwargs) = isempty(kwargs) ? "" : ", $(kwargs_to_db(kwargs, "="))"

kwargs_to_db(kwargs, sep) = join(["$k $sep $(kwarg_val_to_db(v))" for (k,v) in pairs(kwargs)], ", ")
kwarg_val_to_db(x::AbstractString) = "'$x'"
kwarg_val_to_db(x::Symbol) = "$x"
kwarg_val_to_db(x) = "$x"

end
