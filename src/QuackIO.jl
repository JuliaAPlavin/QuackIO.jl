module QuackIO

using DuckDB
using Tables
using DataAPI

export write_table, read_csv, read_parquet, read_json

function write_table(file, tbl; kwargs...)
    con = DuckDB.DB()
    DuckDB.register_table(con, tbl, "my_tbl")

    # Add KV_METADATA to query when metadata is supported and format is Parquet
    qstr = if DataAPI.metadatasupport(typeof(tbl)).read && _format_kw(get(kwargs, :format, nothing)) === :parquet
        md = DataAPI.metadata(tbl)
        kwargs = Dict{Symbol,Any}(kwargs)
        isempty(md) || get!(kwargs, :KV_METADATA, md)
        "copy my_tbl to '$(escape_sql_string(file))' $(kwargs_to_db_brackets(kwargs))"
    else
        "copy my_tbl to '$(escape_sql_string(file))' $(kwargs_to_db_brackets(kwargs))"
    end
    @debug "write_table query" qstr
	DBInterface.execute(con, qstr)
end

_format_kw(fmt) = _format_kw(String(fmt))
_format_kw(fmt::String) = Symbol(lowercase(fmt))
read_csv(fmt, file; kwargs...) = _read_file(fmt, file, "read_csv"; kwargs...)
read_parquet(fmt, file; kwargs...) = _read_file(fmt, file, "read_parquet"; kwargs...)
read_json(fmt, file; kwargs...) = _read_file(fmt, file, "read_json"; kwargs...)

function _read_file(fmt, file, duckdb_func::String; kwargs...)
    qstr = "select * from $duckdb_func($(kwarg_val_to_db_incomma(file)) $(kwargs_to_db_comma(kwargs)))"
    @debug "$duckdb_func query" qstr
    matf = fmt isa Function ? fmt : Tables.materializer(fmt)
    table = DBInterface.execute(DuckDB.DB(), qstr) |> matf

    # Write Parquet metadata to table metadata
    if DataAPI.metadatasupport(typeof(table)).write && duckdb_func == "read_parquet"
        _set_parquet_metadata!(table, file)
    end
    table
end

kwargs_to_db_brackets(kwargs) = isempty(kwargs) ? "" : "($(kwargs_to_db(kwargs, " ", kwarg_val_to_db_inbrackets)))"
kwargs_to_db_comma(kwargs) = isempty(kwargs) ? "" : ", $(kwargs_to_db(kwargs, "=", kwarg_val_to_db_incomma))"

kwargs_to_db(kwargs, sep, val_to_db) = join(["$k $sep $(val_to_db(v))" for (k,v) in pairs(kwargs)], ", ")

kwarg_val_to_db(x::AbstractString) = "'$(escape_sql_string(x))'"
kwarg_val_to_db(x::Symbol) = "$x"
kwarg_val_to_db(x::Number) = "$x"
function kwarg_val_to_db(io::IOBuffer)
	fpath, fio = mktemp()
	write(fpath, take!(io))
	return kwarg_val_to_db(fpath)
end

kwarg_val_to_db(x::Union{NamedTuple,AbstractDict}) = "{" * join(("$(kwarg_val_to_db(k)) : $(kwarg_val_to_db(v))" for (k, v) in pairs(x)), ", ") * "}"
kwarg_val_to_db(x::AbstractVector) = join(kwarg_val_to_db.(x), ", ")

kwarg_val_to_db_inbrackets(x) = kwarg_val_to_db(x)
kwarg_val_to_db_incomma(x) = kwarg_val_to_db(x)

kwarg_val_to_db_inbrackets(x::AbstractVector) = "(" * kwarg_val_to_db(x) * ")"
kwarg_val_to_db_incomma(x::AbstractVector) = "[" * kwarg_val_to_db(x) * "]"

escape_sql_string(x::AbstractString) = replace(x, "'" => "''")

function _set_parquet_metadata!(table, file)
    qstr = """select * from parquet_kv_metadata($(kwarg_val_to_db_incomma(file)))"""
    results = DBInterface.execute(DuckDB.DB(), qstr)
    for (; key, value) in results
        DataAPI.metadata!(table, String(key), String(value))  # DuckDB encodes metadata as string (blobs)
    end
end

end
