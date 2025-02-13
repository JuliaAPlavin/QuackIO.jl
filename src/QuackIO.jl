module QuackIO

using DuckDB
using Tables
using DataAPI

export write_table, read_csv, read_parquet, read_json

function write_table(file, tbl; kwargs...)
    kwargs = merge(NamedTuple(kwargs), _table_metadata_to_kwargs(file, tbl; kwargs))

    con = DuckDB.DB()
	DuckDB.register_table(con, tbl, "my_tbl")
    qstr = "copy my_tbl to '$(escape_sql_string(file))' $(kwargs_to_db_brackets(kwargs))"
    @debug "write_table query" qstr
	DBInterface.execute(con, qstr)
end

"""
    QuackIO.read_csv(fmt, file; kwargs...)

Read the CSV file `file` into the data structure `fmt`.  Equivalent to `read_file(fmt, file, :csv; kwargs...)`,
see [`read_file`](@ref).
"""
read_csv(fmt, file; kwargs...) = read_file(fmt, file, :csv; kwargs...)

"""
    QuackIO.read_parquet(fmt, file; kwargs...)

Read the parquet file `file` into the data structure `fmt`.  Equivalent to `read_file(fmt, file, :parquet; kwargs...)`,
see [`read_file`](@ref).
"""
read_parquet(fmt, file; kwargs...) = read_file(fmt, file, :parquet; kwargs...)

"""
    QuackIO.read_json(fmt, file; kwargs...)

Read the JSON file `file` into the data structure `fmt`.  Equivalent to `read_file(fmt, file, :json; kwargs...)`,
see [`read_file`](@ref).
"""
read_json(fmt, file; kwargs...) = read_file(fmt, file, :json; kwargs...)

_colname_sql_string(col::Union{Symbol,AbstractString}) = string(col)
_colname_sql_string(col::Pair) = string(_colname_sql_string(col[1]), " AS ", _colname_sql_string(col[2]))

function columnsstring(cols)
    if isnothing(cols) || isempty(cols)
        "*"
    else
        join(map(_colname_sql_string, cols), ", ")
    end
end

function duckdb_filetype_func(filetype::Symbol)
    if filetype == :parquet
        "read_parquet"
    elseif filetype == :csv
        "read_csv"
    elseif filetype == :json
        "read_json"
    elseif filetype == :unknown  # this should let DDB guess
        ""
    else
        throw(ArgumentError("unrecognized file type $filetype"))
    end
end

function _selectstring(file, duckdb_func::AbstractString; select=nothing, limit::Integer=-1, kwargs...)
    cols = columnsstring(select)
    readstr = if isempty(duckdb_func)
        kwarg_val_to_db(file)
    else
        string(duckdb_func, "(", kwarg_val_to_db_incomma(file), " ", kwargs_to_db_comma(kwargs), ")")
    end
    qstr = "SELECT $cols FROM $readstr"
    limit ≥ 0 && (qstr = string(qstr, '\n', "LIMIT ", limit))
    qstr
end

function selectstring(file, filetype::Symbol; kwargs...)
    duckdb_func = duckdb_filetype_func(filetype)
    _selectstring(file, duckdb_func; kwargs...)
end

"""
    QuackIO.read_file(fmt, filename, filetype::Symbol=:unknown; select=nothing, kwargs...)

Read a table from file or files `filename` into the data structure `fmt` using DuckDB.  Examples of `fmt`
include `DataFrame`, `StructArray`, `columntable` and `rowtable`.

`filetype` can be `:parquet`, `:csv`, `:json` or `:unknown`.  If `:unknown`, DuckDB will attempt to guess
the filetype, this is equivalent to passing the file name only to DuckDB without an explicit read function.

`select` can be `nothing` or an iterator.  If `nothing` or an empty iterator, all columns will be read.  A
non-empty iterator must have elements that are strings, `Symbol` or `Pair` of string or `Symbol`.  Only the
specified columns will be read, `Pair`s will provide aliases for the read columns.
"""
function read_file(fmt, file, filetype::Symbol=:unknown; kwargs...)
    qstr = selectstring(file, filetype; kwargs...)
    @debug("running query string:", qstr)
    matf = fmt isa Function ? fmt : Tables.materializer(fmt)
    table = DBInterface.execute(DuckDB.DB(), qstr) |> matf
    _read_metadata!(table, file, filetype)
    return table
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


function _table_metadata_to_kwargs(file, tbl; kwargs)
    has_metadata = DataAPI.metadatasupport(typeof(tbl)).read && !isempty(DataAPI.metadata(tbl))
    if has_metadata && lowercase(String(get(kwargs, :format, nothing))) === "parquet"
        return (;KV_METADATA=DataAPI.metadata(tbl))
    end
    return (;)
end

function _read_metadata!(table, file, filetype::Symbol)
    if DataAPI.metadatasupport(typeof(table)).write && filetype == :parquet
        qstr = """
            select *
            from parquet_kv_metadata($(kwarg_val_to_db_incomma(file)))
            where key != 'ARROW:schema'  -- ignore non-string valued internal metadata
        """
        results = DBInterface.execute(DuckDB.DB(), qstr)
        for (;key, value) in results
            # DuckDB encodes metadata as strings (blobs)
            DataAPI.metadata!(table, String(key), String(value))
        end
    end
end

end
