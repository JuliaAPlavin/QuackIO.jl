module QuackIO

using DuckDB
using Tables
using DataAPI

export write_table, read_csv, read_parquet, read_json
# public read_file  # should be public, but 1.10 doesn't support this


const COMMON_ARGUMENTS_DOC = """
- `fmt`: Julia table type or materializer function. Common examples include `columntable` and `rowtable` from `Tables.jl`, and types like `StructArray`, `DataFrame`, `TypedTables.Table`.

## Experimental arguments

- `select = nothing`: columns to select from the file; can be `nothing` or an iterator.
If `nothing`, all columns will be read. If an iterator, it must have elements that are strings or `Symbol`s to select a subset of columns, or `Pair`s of strings or `Symbol`s to rename them.
- `limit = nothing`: number of rows to be read, or `nothing` to read the entire file.
"""

_docstring_for_read(fmt) = """    read_$(lowercase(fmt))(fmt, file; kwargs...)

Read the $fmt file `file` into the Julia table `fmt` using DuckDB.
Equivalent to `read_file(fmt, file, :csv; kwargs...)`.

# Arguments

$COMMON_ARGUMENTS_DOC
"""


function write_table(file, tbl; kwargs...)
    @assert !any(isuppercase, String(get(kwargs, :format, "")))
    kwargs = merge(NamedTuple(kwargs), _table_metadata_to_kwargs(file, tbl; kwargs))

    con = DuckDB.DB()
	DuckDB.register_table(con, tbl, "my_tbl")
    qstr = "copy my_tbl to '$(escape_sql_string(file))' $(kwargs_to_db_brackets(kwargs))"
    @debug "write_table query" qstr
	DBInterface.execute(con, qstr)
end

@doc _docstring_for_read("CSV")
read_csv(fmt, file; kwargs...) = read_file(fmt, file, :csv; kwargs...)

@doc _docstring_for_read("Parquet")
read_parquet(fmt, file; kwargs...) = read_file(fmt, file, :parquet; kwargs...)

@doc _docstring_for_read("JSON")
read_json(fmt, file; kwargs...) = read_file(fmt, file, :json; kwargs...)

"""
    QuackIO.read_file(fmt, file, filetype=nothing; kwargs...)

Read a table from file or files `file` into the Julia table `fmt` using DuckDB.

# Arguments

- `filetype = nothing`: can be `:parquet`, `:csv`, `:json` or `nothing`.
If a symbol, calls the `read_\$filetype` DuckDB function.
If `nothing`, DuckDB will attempt to guess the filetype, equivalent to passing the file name to DuckDB without an explicit `read_*` function.

$COMMON_ARGUMENTS_DOC
"""
read_file(fmt, file, filetype::Union{Symbol,Nothing}=nothing; kwargs...) =
    _read_file(fmt, file, isnothing(filetype) ? nothing : "read_$filetype"; kwargs...)

function _read_file(fmt, file, duckdb_func; select=nothing, limit=nothing, kwargs...)
    qstr = "select $(_select_string(select)) from $(_from_string(file, duckdb_func; kwargs...)) $(_limit_string(limit))"
    @debug "$duckdb_func query" qstr
    matf = fmt isa Function ? fmt : Tables.materializer(fmt)
    table = DBInterface.execute(DuckDB.DB(), qstr) |> matf
    _read_metadata!(table, file; duckdb_func)
    return table
end


_select_string(select::Nothing) = "*"
_select_string(select) = join(map(_colname_sql_string, select), ", ")
_colname_sql_string(col::Union{Symbol,AbstractString}) = "\"$col\""
_colname_sql_string(col::Pair) = "$(_colname_sql_string(col[1])) AS $(_colname_sql_string(col[2]))"

function _from_string(file, duckdb_func::Nothing; kwargs...)
    isempty(kwargs) || throw(ArgumentError("""
    A specific file type was not selected, but additional keyword arguments were passed.
    These arguments should only be used when a specific file type is selected as they are passed to the DuckDB `read_*` function.
    """))
    kwarg_val_to_db_incomma(file)
end
function _from_string(file, duckdb_func::String; kwargs...)
    @assert !any(isuppercase, duckdb_func)
    "$duckdb_func($(kwarg_val_to_db_incomma(file)) $(kwargs_to_db_comma(kwargs)))"
end

_limit_string(limit::Nothing) = ""
_limit_string(limit::Integer) = "limit $limit"


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
    if has_metadata && get(kwargs, :format, nothing) === :parquet
        return (;KV_METADATA=DataAPI.metadata(tbl))
    end
    return (;)
end

function _read_metadata!(table, file; duckdb_func)
    if DataAPI.metadatasupport(typeof(table)).write && duckdb_func == "read_parquet"
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
