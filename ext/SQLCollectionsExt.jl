module SQLCollectionsExt

using QuackIO
using QuackIO: kwarg_val_to_db_incomma, kwargs_to_db_comma, DBInterface, DuckDB,
    columntable, _selectstring, duckdb_filetype_func
using SQLCollections: SQLCollection, FunSQL as F

function QuackIO.read_file(::Type{SQLCollection}, file, filetype::Symbol; kwargs...)
    duckdb_func = duckdb_filetype_func(filetype)
    qstr = _selectstring(file, duckdb_func; kwargs...)
    @debug("running query string:", qstr)
    conn = DuckDB.DB()
    colnames = DBInterface.execute(conn, qstr) |> columntable |> keys |> collect
	ffunc = getproperty(F.Fun, "$duckdb_func(? $(kwargs_to_db_comma(kwargs)))")
    SQLCollection(conn, F.From(ffunc(F.Lit(file)); columns=colnames))
end

# XXX: should upstream
function F.serialize!(val::AbstractVector{<:AbstractString}, ctx)
    print(ctx, '[')
    for (i, v) in enumerate(val)
        i == 1 || print(ctx, ", ")
        F.serialize!(v, ctx)
    end
    print(ctx, ']')
end

end
