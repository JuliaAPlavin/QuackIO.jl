module SQLCollectionsExt

using QuackIO
using QuackIO: DBInterface, DuckDB, columntable, kwarg_val_to_db_incomma, kwargs_to_db_comma, _select_string, _from_string
using SQLCollections: SQLCollection, FunSQL as F

function QuackIO._read_file(::Type{SQLCollection}, file, duckdb_func; select=nothing, limit=nothing, kwargs...)
    isnothing(select) && isnothing(limit) || throw(ArgumentError("reading into an SQLCollection does not support `select` or `limit` arguments"))
    qstr = "select * from $(_from_string(file, duckdb_func; kwargs...)) limit 0"
    @debug "$duckdb_func query" qstr
	conn = DuckDB.DB()
    colnames = DBInterface.execute(conn, qstr) |> columntable |> keys |> collect
	ffunc = isnothing(duckdb_func) ? F.Lit(file) :
            getproperty(F.Fun, "$duckdb_func(? $(kwargs_to_db_comma(kwargs)))")(F.Lit(file))
	SQLCollection(conn, F.From(ffunc; columns=colnames))
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
