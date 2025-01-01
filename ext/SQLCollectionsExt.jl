module SQLCollectionsExt

using QuackIO
using QuackIO: kwarg_val_to_db_incomma, kwargs_to_db_comma, DBInterface, DuckDB, columntable
using SQLCollections: SQLCollection, FunSQL as F

function QuackIO._read_file(::Type{SQLCollection}, file, duckdb_func::String; kwargs...)
    qstr = "select * from $duckdb_func($(kwarg_val_to_db_incomma(file)) $(kwargs_to_db_comma(kwargs))) limit 0"
    @debug "$duckdb_func query" qstr
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
