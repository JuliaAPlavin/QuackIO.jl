using TestItems
using TestItemRunner
@run_package_tests


@testitem "basic io" begin
    using Tables
    using StructArrays

    tbl = (a=[1,2], b=["x", "yz"], c=[1.,missing])
    csvfname = tempname() * ".csv"
    pqfname = tempname() * ".pq"
    
    write_table(csvfname, tbl)
    @test readlines(csvfname) == ["a,b,c", "1,x,1.0", "2,yz,"]

    @test isequal(read_csv(columntable, csvfname), tbl)
    @test isequal(read_csv(rowtable, csvfname), [(a=1, b="x", c=1.0), (a=2, b="yz", c=missing)])
    @test isequal(read_csv(StructArray, csvfname)::StructArray, [(a=1, b="x", c=1.0), (a=2, b="yz", c=missing)])

    write_table(csvfname, tbl; format=:csv)
    @test readlines(csvfname) == ["a,b,c", "1,x,1.0", "2,yz,"]
    @test isequal(read_csv(columntable, csvfname), tbl)
    @test isequal(read_csv(columntable, csvfname; delim=";"), (var"a,b,c"=["1,x,1.0", "2,yz,"],))
    @test isequal(read_csv(columntable, csvfname; delim=","), tbl)
    
    write_table(csvfname, tbl; format="csv", delim=";")
    @test readlines(csvfname) == ["a;b;c", "1;x;1.0", "2;yz;"]
    
    write_table(csvfname * ".gz", tbl)
    run(`gunzip --force $(csvfname * ".gz")`)
    @test readlines(csvfname) == ["a,b,c", "1,x,1.0", "2,yz,"]

    write_table(pqfname, tbl; format=:parquet)
    @test 300 < filesize(pqfname) < 500
    @test String(read(pqfname, 4)) == "PAR1"
    @test isequal(read_parquet(columntable, pqfname), tbl)
end

@testitem "different parameters" begin
    # using Logging; ConsoleLogger(stdout, Logging.Debug) |> global_logger
    using Tables

    tbl = (a=[1,2], b=["x", "yz"], c=[1.,missing])
    fname = tempname() * " \\ ''' abc '' def ' .csv"

    write_table(fname, tbl)
    @test isfile(fname)
    @test isequal(read_csv(columntable, fname), tbl)

    write_table(fname, tbl; header=false, use_tmp_file=false)
    @test readlines(fname) == ["1,x,1.0", "2,yz,"]

    write_table(fname, tbl; force_quote=["a", "c"], use_tmp_file=false)
    @test readlines(fname) == ["a,b,c", "\"1\",x,\"1.0\"", "\"2\",yz,"]
    write_table(fname, tbl; force_quote=[:a, :c], use_tmp_file=false)
    @test readlines(fname) == ["a,b,c", "\"1\",x,\"1.0\"", "\"2\",yz,"]

    @test isequal(read_csv(columntable, fname; auto_type_candidates=["int", "varchar"]), (a=[1,2], b=["x", "yz"], c=["1.0",missing]))
    @test isequal(read_csv(columntable, fname; columns=(
        a="float",
        b="varchar",
        c="int",
    )), (a=[1.0,2.0], b=["x", "yz"], c=[1,missing]))
    @test isequal(read_csv(columntable, fname; names=["xx", "абв ' \"", "\\ 1"]), (
        xx=[1,2],
        var"абв ' \""=["x", "yz"],
        var"\ 1"=[1.0,missing],
    ))
end

@testitem "_" begin
    import Aqua
    Aqua.test_all(QuackIO; ambiguities=false)
    Aqua.test_ambiguities(QuackIO)

    import CompatHelperLocal as CHL
    CHL.@check()
end
