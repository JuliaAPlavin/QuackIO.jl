using TestItems
using TestItemRunner
@run_package_tests


@testitem "basic io" begin
    using Tables
    using StructArrays

    tbl = (a=[1,2], b=["x", "yz"], c=[1.,missing])
    csvfname = tempname() * ".csv"
    pqfname = tempname() * ".pq"
    jsonname = tempname() * ".json"
    
    write_table(csvfname, tbl)
    @test readlines(csvfname) == ["a,b,c", "1,x,1.0", "2,yz,"]

    @test isequal(read_csv(columntable, csvfname), tbl)
    @test isequal(read_csv(rowtable, csvfname), rowtable(tbl))
    @test isequal(read_csv(StructArray, csvfname)::StructArray, rowtable(tbl))

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

    @test_throws "uppercase" write_table(pqfname, tbl; format=:Parquet)
    @test !isfile(pqfname)
    write_table(pqfname, tbl; format=:parquet)
    @test 300 < filesize(pqfname) < 1000
    @test String(read(pqfname, 4)) == "PAR1"
    @test isequal(read_parquet(columntable, pqfname), tbl)

    write_table(jsonname, tbl; format=:json)
    @test readlines(jsonname) == ["{\"a\":1,\"b\":\"x\",\"c\":1.0}", "{\"a\":2,\"b\":\"yz\",\"c\":null}"]
    @test isequal(read_json(columntable, jsonname), tbl)
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

    @test isequal(read_csv(columntable, [fname, fname]), (
        a=[1,2,1,2],
        b=["x", "yz", "x", "yz"],
        c=[1.0,missing,1.0,missing],
    ))

    @test read_csv(columntable, IOBuffer("a\n1\n2\n")) == (a=[1,2],)
end

@testitem "SQLCollections" begin
    using SQLCollections
    using DataManipulation

    tbl = (a=[1,2], b=["x", "yz"], c=[1.,missing])
    csvfname = tempname() * ".csv"
    write_table(csvfname, tbl)

    sc = read_csv(SQLCollection, csvfname)
    @test sc isa SQLCollection
    @test count(Returns(true), sc) == 2
    @test isequal(collect(sc), [(a=1, b="x", c=1.0), (a=2, b="yz", c=missing)])

    @test (@p let
        sc
        group_vg(@o (;_.b))
        map(@o (b=key(_).b, cnt=length(_)))
        collect
    end) == [(b = "x", cnt = 1), (b = "yz", cnt = 1)]

    sc1 = read_csv(SQLCollection, [csvfname])
    @test sc1 isa SQLCollection
    @test isequal(collect(sc1), collect(sc))

    sc2 = read_csv(SQLCollection, [csvfname, csvfname])
    @test sc2 isa SQLCollection
    @test isequal(collect(sc2), repeat(collect(sc), outer=2))
end

@testitem "read and guess format" begin
    using Tables
    using SQLCollections

    tbl = (a=[1,2], b=["x", "yz"], c=[1.,missing])
    csvfname = tempname() * ".csv"
    write_table(csvfname, tbl)

    @test isequal(QuackIO.read_file(columntable, csvfname), tbl)
    @test isequal(QuackIO.read_file(SQLCollection, csvfname) |> columntable, tbl)
end

@testitem "select columns and limit rows" begin
    using Tables
    using SQLCollections

    tbl = (a=[1,2], b=["x", "yz"], c=[1.,missing])
    csvfname = tempname() * ".csv"
    write_table(csvfname, tbl)

    @test isequal(read_csv(columntable, csvfname, select=(:a, :b)), tbl[(:a, :b)])
    @test isequal(read_csv(columntable, csvfname, select=("a"=>"c", "b"=>"d")), (c=tbl.a, d=tbl.b))
    @test isequal(read_csv(columntable, csvfname, select=("a"=>"c", "b"=>"d"), limit=1), (c=[1], d=["x"]))

    tbl = (;var"a b"=[1,2])
    write_table(csvfname, tbl; format=:csv)
    @test isequal(read_csv(columntable, csvfname, select=("a b"=>"c d",)), (var"c d"=tbl.var"a b",))
end

@testitem "metadata" begin
    using DataFrames: DataFrame
    import DataAPI
    using Tables

    pqfname = tempname() * ".pq"

    # types that support metadata but don't contain it:
    df = DataFrame((a=[1, 2], b=["x", "yz"], c=[1.0, missing]))
    write_table(pqfname, df; format=:parquet)
    ndf = read_parquet(DataFrame, pqfname)
    @test DataAPI.metadata(ndf) == DataAPI.metadata(df)

    # tables that actually contain metadata:
    df = DataFrame((a=[1, 2], b=["x", "yz"], c=[1.0, missing]))
    DataAPI.metadata!(df, "writer", "Quack'IO"; style=:note)  # ' for escaping
    DataAPI.metadata!(df, "1", 2)
    write_table(pqfname, df; format=:parquet, compression=:zstd)
    ndf = read_parquet(DataFrame, pqfname)
    @test DataAPI.metadata(ndf)["1"] == string(DataAPI.metadata(df)["1"])
    @test DataAPI.metadata(ndf)["writer"] == DataAPI.metadata(df)["writer"]

    # metadata is ignored when not supported by table types
    ntbl = read_parquet(columntable, pqfname)
    @test isequal(ntbl, columntable(df))

    # metadata is ignored when not supported by file formats
    csvfname = tempname() * ".csv"
    write_table(pqfname, df; format=:csv)
end

@testitem "_" begin
    import Aqua
    Aqua.test_all(QuackIO; ambiguities=false)
    Aqua.test_ambiguities(QuackIO)

    import CompatHelperLocal as CHL
    CHL.@check(checktest=false)
end
