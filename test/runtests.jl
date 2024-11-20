using TestItems
using TestItemRunner
@run_package_tests


@testitem "io" begin
    using Tables: rowtable, columntable
    using StructArrays

    tbl = (a=[1,2], b=["x", "yz"], c=[1.,missing])
    
    write_table("test.csv", tbl)
    @test readlines("test.csv") == ["a,b,c", "1,x,1.0", "2,yz,"]

    @test isequal(read_csv(columntable, "test.csv"), tbl)
    @test isequal(read_csv(rowtable, "test.csv"), [(a=1, b="x", c=1.0), (a=2, b="yz", c=missing)])
    @test isequal(read_csv(StructArray, "test.csv")::StructArray, [(a=1, b="x", c=1.0), (a=2, b="yz", c=missing)])

    write_table("test.csv", tbl; format=:csv)
    @test readlines("test.csv") == ["a,b,c", "1,x,1.0", "2,yz,"]
    @test isequal(read_csv(columntable, "test.csv"), tbl)
    @test isequal(read_csv(columntable, "test.csv"; delim=";"), (var"a,b,c"=["1,x,1.0", "2,yz,"],))
    @test isequal(read_csv(columntable, "test.csv"; delim=","), tbl)
    
    write_table("test.csv", tbl; format="csv", delim=";")
    @test readlines("test.csv") == ["a;b;c", "1;x;1.0", "2;yz;"]
    
    write_table("test.csv.gz", tbl)
    run(`gunzip --force test.csv.gz`)
    @test readlines("test.csv") == ["a,b,c", "1,x,1.0", "2,yz,"]

    write_table("test.pq", tbl; format=:parquet)
    @test 300 < filesize("test.pq") < 500
    @test String(read("test.pq", 4)) == "PAR1"
    @test isequal(read_parquet(columntable, "test.pq"), tbl)
end

@testitem "_" begin
    import Aqua
    Aqua.test_all(QuackIO; ambiguities=false)
    Aqua.test_ambiguities(QuackIO)

    import CompatHelperLocal as CHL
    CHL.@check()
end
