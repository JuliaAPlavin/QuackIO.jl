using TestItems
using TestItemRunner
@run_package_tests


@testitem "_" begin
    import Aqua
    Aqua.test_all(QuackIO; ambiguities=false)
    Aqua.test_ambiguities(QuackIO)

    import CompatHelperLocal as CHL
    CHL.@check()
end
