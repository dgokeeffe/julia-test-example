module CountryCountJobUnitTest

using CountryCountJob 
using Test
using DataFrames
using Random
using Dates


@testset "Parse Filename Datetime" begin
    test_filename = "20210115_140011_new_investors.arrow"
    @test parse_filename_datetime(test_filename) == DateTime(2021, 01, 15, 14, 00, 11) 
end

@testset "Count Countries" begin
    input_df = DataFrame(
        countries = ["australia", "australia", "japan", "china", "china", "fiji"],
    )
    output_df = DataFrame(countries = ["australia", "japan", "china", "fiji"], count = [2,1,2,1])
    @test count_countries(input_df) == output_df
end

end