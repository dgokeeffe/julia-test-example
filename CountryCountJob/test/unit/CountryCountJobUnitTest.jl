module CountryCountJobUnitTest

using CountryCountJob 
using Test
using DataFrames
using Random

@testset "Multiply Net Worth" begin
    input_df = DataFrame(net_worth = repeat(1:2, 3))
    output_df = DataFrame(net_worth = repeat(1:2, 3), multiplied_net_worth = repeat([10,20], 3))
    @test multiply_net_worth(input_df, 10) == output_df
end

@testset "Count Countries" begin
    input_df = DataFrame(
        countries = ["australia", "australia", "japan", "china", "china", "fiji"],
    )
    output_df = DataFrame(countries = ["australia", "japan", "china", "fiji"], count = [2,1,2,1])
    @test count_countries(input_df) == output_df
end

end