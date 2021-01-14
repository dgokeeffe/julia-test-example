module CountryCountJobIntegrationTest

using CountryCountJob 
using Test
using Arrow
using DataFrames
using Random

df = DataFrame(
    names = ["sally", "bob", "jill", "judy", "sam", "dave"],
    countries = ["australia", "australia", "japan", "china", "china", "fiji"],
    net_worth = repeat(1:2, 3), holdings = rand(Float64, 6), y = 4:9, z = [3:7; missing], id = 'a':'f'
)
tmp_input_arrow_file = joinpath(pwd(), "tmp_input_dataframe.arrow")
tmp_output_arrow_file = joinpath(pwd(), "tmp_output_dataframe.arrow")
Arrow.write(tmp_input_arrow_file, df)

@testset "Count Countries" begin
    main(tmp_input_arrow_file, tmp_output_arrow_file, 20)
    expected_df = DataFrame(countries = ["australia", "japan", "china", "fiji"], count = [2,1,2,1])
    @test read_arrow_file(tmp_output_arrow_file) == expected_df
end
    
end