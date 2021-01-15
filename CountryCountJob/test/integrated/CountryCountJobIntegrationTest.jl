module CountryCountJobIntegrationTest

using CountryCountJob 
using Test
using Arrow
using DataFrames
using Random
using Dates

@testset "Test Entire Country Count Job" begin
    input_df = DataFrame(
        names = ["sally", "bob"],
        countries = ["australia", "usa"],
        z = [1; missing], id = 'a':'b'
    )
    history_df = DataFrame(
        countries = ["japan", "china"],
        count = [2,1],
        start_dt_utc = [
            DateTime(2021, 01, 15, 14, 00, 11),
            DateTime(2021, 01, 15, 14, 00, 11),
        ]
    )
    tmp_input_arrow_file = joinpath(pwd(), "20210115_150051_tmp_input_dataframe.arrow")
    tmp_output_arrow_file = joinpath(pwd(), "tmp_output_dataframe.arrow")
    Arrow.write(tmp_input_arrow_file, input_df)
    Arrow.write(tmp_output_arrow_file, history_df)

    main(tmp_input_arrow_file, tmp_output_arrow_file)
    expected_df = DataFrame(
        countries = ["japan", "china", "australia", "usa"],
        count = [2,1,1,1],
        start_dt_utc = [
            DateTime(2021, 01, 15, 14, 00, 11),
            DateTime(2021, 01, 15, 14, 00, 11),
            DateTime(2021, 01, 15, 15, 00, 51),
            DateTime(2021, 01, 15, 15, 00, 51),
        ])
    @test read_arrow_file("new" * tmp_output_arrow_file) == expected_df
end
    
end