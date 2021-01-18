module CountryCountJob 
using DataFrames
using Arrow
using Statistics
using Dates

export main, count_countries, parse_filename_datetime, add_start_datetime_column, read_arrow_file

function read_arrow_file(filename::String)
    return DataFrame(Arrow.Table(filename))
end


function write_arrow_file(df::DataFrame, filename::String)
    show(df)
    Arrow.write(filename, df)
end


function parse_filename_datetime(filename::String)
    file_datetime_regex = r"([12]\d{3}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])_(?:[0-6]{6}))"
    m = match(file_datetime_regex, filename)
    file_datetime = m.match
    return DateTime(file_datetime, "yyyymmdd_HHMMSS") 
end


function count_countries(df::DataFrame)
    return combine(groupby(df, :countries), nrow => :count)
end


function add_start_datetime_column(df::DataFrame, start_datetime::DateTime)
    insertcols!(df, 3, :start_dt_utc => repeat([start_datetime], nrow(df)))
end


function main(input_filename::String, output_filename::String)
    new_investrs_df = read_arrow_file(input_filename)
    investor_country_count_hist_df = read_arrow_file(output_filename)
    @info "Our Input DataFrame \n"
    show(new_investrs_df)
    @info "Our Historical DataFrame \n"
    show(investor_country_count_hist_df)
    start_datetime = parse_filename_datetime(input_filename)
    new_investor_count_countries_df = count_countries(new_investrs_df)
    add_start_datetime_column(new_investor_count_countries_df, start_datetime)
    @info "Country Count DataFrame \n"
    show(new_investor_count_countries_df)
    @info "New investor dataframe appended to the historical dataframe \n"
    write_arrow_file(
        vcat(investor_country_count_hist_df, new_investor_count_countries_df),
        output_filename 
    )
end

end
