module CountryCountJob 
using DataFrames
using Arrow
using Statistics
using Dates

export main, count_countries, parse_filename_datetime, read_arrow_file

function read_arrow_file(filename::String)
    return DataFrame(Arrow.Table(filename))
end

function parse_filename_datetime(filename::String)
    file_datetime_regex = r"([12]\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])_(?:[0-6]{6}))"
    m = match(file_datetime_regex, filename)
    file_datetime = m.match
    return DateTime("yyyymmdd_HHMMSS", file_datetime)
end

function write_arrow_file(df::DataFrame, filename::String)
    Arrow.write(filename, df)
end


function count_countries(df::DataFrame)
    return combine(groupby(df, :countries), nrow => :count)
end


function multiply_net_worth(df::DataFrame, multiply::Int64)
    return transform(df, :net_worth => (x -> x .* multiply) => :multiplied_net_worth)
end


function main(input_filename::String, output_filename::String, multiply::Int64)
    df = read_arrow_file(input_filename)
    count_countries_df = count_countries(df)
    write_arrow_file(count_countries_df, output_filename)
end

end