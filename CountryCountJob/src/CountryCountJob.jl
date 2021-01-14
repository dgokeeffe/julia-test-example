module CountryCountJob 
using DataFrames
using Arrow
using Statistics

export main, normalize, count_countries, multiply_net_worth, read_arrow_file

function read_arrow_file(filename::String)
    return DataFrame(Arrow.Table(filename))
end


function write_arrow_file(df::DataFrame, filename::String)
    Arrow.write(filename, df)
end


function main(input_filename::String, output_filename::String, multiply::Int64)
    df = read_arrow_file(input_filename)
    net_worth_df = multiply_net_worth(df, multiply)
    write_arrow_file(net_worth_df, output_filename)
end


function count_countries(df::DataFrame)
    return combine(groupby(df, :countries), nrow => :count)
end


function multiply_net_worth(df::DataFrame, multiply::Int64)
    return transform(df, :net_worth => (x -> x .* multiply) => :multiplied_net_worth)
end

end