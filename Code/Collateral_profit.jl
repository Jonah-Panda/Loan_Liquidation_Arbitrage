using JLD2, DataFrames, LibPQ, Dates
using CSV
@time df = load_object("/home/jonah.pandarinath/Lending_Project/Collateral_Exploration/Collateral_data.jld2")

conn = "CONNECT TO SQL DATABASE"

function short_address(target_text)
    uppercase(target_text[(end-39):end])
end

function cancel_pairs(block_df::DataFrame)
    gdf = groupby(block_df, :wad)
    _nrow = combine(gdf, nrow)
    _nrow = sort(_nrow, :nrow)
    block_df = leftjoin!(block_df, _nrow, on=:wad)
    block_df = subset(block_df, :nrow => ByRow(==(1)))
    block_df = block_df[:, begin:end-1]
    return block_df
end

function calc_profit(clean_df::DataFrame, purchaseamount, liquidatedcollateralamount, collateral_token, reserve_token)
    profit = missing
    profit_currency = missing

    if collateral_token == reserve_token
        profit = liquidatedcollateralamount-purchaseamount
        profit_currency = "PA"
    end

    if size(clean_df)[1] == 2
        wad1 = clean_df.wad[1]
        wad2 = clean_df.wad[2]
        if wad1 > wad2
            profit = wad1-wad2
            profit_currency = "ETH"
        else
            profit = wad2-wad1
            profit_currency = "ETH"
        end
    end
    if size(clean_df)[1] == 1
        profit = clean_df.wad[1]
        # println("here1")
        # wad = clean_df.wad[1]
        # calc1 = purchaseamount-wad
        # calc2 = liquidatedcollateralamount-wad
        # if calc1 < 0
        #     calc1 = 10^25
        # end
        # if calc2 < 0
        #     calc2 = 10^25
        # end
        # println(calc1)
        # println(calc2)
        # if calc1 < calc2
        #     profit = calc1
        #     profit_currency = "PA"
        # elseif calc2 < calc1
        #     profit = calc2
        #     profit_currency = "LCA"
        # else
        #     profit = missing
        #     profit_currency = missing
        # end
    end

    if size(clean_df)[1] > 3
        profit = missing
        profit_currency = missing
    end

    return (size(clean_df)[1], profit, profit_currency)
end

function get_input_data(block_height::Int32, tx_offset::Int32)
    query = "
    SELECT 
        e.block_signed_at AS signed_at,
        e.block_height AS block_height,
        e.tx_offset AS tx_offset,
        e.log_offset AS log_offset,
        encode(e.topics[2], 'hex') AS collateral,
        encode(e.topics[3], 'hex') AS reserve,
        substring(encode(e.data, 'hex') from (1+64*0) for 64) AS purchaseamount,
        substring(encode(e.data, 'hex') from (1+64*1) for 64) AS liquidatedcollateralamount,
        substring(encode(e.data, 'hex') from (1+64*3) for 64) AS liquidator
    FROM public.block_log_events e 
    WHERE e.block_height = "
    dynamic_string = string(block_height, " AND
        e.tx_offset = ", tx_offset, " AND
        e.topics @> ARRAY[CAST( '\\x56864757fd5b1fc9f38f5f3a981cd8ae512ce41b902cf73fc506ee369c6bc237'AS bytea)]
    ")
    query = string(query, dynamic_string)
    results=execute(conn, query)
    liquidation_log = DataFrame(results)
    purchaseamount = parse(BigInt, liquidation_log.purchaseamount[1], base=16)
    liquidatedcollateralamount = parse(BigInt, liquidation_log.liquidatedcollateralamount[1], base=16)
    liquidator = short_address(liquidation_log.liquidator[1])
    collateral_token = short_address(liquidation_log.collateral[1])
    reserve_token = short_address(liquidation_log.reserve[1])


    query = "
    SELECT
        e.block_height AS block_height,
        e.tx_offset AS tx_offset, 
        encode(e.topics[2], 'hex') AS topic1,
        encode(e.topics[3], 'hex') AS topic2,
        substring(encode(e.data, 'hex') from (1+64*0) for 64) AS wad
    FROM public.block_log_events e
    WHERE e.block_height = "
    dynamic_string = string(block_height, " AND
        e.tx_offset = ", tx_offset, " AND
        e.topics @> ARRAY[CAST( '\\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'AS bytea)]
    ")
    query = string(query, dynamic_string)
    results=execute(conn, query)
    df_block = DataFrame(results)
    df_block.topic1 = short_address.(df_block.topic1)
    df_block.topic2 = short_address.(df_block.topic2)
    df_block.wad = parse.(BigInt, df_block.wad, base=16)

    filter = []
    i = 0
    while i < size(df_block)[1]
        i += 1
        if df_block.topic1[i] == liquidator || df_block.topic2[i] == liquidator
            if string(df_block.wad[i])[begin:begin+3] == string(purchaseamount)[begin:begin+3] || string(df_block.wad[i])[begin:begin+3] == string(liquidatedcollateralamount)[begin:begin+3]
                push!(filter, missing)
            else
                #if df_block.wad[i] > purchaseamount
                    push!(filter, "Keep")
                #else
                #    push!(filter, missing)
                #end
            end
        else
            push!(filter, missing)
        end
    end
    # print(filter)
    df_block.filter = filter
    dropmissing!(df_block)
    df_block = df_block[:, begin:end-1]

    if size(df_block)[1] > 1
        df_block = cancel_pairs(df_block)
    end
    data_tuple = calc_profit(df_block, purchaseamount, liquidatedcollateralamount, collateral_token, reserve_token)
    return data_tuple
end


gdf = groupby(df, :liquidator)
LL_df = gdf[2]
LL_df.liquidator[1]

row = 2
tx_hash = LL_df.tx_hash[row]
block_height = convert(Int32, LL_df.block_height[row])
tx_offset = convert(Int32, LL_df.tx_offset[row])
data_tuple = get_input_data(block_height, tx_offset)


transfer_size = []
profit = []
currency_amount = []

i = 0
while i < size(LL_df)[1]
    i += 1

    # df.tx_hash[5]
    block_height = convert(Int32, LL_df.block_height[i])
    tx_offset = convert(Int32, LL_df.tx_offset[i])

    data_tuple = get_input_data(block_height, tx_offset)
    push!(transfer_size, data_tuple[1])
    push!(profit, data_tuple[2])
    push!(currency_amount, data_tuple[3])
end

LL_df.transfer_size = transfer_size
LL_df.profit = profit
LL_df.currency_amount = currency_amount
LL_df

CSV.write("Liquidator_1.csv", LL_df)

print(df_size)



# # Getting list of columns all datafields in database
# query = "SELECT *
# FROM information_schema.columns
# WHERE
#     table_schema = 'public'
# "
# @time results=execute(conn,query)
# @time db_columns = DataFrame(results)