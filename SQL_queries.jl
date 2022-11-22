using LibPQ, DataFrames, Dates, JLD2, CSV


conn = "CONNECT TO SQL DATABASE"


# Formating address
function short_address(target_text)
    uppercase(target_text[(end-39):end])
end

function topic_standard(target_topic::String)
    uppercase(target_topic)[(end-63):end]
end

query = "
SELECT 
    e.block_signed_at AS signed_at,
    e.block_height AS block_height,
    e.tx_offset AS tx_offset,
    e.log_offset AS log_offset,
    encode(e.tx_hash, 'hex') AS tx_hash,
    encode(e.topics[1], 'hex') AS identifier,
    encode(e.topics[2], 'hex') AS collateral,
    encode(e.topics[3], 'hex') AS reserve,
    encode(e.topics[4], 'hex') AS user,
    substring(encode(e.data, 'hex') from (1+64*0) for 64) AS purchaseamount,
    substring(encode(e.data, 'hex') from (1+64*1) for 64) AS liquidatedcollateralamount,
    substring(encode(e.data, 'hex') from (1+64*2) for 64) AS accruedborrowinterest,
    substring(encode(e.data, 'hex') from (1+64*3) for 64) AS liquidator,
    substring(encode(e.data, 'hex') from (1+64*4) for 64) AS receiveatoken
FROM public.block_log_events e 
WHERE e.topics @> ARRAY[CAST( '\\x56864757fd5b1fc9f38f5f3a981cd8ae512ce41b902cf73fc506ee369c6bc237'AS bytea)] AND
    e.block_signed_at >= '2022-01-01 00:00:00' AND
    e.block_signed_at < '2022-10-01 00:00:00'
"
@time results=execute(conn,query)
@time df = DataFrame(results)

df.signed_at = DateTime.(df.signed_at)
df.tx_hash = topic_standard.(df.tx_hash)
df.identifier = topic_standard.(df.identifier)
df.collateral = short_address.(df.collateral)
df.reserve = short_address.(df.reserve)
df.user = short_address.(df.user)

df.purchaseamount = parse.(BigInt, df.purchaseamount, base=16)
df.liquidatedcollateralamount = parse.(BigInt, df.liquidatedcollateralamount, base=16)
df.accruedborrowinterest = parse.(BigInt, df.accruedborrowinterest, base=16)
df.liquidator = short_address.(df.liquidator)
df.receiveatoken = parse.(Int8, df.receiveatoken, base=16)


#######################################################################################
############################## Gathering Collateral Data ##############################
#######################################################################################

gdf = groupby(df, [:collateral])

nrow_collateral = combine(gdf, nrow)
nrow_collateral = sort(nrow_collateral, :nrow, rev=true)
nrow_collateral.collateral[1]

Token_dict = Dict(
    "aETH" => ("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE", 18),
    "LINK" => ("514910771AF9CA656AF840DFF83E8264ECF986CA", 18),
    "YFI Token" => ("0BC529C00C6401AEF6D220BE8C6EA1667F6AD93E", 18),
    "WBTC" => ("2260FAC5E5542A773AA44FBCFEDF7C193BC2C599", 8),
    "AAVE Token" => ("7FC66500C84A76AD7E9C93437BFC5AC33E2DDAE9", 18),
    "UNI token" => ("1F9840A85D5AF5BF1D1762F925BDADDC4201F984", 18),
    "MANA Token" => ("0F5D2FB29FB7D3CFEE444A200298F468908CC942", 18))

tokens = []
token_addresses = []
collateral_wad = []
for (k, v) in Token_dict
    push!(tokens, k)
    push!(token_addresses, v[1])
    push!(collateral_wad, v[2])
end

token_df = DataFrame()
token_df.collateral_token_name = tokens
token_df.collateral = token_addresses
token_df.collateral_wad = collateral_wad
token_df

df2 = leftjoin!(df, token_df, on=:collateral)
dropmissing!(df2, :collateral_token_name)
df2 = sort(df2, :signed_at)

########################################################################################
################################ Gathering Reserve Data ################################
########################################################################################

gdf = groupby(df, [:reserve])

nrow_reserve = combine(gdf, nrow)
nrow_reserve = sort(nrow_reserve, :nrow, rev=true)
nrow_reserve.reserve[1]

reserve_dict = Dict(
    "USDC" => ("A0B86991C6218B36C1D19D4A2E9EB0CE3606EB48", 6),
    "USDT" => ("DAC17F958D2EE523A2206206994597C13D831EC7", 6),
    "Dai" => ("6B175474E89094C44DA98B954EEDEAC495271D0F", 18),
    "TrueUSD" => ("0000000000085D4780B73119B644AE5ECD22B376", 18),
    "Binance USD" => ("4FABB145D64652A948D72533023F6E7A623C7C53", 18),
    "aETH" => ("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE", 18),
    "Proxy sUSD Token" => ("57AB1EC28D129707052DF4DF418D58A2D46D5F51", 18),
    "MANA Token" => ("0F5D2FB29FB7D3CFEE444A200298F468908CC942", 18),
    "LINK Token" => ("514910771AF9CA656AF840DFF83E8264ECF986CA", 18),
    "WBTC" => ("2260FAC5E5542A773AA44FBCFEDF7C193BC2C599", 8),
    "Proxy SNX Token" => ("C011A73EE8576FB46F5E1C5751CA3B9FE0AF2A6F", 18))

tokens = []
token_addresses = []
reserve_wad = []
for (k, v) in reserve_dict
    push!(tokens, k)
    push!(token_addresses, v[1])
    push!(reserve_wad, v[2])
end

Reserve_df = DataFrame()
Reserve_df.Reserve_token_name = tokens
Reserve_df.reserve = token_addresses
Reserve_df.reserve_wad = reserve_wad
Reserve_df

df2 = leftjoin!(df2, Reserve_df, on=:reserve)
dropmissing!(df2, :Reserve_token_name)

df2


########################################################################################
################################### Merging WAD Data ###################################
########################################################################################
df3 = df2[:, [:signed_at, :tx_hash, :block_height, :tx_offset, :log_offset, :user, :purchaseamount, :liquidatedcollateralamount, :liquidator, :collateral_token_name, :collateral_wad, :Reserve_token_name, :reserve_wad]]
df3.reserve_wad = 10 .^ (df3.reserve_wad)
df3.collateral_wad = 10 .^ (df3.collateral_wad)
df3.liquidIn = ./(df3.purchaseamount, df3.reserve_wad)
df3.RealCollateral = ./(df3.liquidatedcollateralamount, df3.collateral_wad)

jldopen("Collateral_data.jld2", "w") do file
    file["df3"] = df3
end;

df2.tx_hash[8]
