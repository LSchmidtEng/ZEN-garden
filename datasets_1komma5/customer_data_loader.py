import polars as pl

######### Customer Data Loader #########

#Paths to csv files
path = "~/Google Drive/My Drive/05 Heartbeat Consumer Product/Market_Intelligence_Data_Sharing/MI_Data_Sweden/Customers_in_ToU/bq-results-20250306-151034-1741273929375.csv"

#Get Schema
# Read the CSV once to get the columns
temp_df = pl.read_csv(path, n_rows=1,ignore_errors=True)
columns = temp_df.columns
# Default dtype and specific overrides
default_dtype = pl.Float64
overrides = {
    "system_id": pl.Utf8,
    "hour": pl.Utf8
}
# Build the schema: apply default and then overwrite where needed
schema = {col: overrides.get(col, default_dtype) for col in columns}


#Reading the CSV file
data = pl.read_csv(path,separator=","
,schema_overrides=schema)
# Convert "hour" column to datetime
data = data.with_columns(
    pl.col("hour").str.strip_suffix(" UTC").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("hour")
)

#Filter out household with data for less than 95% of all hours
# Count rows per system_id
id_counts = data.group_by("system_id").count()
# Filter for IDs with counts >= 8322
valid_ids = id_counts.filter(pl.col("count") >= 8760)["system_id"]
# Filter original DataFrame to only include those valid IDs
filtered_data = data.filter(pl.col("system_id").is_in(valid_ids))
#fill grid_supply_w with 0
filtered_data = filtered_data.with_columns(pl.col("grid_supply_w").fill_null(0))

#choose customers with data for all hours
customer = ["01ddf940-72f8-47b7-be3a-132c6739dcda"]

#Filter out the chosen customers
filtered_data = filtered_data.filter(pl.col("system_id").is_in(customer))

#fill all missing values with 0
filtered_data = filtered_data.fill_null(0)

#export only household_consumption column to csv;
demand_data = filtered_data.sort("hour")
demand_data = demand_data.select(["hour", "household_consumption_w"]).rename({"household_consumption_w": "DE","hour":"time"})
index = pl.Series("time", range(len(demand_data)))  # Rename index to 'hour' to replace it
demand_data = demand_data.with_columns(index)
demand_data.write_csv("household_debug/set_carriers/electricity/demand.csv")

#export ratio of pv production to max pv production to csv
pv_data = filtered_data.sort("hour")
pv_data = pv_data.select(["hour", "pv_production_w"]).rename({"pv_production_w": "DE","hour":"time"})
#get max value of pv_production_w
max_pv = pv_data["DE"].max()
#calculate ratio
pv_data = pv_data.with_columns(pl.col("DE") / max_pv)
index = pl.Series("time", range(len(pv_data)))  # Rename index to 'hour' to replace it
pv_data = pv_data.with_columns(index)
pv_data.write_csv("household_debug/set_technologies/set_conversion_technologies/photovoltaics/max_load.csv")


########## Price Data Loader ##########
#load spot price
path_spot = "~/Google Drive/My Drive/05 Heartbeat Consumer Product/Market_Intelligence_Data_Sharing/MI_Data_Sweden/Customers_in_ToU/spot_se4_2024.csv"
schema= {
    "hour":pl.Utf8,
     "spot_price": pl.Float64
}
spot_data = pl.read_csv(path_spot,separator=","
,schema_overrides=schema)
# Convert "hour" column to datetime
spot_data = spot_data.with_columns(
    pl.col("hour").str.to_datetime("%Y-%m-%dT%H:%M%z").alias("hour")
)
#Convert spot price to Euro/kWh
spot_data = spot_data.with_columns(pl.col("spot_price")*1/1000)

#rename columns
spot_data = spot_data.rename({"spot_price": "DE","hour":"time"})

#export spot price to csv
spot_data = spot_data.sort("time")
index = pl.Series("time", range(len(spot_data)))  # Rename index to 'hour' to replace it
spot_data = spot_data.with_columns(index)
spot_data.write_csv("household_debug/set_carriers/electricity/price_import.csv")

