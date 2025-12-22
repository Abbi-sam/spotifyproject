import dlt

@dlt.table
def DimDate():
    df = spark.readStream.table("spotify_cat.silver.dimdate")
    return df

dlt.create_streaming_table("dimdate_stream")

dlt.create_auto_cdc_flow(
  target = "dimdate_stream",
  source = "DimDate",
  keys = ["date_key"],
  sequence_by = "date",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  name = None,
  once = False
)