import dlt

@dlt.table
def FactStream():
    df = spark.readStream.table("spotify_cat.silver.factstream")
    return df

dlt.create_streaming_table("factstream_stream")

dlt.create_auto_cdc_flow(
  target = "factstream_stream",
  source = "FactStream",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 1,
  track_history_column_list = None,
  name = None,
  once = False
)