import dlt

@dlt.table
def DimTrack():
    df = spark.readStream.table("spotify_cat.silver.dimtrack")
    return df

dlt.create_streaming_table("dimtrack_stream")

dlt.create_auto_cdc_flow(
  target = "dimtrack_stream",
  source = "DimTrack",
  keys = ["track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  name = None,
  once = False
)