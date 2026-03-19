
import dlt
import pyspark.sql.functions as F

# Creating a VIEW to perform transformations on Sales Data from Bronze Layer.
# Transformed data is fed to Silver layer UPSERT table.
# Transformed data is also fed to Gold Layer.

@dlt.view(
    name = "sales_enr_view"
)

def sales_enr_view():
    df = spark.readStream.table("sales_stg")
    df = df.withColumn("total_amount", F.col("quantity") * F.col("amount"))
    return df

# Creating destination Silver Table, which would be an UPSERTED table:
dlt.create_streaming_table(
    name = "sales_enr",
    comment = "Enriched Sales Data"
)

dlt.create_auto_cdc_flow(
    target = "sales_enr",
    source = "sales_enr_view",
    keys = ["sales_id"], # keys by which bronze and silver layer records are mapped
    sequence_by = "sale_timestamp", # Column by which bronze and silver layer records are sequenced
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None
)
