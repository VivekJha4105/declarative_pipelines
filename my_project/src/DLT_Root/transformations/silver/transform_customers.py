
import dlt
import pyspark.sql.functions as F

# Creating a VIEW to perform transformations on Customers Data from Bronze Layer.
# Transformed data is fed to Silver layer UPSERT table.
# Transformed data is also fed to Gold Layer.

@dlt.view(
    name = "customers_enr_view"
)
def customers_enr_view():
    df = spark.readStream.table("customers_stg")
    df = df.withColumn("customer_name", F.upper(F.col("customer_name")))
    return df


# Creating destination Silver Table, which would be an UPSERTED table:
dlt.create_streaming_table(
    name = "customers_enr",
    comment = "Enriched Customers Data"
)

dlt.create_auto_cdc_flow(
    target = "customers_enr",
    source = "customers_enr_view",
    keys = ["customer_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None
)