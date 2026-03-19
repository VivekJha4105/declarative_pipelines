
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# Creating a VIEW to perform transformations on Products Data from Bronze Layer.
# Transformed data is fed to Silver layer UPSERT table.
# Transformed data is also fed to Gold Layer.

@dlt.view(
    name = "products_enr_view"
)
def products_enr_view():
    df = spark.readStream.table("products_stg")
    df = df.withColumn("price", F.col("price").cast(IntegerType()))
    return df


# Creating destination Silver Table, which would be an UPSERTED table:
dlt.create_streaming_table(
    name = "products_enr",
    comment = "Enriched Products Table"
)
dlt.create_auto_cdc_flow(
    target = "products_enr",
    source = "products_enr_view",
    keys = ["product_id"],
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