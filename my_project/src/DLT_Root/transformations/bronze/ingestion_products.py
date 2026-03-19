
import dlt

##### ==============================================================================================
# OBJECTIVE: Below we are ingesting Products data from source.
##### ==============================================================================================

# Dictionary of Rules to define the EXPECTATIONS:
products_expectations = {
    'rule_1': 'product_id IS NOT NULL',
    'rule_2': 'product_name IS NOT NULL',
    'rule_3': 'price >= 0 AND price IS NOT NULL'
}

# ingesting products data
@dlt.table(
    name = "products_stg"
)

# Declaring to drop the rules violating records and continue the pipeline.
@dlt.expect_all_or_drop(products_expectations)
def products_stg():
    df = spark.readStream.table("my_dlt.source.products")
    return df