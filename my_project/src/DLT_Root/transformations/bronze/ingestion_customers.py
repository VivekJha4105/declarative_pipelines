

import dlt

##### ==============================================================================================
# OBJECTIVE: Below we are ingesting Customers data from source.
##### ==============================================================================================

# Dictionary of Rules Available to define the EXPECTATIONS:
customers_expectations = {
    'rule_1': 'customer_id IS NOT NULL',
    'rule_2': 'customer_name IS NOT NULL'
}

# ingesting customers data
@dlt.table(
    name = "customers_stg"
)

# Declaring to drop the rules violating records and continue the pipeline.
@dlt.expect_all_or_drop(customers_expectations)

def customers_stg():
    df = spark.readStream.table("my_dlt.source.customers")
    return df