
import dlt

##### ==============================================================================================
# OBJECTIVE: Below we are ingesting sales data from two different source tables.
# Instead of creating two different tables, we can combine them into one streaming table, which is
###  initially empty untill data flows to it.
# Using the dlt.append_flow() function, we can append the data from both tables into the same table.
# This is a great way to combine data from
##### ==============================================================================================

# Defining EXPECTATIONS for streaming table with a FLOW:
sales_expectations = {
    'rule_1': 'sales_id IS NOT NULL'
}

# Empty Streaming Table
dlt.create_streaming_table(
    name = "sales_stg",
    expect_all_or_drop = sales_expectations
)


# Creating sales_east Table Data FLOW targeting the Empty Streaming Table:
@dlt.append_flow(target="sales_stg")
def sales_east():
    df = spark.readStream.table("my_dlt.source.sales_east")
    return df

# Creating sales_west Table Data FLOW targeting the Empty Streaming Table:
@dlt.append_flow(target="sales_stg")
def sales_west():
    df = spark.readStream.table("my_dlt.source.sales_west")
    return df