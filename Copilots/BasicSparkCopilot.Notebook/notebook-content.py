# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2db08d21-75e1-458e-b215-b39cffbbba8f",
# META       "default_lakehouse_name": "copilot",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "2db08d21-75e1-458e-b215-b39cffbbba8f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Importing the Products.csv file into a Spark DataFrame

file_path = 'Files/Products.csv'
spark_df = spark.read.format("csv").option("header", "true").load(file_path)

# Display the DataFrame to verify the contents
display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate a new DataFrame with distinct rows

distinct_spark_df = spark_df.distinct()

# Display the distinct DataFrame to verify the contents
display(distinct_spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert ProductID to a number, multiply it by 10, and save the result back to the ProductID column

from pyspark.sql.functions import col

# Assuming 'ProductID' is of string type, first convert it to integer, multiply by 10, and save back.
spark_df = spark_df.withColumn("ProductID", col("ProductID").cast("int") * 10)

# Display the updated DataFrame to verify the changes
display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
