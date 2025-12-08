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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Copilots
# 
# * Copilot button on the left
# * Dataflow
#   * New Item
#   * `Get data from the lakehouse called copilot, file is called Sales.csv`
#   * `the first row is the header`
#   * `describe this query`
#   * `Add a column 'Gross Revenue' that is a product of 'UnitPrice' and 'OrderQty', the result is rounded to two decimal places.`
#   * `new col "Discount Value" which is Gross Revenue * UnitPiceDiscount`
#   * new col DaysToShip which is the difference of OrderDate and ShipDate
#   * I want to Create a new query that shows only 'StoreKey' and 'StoreName' and keep unique values.  Call this dimStore
#   
#   
#   
#   


# MARKDOWN ********************

# ### Spark Notebooks
# 
# * products.csv to a dataframe


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Products.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
