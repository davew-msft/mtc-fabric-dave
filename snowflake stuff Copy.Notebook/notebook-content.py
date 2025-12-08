# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a779b29a-887a-4750-b5e3-f443872aadc1",
# META       "default_lakehouse_name": "snowflake",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "a779b29a-887a-4750-b5e3-f443872aadc1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# consider changing the session timeout

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Imports and whatnot
from pyspark.sql.types import *
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf()

# Enable Arrow-based spark configuration
conf.set("spark.sql.execution.arrow.enabled", "true")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# helper functions
def RunSnowflake (qry):
    """ 
        runs a snowflake sql call and returns a spark df

    """
    # Execute query
    cursor = conn.cursor()
    cursor.execute(qry)

    # Fetch result into a pandas DataFrame
    pdf = cursor.fetch_pandas_all()

    # Close cursor
    cursor.close()
    #len(pdf)
    df = spark.createDataFrame(pdf)
    return df



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sample call
query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TPCH_SF100'"
df = RunSnowflake(query)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ways to connect to Snowflake

# CELL ********************

# !spark-shell --packages net.snowflake:snowflake-jdbc:3.12.17,net.snowflake:spark-snowflake_2.12:2.8.4-spark_3.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install snowflake-connector-python

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import snowflake.connector
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conn = snowflake.connector.connect(
    user='dwentzel',
    password='rqpYbge1B47j4(',
    account='LDWNHFB-TJA33218',
    database='SNOWFLAKE_SAMPLE_DATA',
    # warehouse='',
    # schema=''
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER"
query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TPCH_SF100'"
# query = "SELECT DISTINCT TABLE_TYPE FROM SNOWFLAKE_SAMPLE_DATA.INFORMATION_SChEMA.TABLES"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# can this handle datatypes.  Fabric pipelines are struggling with this?  
# need to check this works with snowflake views tho.  
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# can we get the schema for a given table?
#query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TPCH_SF100'"
#query = "SHOW COLUMNS IN VIEW SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.SUPPLIER"
query = """
select 
        ordinal_position as position,
       column_name,
       data_type,
       case when character_maximum_length is not null
            then character_maximum_length
            else numeric_precision end as max_length
from information_schema.columns
where table_schema ilike 'TPCH_SF100' 
       and table_name ilike 'SUPPLIER'  
order by ordinal_position;

"""

# Execute query
cursor = conn.cursor()
cursor.execute(query)

# Fetch result into a pandas DataFrame
pdf = cursor.fetch_pandas_all()

# Close cursor
cursor.close()



len(pdf)
df = spark.createDataFrame(pdf)
display (df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# constraints
query = """

select *
from  information_schema.TABLE_CONSTRAINTS 
--where table_name = 'SUPPLIER' 
where table_name = 'PROMOTION'
and constraint_type = 'PRIMARY KEY';


"""

# select 
#         ordinal_position as position,
#        column_name,
#        data_type,
#        case when character_maximum_length is not null
#             then character_maximum_length
#             else numeric_precision end as max_length
# from information_schema.columns
# where table_schema ilike 'TPCH_SF100' 
#        and table_name ilike 'SUPPLIER'  
# order by ordinal_position;

# Execute query
cursor = conn.cursor()
cursor.execute(query)

# Fetch result into a pandas DataFrame
pdf = cursor.fetch_pandas_all()

# Close cursor
cursor.close()



len(pdf)
df = spark.createDataFrame(pdf)
display (df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS RAFFT

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# pull one table, preserve the datatypes, write it to a table
query = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER"

# Execute query
cursor = conn.cursor()
cursor.execute(query)

# Fetch result into a pandas DataFrame
pdf = cursor.fetch_pandas_all()

# Close cursor
cursor.close()



len(pdf)
df = spark.createDataFrame(pdf)
display (df)
df.printSchema()

# Save the DataFrame to a Lakehouse table
df.write.mode("overwrite").format("delta").saveAsTable(f"RAFFT.CALL_CENTER")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE TABLE RAFFT.call_center

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## metadata needed for loading multiple tables

# CELL ********************

# Define schema
defDate = datetime.strptime('1980-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

schema = StructType([
    StructField("ID", IntegerType(), nullable=False),
    StructField("TableName", StringType(), nullable=False),
    StructField("Enabled", BooleanType(), nullable=True),
    StructField("LastLoadDate", TimestampType(), nullable=True),
    StructField("LoadDuration", IntegerType(), nullable=True),
    #ErrorMessage TODO
])

# Create data
data = [
    (1, "Value1", True, defDate, 0),
    (2, "Value1", True, defDate, 0),
    (3, "Value1", True, defDate, 0)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display DataFrame
display (df)
df.write.mode("overwrite").format("delta").saveAsTable(f"RAFFT.metadata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE RAFFT.metadata
# MAGIC (
# MAGIC     ID INT NOT NULL,
# MAGIC     TableName VARCHAR(200) NOT NULL,
# MAGIC     Enabled BOOLEAN NOT NULL,
# MAGIC     LastLoadDate TIMESTAMP NULL,
# MAGIC     LoadDuration INT NULL
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# if schema has evolved, set alert


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
