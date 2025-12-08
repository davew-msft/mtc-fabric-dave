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

df_person = spark.sql("SELECT * FROM dbo.Person")
df_products = spark.sql("SELECT * FROM dbo.Products")
df_sales = spark.sql("SELECT * FROM dbo.Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, udf, to_date, lit

df_sales_details = df_sales.join(df_products, "ProductID", "inner").select(
    col("PersonID"),
    col("ProductID"),
    col("Model"),
    col("ProductCategoryName"),
    col("Region"),
)


df_cust_details = (
    df_sales_details.join(df_person, "PersonID", "inner")
    .drop("AddressLine2", "Title")
    .withColumnRenamed("AddressLine1", "Address")
    .withColumnRenamed("ProductCategoryName", "ProductCategory")
    .withColumnRenamed("Model", "ProductModel")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
)
import xml.etree.ElementTree as ET


schema = StructType(
    [
        StructField("TotalPurchaseYTD", DecimalType(), True),
        StructField("DateFirstPurchase", StringType(), True),
        StructField("BirthDate", StringType(), True),
        StructField("MaritalStatus", StringType(), True),
        StructField("YearlyIncome", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("TotalChildren", IntegerType(), True),
        StructField("NumberChildrenAtHome", IntegerType(), True),
        StructField("Education", StringType(), True),
        StructField("Occupation", StringType(), True),
        StructField("HomeOwnerFlag", IntegerType(), True),
        StructField("NumberCarsOwned", IntegerType(), True),
        StructField("CommuteDistance", StringType(), True),
    ]
)


def parse_xml(xml_str):
    if xml_str is None:
        return (None,) * 13

    xml_str = xml_str.strip('"').replace('"""', '"')
    xml_str = xml_str.replace('""', '"')

    if not xml_str.startswith("<") or not xml_str.endswith(">"):
        return (None,) * 13

    try:
        root = ET.fromstring(xml_str)
        namespace = {
            "ns": "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"
        }
        return (
            root.findtext("ns:TotalPurchaseYTD", default="", namespaces=namespace),
            root.findtext("ns:DateFirstPurchase", default="", namespaces=namespace),
            root.findtext("ns:BirthDate", default="", namespaces=namespace),
            root.findtext("ns:MaritalStatus", default="", namespaces=namespace),
            root.findtext("ns:YearlyIncome", default="", namespaces=namespace),
            root.findtext("ns:Gender", default="", namespaces=namespace),
            int(root.findtext("ns:TotalChildren", default=0, namespaces=namespace)),
            int(
                root.findtext(
                    "ns:NumberChildrenAtHome", default=0, namespaces=namespace
                )
            ),
            root.findtext("ns:Education", default="", namespaces=namespace),
            root.findtext("ns:Occupation", default="", namespaces=namespace),
            int(root.findtext("ns:HomeOwnerFlag", default=0, namespaces=namespace)),
            int(root.findtext("ns:NumberCarsOwned", default=0, namespaces=namespace)),
            root.findtext("ns:CommuteDistance", default="", namespaces=namespace),
        )
    except Exception:
        raise ValueError(f"Unable to parse XML: {xml_str}")


parse_xml_udf = udf(parse_xml, schema)

df_cust_details = df_cust_details.withColumn(
    "expanded", parse_xml_udf(col("Demographics"))
)

for field in schema.fields:
    df_cust_details = df_cust_details.withColumn(
        field.name, col("expanded." + field.name)
    )

df_cust_details = df_cust_details \
    .withColumn("BirthDate", to_date(col("BirthDate").substr(1, 10), "yyyy-MM-dd")) \
    .withColumn("DateFirstPurchase", to_date(col("DateFirstPurchase").substr(1, 10), "yyyy-MM-dd")) \

df_cust_details = df_cust_details.select(
    col("FirstName"),
    col("LastName"),
    col("Address"),
    col("DateFirstPurchase"),
    col("BirthDate"),
    col("MaritalStatus"),
    col("YearlyIncome"),
    col("Gender"),
    col("TotalChildren"),
    col("NumberChildrenAtHome"),
    col("Education"),
    col("Occupation"),
    col("HomeOwnerFlag"),
    col("NumberCarsOwned"),
    col("CommuteDistance"),
    col("TotalPurchaseYTD"),
    col("ProductModel"),
    col("ProductCategory"),
)

df_cust_details = df_cust_details.where(df_cust_details["YearlyIncome"] != "")

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
