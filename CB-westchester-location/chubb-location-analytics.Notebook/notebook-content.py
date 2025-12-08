# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "88770eba-1f80-4a8e-852c-a59ba5c61b3c",
# META       "default_lakehouse_name": "cb",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "88770eba-1f80-4a8e-852c-a59ba5c61b3c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## This is the RP with Blair and Jamie
# 
# * I was given [this file](abfss://99328097-99f9-47f7-bb76-63191a1432c5@onelake.dfs.fabric.microsoft.com/88770eba-1f80-4a8e-852c-a59ba5c61b3c/Files/LER - MIIA - 2023-06-21-16-08.xlsx)
#   * xlsx in a truly obnoxious format
#   * I removed a lot of columns from the right
#   * these can be added back in but col names "repeat" so it's a PITA right now
# 
# * [ArcGIS GeoAnalytics](https://blog.fabric.microsoft.com/en-us/blog/arcgis-geoanalytics-for-microsoft-fabric-spark-generally-available?ft=All)
# * [Documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-arcgis-geoanalytics?tabs=python)
# 
# * See this workspace:  https://app.fabric.microsoft.com/groups/d8438639-43c6-42cf-a897-73b46b046e08/synapsenotebooks/d46e29eb-e053-4c6e-be2c-c6beb1a2ac14?experience=fabric-developer
# 
# 
# Sent on the teams chat.  Alternatives:
# 
# Assuming yes, we'll need a few things on the chubb side and one of you (I suppose) will need to figure out how to make that happen
# a fabric workspace 
# their fabric tenant admin to enable the ArcGIS for the given capacity
# The steps to do the auth (I'll figure that out)
# They'll need to purchase all of this.  This has some of the documentation.  I have other links too.  I'm not 100% sure if all of this is a non-starter or if we can get chubb some kind of time-bombed license for now.  It is a marketplace offering:  https://learn.microsoft.com/en-us/fabric/data-engineering/spark-arcgis-geoanalytics?tabs=python#licensing-and-cost
# Before I get too in the weeds with this I want to make sure this is feasible
#  
#  Use straight PBI maps.  I'm not a PBI person so I'll rely on one of you to help me out.  There's a Map and FilledMap visual.  I can't figure them out but looks like I could do something around TIVs by PostalCode.  We could put this in a PowerApp or whatever which might be easier.  
# stick with other spark visualizations in the notebooks that are open source.  geopandas might be an option.  I'm not sure how we'd embed this into their existing app/Power App.  
# azure maps.  Easy to embed with js.  There's an azure maps visual in PBI already but I don't know how to use PBI, so again, need assistance
# 


# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/LER.csv")
# df now is a Spark DataFrame containing CSV data from "Files/LER.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import GeoAnalytics for Fabric

import geoanalytics_fabric as GAE
import geoanalytics_fabric.sql.functions as ST
import pyspark.sql.functions as F

print(f"GeoAnalytics for Fabric version: {GAE.version()} loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import geoanalytics_fabric
geoanalytics_fabric.auth()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import geoanalytics_fabric
from geoanalytics_fabric.sql import functions as ST
spark.sql("show user functions like 'ST_*'").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import geoanalytics_fabric
df = spark.read.format("feature-service").load("https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/USA_Census_Counties/FeatureServer/0")
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from geoanalytics_fabric.tools import FindHotSpots

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import the Find Hot Spots tool
from geoanalytics_fabric.tools import FindHotSpots

# Use Find Hot Spots to evaluate the data using bins of 0.1 mile size, and compare to a neighborhood of 0.5 mile around each bin
result_service_calls = FindHotSpots() \
            .setBins(bin_size=0.1, bin_size_unit="Miles") \
            .setNeighborhood(distance=0.5, distance_unit="Miles") \
            .run(dataframe=df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TIV calc

df_points_agg.st.plot (basemap="light",
    figsize=(10,10),
    cmap_values="TIV_SUM_mil",
    vmax=500,
    legend=True,
    legend_kwds={"location":"bottom, "pad":0},
    extent=(-127.83,21.88561,-64.81539,50.69))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
