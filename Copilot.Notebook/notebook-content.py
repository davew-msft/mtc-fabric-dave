# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d21695b1-aec9-4ecc-9471-c056ff071f9d",
# META       "default_lakehouse_name": "davelake",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5"
# META     }
# META   }
# META }

# CELL ********************

# start spark session 
print('starting session, consider adjusting the session timeout')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# #### Run the cell below to install the required packages for Copilot
# 
# (this cell will be automatically added to the top of any notebook where you enable the copilot button)


# CELL ********************

# None of this should be needed anymore given improvements to Fabric Copilot
#Run this cell to install the required packages for Copilot
%pip install https://aka.ms/chat-magics-0.0.0-py3-none-any.whl
%load_ext chat_magics


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fabric Copilot Notes
# 
# * [How to Setup Fabric Copilot](https://blog.fabric.microsoft.com/en-US/blog/fabric-change-the-game-how-easy-is-it-to-use-copilot-in-microsoft-fabric/)
# * [How to ENABLE Fabric Copilot](https://blog.fabric.microsoft.com/en-US/blog/how-to-enable-copilot-in-fabric-for-everyone-2/): you must be a tenant admin
# * [Here is some additional information if you are having problems with the setup](https://www.serverlesssql.com/using-copilot-in-fabric-notebooks-for-data-engineering/)
# 
# 1. Open a new Notebook and ensure you've added a lakehouse on the left
# 2. Copilot should be available as a button on the top right of the navbar.  Click it.  You should get a little explanatory window on the right side.  
# 3. It should add a new cell with the necessary `chat_magics` setup process.  Run that code.  [Link to information on Chat-Magics](https://learn.microsoft.com/en-us/fabric/get-started/copilot-notebooks-chat-magics)


# CELL ********************

%chat_magics
# This shows the help screen, but may not be needed anymore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This is a cell-level `chat-magic`.  Simply use natural language to chat with the data.  

# CELL ********************

# MAGIC %%chat
# MAGIC What variables are currently defined?  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%chat
# MAGIC # in the copilot pane or here...
# MAGIC schema for davelake.dbo.masterentity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# `%%code` is used to have Copilot actually write code for you. You can also do this with the `Copilot` window that should be open on the right side of the screen. 

# CELL ********************

# MAGIC %%code
# MAGIC In the bronze/theaters folder, Load Theatres.csv into a spark dataframe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Load the CSV into a Spark DataFrame
# file_path = '/lakehouse/default/Files/bronze/theaters/Theatres.csv'

# Note that it gets the file_path wrong above.  We had to fix it.  
file_path = 'Files/bronze/theaters/Theatres.csv'

spark_df = spark.read.format("csv").option("header", "true").load(file_path)

# Display the Spark DataFrame (optional)
spark_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Other commands available:
# * `%describe df`
# * %%add_comments
# * %%fix_errors
# * %pin df
# * %new_task

# CELL ********************

%describe spark_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Let's look at a little demo.  I have a file of `London theaters` and we can do analytics against it.  
# 
# 
# [London Cultural Infrastructure Map](https://data.london.gov.uk/dataset/cultural-infrastructure-map)  
# [Direct link to theaters.csv](https://data.london.gov.uk/download/cultural-infrastructure-map/c9bda99b-2156-4ecd-b09d-c207ae5947e2/Theatres.csv)  
# 
# You will need to download the csv file and upload it somewhere to your lakehouse.  
# 
# **Click the copilot option to "load my data from my lakehouse into a dataframe"** and type `Load Theatres.csv into a DataFrame`

# CELL ********************

# try the next option:
# Check the first few rows of the DataFrame using the head() function.
# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# you should get some other interesting options to aid in Exploratory
# Data Analytics (EDA)

# calculate summary statistics for numerical columns
spark_df.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Let's try something more advanced.  
# 
# Try this:
# 
# `Analyze Theatres.csv and provide me with insights about the data`
# 
# ...but...we can use the data already in the Spark context, so instead of doing this in the chat window...do it in a new call with a chat magic.  

# CELL ********************

# MAGIC %%code
# MAGIC 
# MAGIC Analyze the spark_df dataframe and tell me some insights about the data.  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Display the schema of the Spark DataFrame
spark_df.printSchema()

# Show first few rows of the dataframe
spark_df.show()

# Summary statistics for numerical columns
spark_df.describe().show()

# Count the number of rows in the dataframe
row_count = spark_df.count()
print(f"Number of rows: {row_count}")

# Count the number of distinct values in 'borough_name' column
distinct_boroughs = spark_df.select('borough_name').distinct().count()
print(f"Number of distinct borough names: {distinct_boroughs}")

# Group by 'borough_name' and count occurrences
borough_count = spark_df.groupBy('borough_name').count()
borough_count.show()

# Show the distribution of mean runtime
#spark_df.groupBy().mean('runtime').show()

# Show the distribution of mean latitude and longitude
#spark_df.groupBy().mean('latitude', 'longitude').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# If we instead used `%%chat` instead of `%%code` it would have more explanatory text and the generated code cell would not be present.  
# 
# That's it!

# CELL ********************

# MAGIC %%code
# MAGIC Which ward has the most theaters (a ward is defined as the column called ward_2018_name?  
# MAGIC print that value.  
# MAGIC What is the mean?  
# MAGIC What is the standard deviation?  
# MAGIC Can you plot those?

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt

# Count the number of theaters in each ward
ward_theater_counts = result['ward_2018_name'].value_counts()

# Find the ward with the most theaters
ward_with_most_theaters = ward_theater_counts.idxmax()

# Calculate the mean and standard deviation of theater counts
mean_theater_count = ward_theater_counts.mean()
std_theater_count = ward_theater_counts.std()

# Plot the theater counts by ward
plt.figure(figsize=(12, 6))
ward_theater_counts.plot(kind='bar')
plt.title('Number of Theaters by Ward')
plt.xlabel('Ward')
plt.ylabel('Number of Theaters')
plt.show()

ward_with_most_theaters, mean_theater_count, std_theater_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%code
# MAGIC Can you make this a normal distribution?  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df["English"] = df["transcript"].ai.translate("english")

df.ai.classify("blah","blahblah")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# 

