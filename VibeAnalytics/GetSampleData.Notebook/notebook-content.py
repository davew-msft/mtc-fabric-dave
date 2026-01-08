# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7bd5bf37-b719-4bc8-81a8-1f88fd0b2046",
# META       "default_lakehouse_name": "VibeAnalytics",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "7bd5bf37-b719-4bc8-81a8-1f88fd0b2046"
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

# This is my prompt below:
# 
# ```
# using the code below, can you download the zip file and place it in the Files area of my lakehouse
# 
# curl -L -o ~/Downloads/brazilian-ecommerce.zip
# https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce
# ```

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# This will download the zip file from Kaggle and place it into the Files area of your Lakehouse.
import os

# Download directory within the Lakehouse Files area
destination_dir = '/lakehouse/default/Files/'
os.makedirs(destination_dir, exist_ok=True)

# Download the file to the specified destination
zip_url = 'https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce'
destination_path = os.path.join(destination_dir, 'brazilian-ecommerce.zip')

# Use requests to download since curl and wget may not be available in this runtime
import requests

response = requests.get(zip_url, allow_redirects=True)
with open(destination_path, 'wb') as f:
    f.write(response.content)

print(f"Downloaded to: {destination_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This is my prompt:
# `can you unzip that file and place the contents in a folder under the Files section of my lakehouse, and call it "ecommerce"`

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

import os
import zipfile

# Define paths
zip_path = '/lakehouse/default/Files/brazilian-ecommerce.zip'
extract_dir = '/lakehouse/default/Files/ecommerce'

# Create the output directory if it doesn't exist
os.makedirs(extract_dir, exist_ok=True)

# Extract the zip file into the ecommerce folder
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

print(f"Extraction complete! Files are available in: {extract_dir}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Prompt:  `Now load each of those csvs as Tables.  Do your best guess as to datatypes and column names.  I have no idea if those files have headers or not`

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

import os

# Directory containing the extracted files
csv_dir = '/lakehouse/default/Files/ecommerce'

# List all csv files in the directory
csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

for csv_file in csv_files:
    file_path = os.path.join("Files/ecommerce", csv_file)
    print (file_path)
    # Remove file extension and non-alphanumeric chars for table name
    table_name = os.path.splitext(csv_file)[0].replace("-", "_").replace(" ", "_")
    
    # Try to load with header.
    try:
        spark_df = spark.read.format("csv").option("header", "true").load(file_path)
        # Save as table
        spark_df.write.format("delta").saveAsTable(table_name)
    except Exception as e:
        print(f"Error loading/saving {csv_file}: {e}")

print("All CSVs loaded as tables! If you want to overwrite, rerun with .mode('overwrite') in the write statement.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
