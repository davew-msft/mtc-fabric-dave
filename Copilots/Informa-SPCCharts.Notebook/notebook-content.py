# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8972a9ea-a943-4df8-b782-074ab1d9e236",
# META       "default_lakehouse_name": "informa",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "8972a9ea-a943-4df8-b782-074ab1d9e236"
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

# CELL ********************

# Load dbo.informa-spc-data into a DataFrame
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("InformaSPCData").getOrCreate()

# Load the table into a Spark DataFrame with back-quotes
spark_df = spark.read.table("`informa-spc-data`")

# Optionally, set a reasonable limit for rows to read
max_rows_to_read = 1000
spark_df = spark_df.limit(max_rows_to_read)

# Display the Spark DataFrame
display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
## prompt:  based on spark_df, can you build me a SPC chart for the BATCH_NUMBER column, the number to plot is NUMERIC_PARAMETER_VALUE.  The UCL and the LCL values should be 2 stddevs from the mean.  
import plotly.graph_objects as go

df = spark_df.select("BATCH_NUMBER", "NUMERIC_PARAMETER_VALUE").toPandas()
df = df.dropna(subset=["BATCH_NUMBER", "NUMERIC_PARAMETER_VALUE"])
df = df.sort_values("BATCH_NUMBER")
means = df.groupby("BATCH_NUMBER")["NUMERIC_PARAMETER_VALUE"].mean().reset_index()

center_line = means["NUMERIC_PARAMETER_VALUE"].mean()
std_dev = means["NUMERIC_PARAMETER_VALUE"].std()
ucl = center_line + 2*std_dev
lcl = center_line - 2*std_dev

fig = go.Figure()
fig.add_trace(go.Scatter(x=means["BATCH_NUMBER"], y=means["NUMERIC_PARAMETER_VALUE"], mode='lines+markers', name="Mean Value"))
fig.add_trace(go.Scatter(x=means["BATCH_NUMBER"], y=[center_line]*len(means), mode='lines', name="Center Line (Mean)", line=dict(dash='dash')))
fig.add_trace(go.Scatter(x=means["BATCH_NUMBER"], y=[ucl]*len(means), mode='lines', name="UCL (+2σ)", line=dict(dash='dot', color='red')))
fig.add_trace(go.Scatter(x=means["BATCH_NUMBER"], y=[lcl]*len(means), mode='lines', name="LCL (-2σ)", line=dict(dash='dot', color='red')))

fig.update_layout(
    title="SPC Chart for NUMERIC_PARAMETER_VALUE by BATCH_NUMBER (UCL/LCL = ±2σ)",
    xaxis_title="BATCH_NUMBER",
    yaxis_title="NUMERIC_PARAMETER_VALUE",
    template='plotly_dark'
)
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import plotly.express as px

# Convert Spark DataFrame to Pandas DataFrame for Plotly
pandas_df = spark_df.select("BATCH_NUMBER", "NUMERIC_PARAMETER_VALUE").toPandas()

# Generate SPC Chart
fig = px.scatter(pandas_df, x="BATCH_NUMBER", y="NUMERIC_PARAMETER_VALUE", title="SPC Chart: Batch Number vs. Numeric Parameter Value")
fig.update_layout(template='plotly_dark')

# Add control limits (optional)
mean_val = pandas_df["NUMERIC_PARAMETER_VALUE"].mean()
std_dev = pandas_df["NUMERIC_PARAMETER_VALUE"].std()
upper_control_limit = mean_val + 3 * std_dev
lower_control_limit = mean_val - 3 * std_dev

fig.add_hline(y=upper_control_limit, line_dash="dash", line_color="red", annotation_text="UCL", annotation_position="top right")
fig.add_hline(y=mean_val, line_dash="dash", line_color="green", annotation_text="Mean", annotation_position="top right")
fig.add_hline(y=lower_control_limit, line_dash="dash", line_color="red", annotation_text="LCL", annotation_position="bottom right")

# Show figure
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import plotly.express as px

# Using df, build an SPC chart with specified control limits
fig = px.scatter(pandas_df, x="BATCH_NUMBER", y="NUMERIC_PARAMETER_VALUE", title="SPC Chart: Batch Number vs. Numeric Parameter Value")
fig.update_layout(template='plotly_dark')

# Add specified control limits
upper_control_limit = 300
lower_control_limit = 10

fig.add_hline(y=upper_control_limit, line_dash="dash", line_color="red", annotation_text="UCL", annotation_position="top right")
fig.add_hline(y=lower_control_limit, line_dash="dash", line_color="red", annotation_text="LCL", annotation_position="bottom right")

# Show figure
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import plotly.express as px

# Using df, build an SPC chart with UCL and LCL as 1 standard deviation from the mean
mean_val = pandas_df["NUMERIC_PARAMETER_VALUE"].mean()
std_dev = pandas_df["NUMERIC_PARAMETER_VALUE"].std()
upper_control_limit = mean_val + std_dev
lower_control_limit = mean_val - std_dev

fig = px.scatter(pandas_df, x="BATCH_NUMBER", y="NUMERIC_PARAMETER_VALUE", title="SPC Chart: Batch Number vs. Numeric Parameter Value")
fig.update_layout(template='plotly_dark')

# Add calculated control limits
fig.add_hline(y=upper_control_limit, line_dash="dash", line_color="red", annotation_text="UCL", annotation_position="top right")
fig.add_hline(y=mean_val, line_dash="dash", line_color="green", annotation_text="Mean", annotation_position="top right")
fig.add_hline(y=lower_control_limit, line_dash="dash", line_color="red", annotation_text="LCL", annotation_position="bottom right")

# Show figure
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

can you build me a widget for the following:  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from ipywidgets import VBox, Text, Dropdown, Button
from IPython.display import display
import pandas as pd

# Define widget elements
first_name_text = Text(description='First Name:')
last_name_text = Text(description='Last Name:')
city_dropdown = Dropdown(description='City:', options=['Collegeville', 'Philly'])
save_button = Button(description='Save')

# Define the container for widgets
widget_box = VBox([first_name_text, last_name_text, city_dropdown, save_button])

# Initialize an empty DataFrame
dfTest = pd.DataFrame(columns=["First Name", "Last Name", "City"])

# Define the save function
def save_data(b):
    global dfTest
    new_entry = {
        "First Name": first_name_text.value,
        "Last Name": last_name_text.value,
        "City": city_dropdown.value
    }
    dfTest = pd.concat([dfTest, pd.DataFrame([new_entry])], ignore_index=True)
    first_name_text.value = ''
    last_name_text.value = ''
    print(dfTest)

# Add the save function to the button
save_button.on_click(save_data)

# Display the widget
display(widget_box)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
