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

# MARKDOWN ********************

# load informa-spc-data into a datafram

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Using Spark SQL to load dbo.informa-spc-data into a DataFrame
table_name = "informa-spc-data"
df = spark.sql(f"SELECT * FROM `{table_name}`")

# Show the schema of the dataframe to verify
df.printSchema()

# Display the first few rows of the dataframe
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# using df, build an spc chart.  
# 
# x axis = batch_number
# y axis = numeric_parameter_value

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
import plotly.express as px

# Convert column data types to string for proper plotting if necessary
df = df.withColumn("BATCH_NUMBER", df["BATCH_NUMBER"].cast("string"))

# Convert to Pandas DataFrame for plotting with Plotly
pandas_df = df.toPandas()

# Create an SPC chart
fig = px.scatter(pandas_df, x="BATCH_NUMBER", y="NUMERIC_PARAMETER_VALUE", title="SPC Chart: Numeric Parameter Value by Batch Number", template='plotly_dark')
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Convert column data types to string for proper plotting if necessary
df = df.withColumn("BATCH_NUMBER", df["BATCH_NUMBER"].cast("string"))

# Convert to Pandas DataFrame for plotting with Plotly
pandas_df = df.toPandas()

# Calculate mean of numeric_parameter_value
mean_value = pandas_df['NUMERIC_PARAMETER_VALUE'].mean()

# Create an SPC chart
fig = px.scatter(pandas_df, x="BATCH_NUMBER", y="NUMERIC_PARAMETER_VALUE", 
                 title="SPC Chart: Numeric Parameter Value by Batch Number",
                 template='plotly_dark')

# Add mean line in blue
fig.add_trace(go.Scatter(x=pandas_df['BATCH_NUMBER'], y=[mean_value]*len(pandas_df), 
                         mode='lines', name='Mean', line=dict(color='blue')))

# Add UCL line
fig.add_trace(go.Scatter(x=pandas_df['BATCH_NUMBER'], y=[150]*len(pandas_df), 
                         mode='lines', name='UCL', line=dict(color='red', dash='dash')))

# Add LCL line
fig.add_trace(go.Scatter(x=pandas_df['BATCH_NUMBER'], y=[20]*len(pandas_df), 
                         mode='lines', name='LCL', line=dict(color='red', dash='dash')))

fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Convert column data types to string for proper plotting if necessary
df = df.withColumn("BATCH_NUMBER", df["BATCH_NUMBER"].cast("string"))

# Convert to Pandas DataFrame for plotting with Plotly
pandas_df = df.toPandas()

# Calculate mean and standard deviation of numeric_parameter_value
mean_value = pandas_df['NUMERIC_PARAMETER_VALUE'].mean()
std_dev = pandas_df['NUMERIC_PARAMETER_VALUE'].std()

# Calculate UCL and LCL
UCL = mean_value + std_dev
LCL = mean_value - std_dev

# Create an SPC chart
fig = px.scatter(pandas_df, x="BATCH_NUMBER", y="NUMERIC_PARAMETER_VALUE", 
                 title="SPC Chart: Numeric Parameter Value by Batch Number",
                 template='plotly_dark')

# Add mean line in blue
fig.add_trace(go.Scatter(x=pandas_df['BATCH_NUMBER'], y=[mean_value]*len(pandas_df), 
                         mode='lines', name='Mean', line=dict(color='blue')))

# Add UCL line
fig.add_trace(go.Scatter(x=pandas_df['BATCH_NUMBER'], y=[UCL]*len(pandas_df), 
                         mode='lines', name='UCL', line=dict(color='red', dash='dash')))

# Add LCL line
fig.add_trace(go.Scatter(x=pandas_df['BATCH_NUMBER'], y=[LCL]*len(pandas_df), 
                         mode='lines', name='LCL', line=dict(color='red', dash='dash')))

fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# Example of interactive spark notebooks...could this be used as the input forms?


# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
import ipywidgets as widgets
from IPython.display import display
import pandas as pd

# Create widgets for user input
first_name_widget = widgets.Text(description="First Name:")
last_name_widget = widgets.Text(description="Last Name:")
location_widget = widgets.Dropdown(options=["Collegeville", "Malvern"], description="Location:")

# Display the widgets
display(first_name_widget)
display(last_name_widget)
display(location_widget)

# Button to save data
button = widgets.Button(description="Save Data")
output = widgets.Output()

def save_data(button):
    with output:
        # Capture the data from widgets
        data = {
            "First Name": [first_name_widget.value],
            "Last Name": [last_name_widget.value],
            "Location": [location_widget.value]
        }
        global df
        df = pd.DataFrame(data)
        print(df)

button.on_click(save_data)
display(button, output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
