# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f5181b9f-5e33-4e46-ab81-b316b571336e",
# META       "default_lakehouse_name": "adventureworks",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Fabric Data Agent (nee AI Skills)
# 
# * [What is this? (Blog Post)](https://blog.fabric.microsoft.com/en-us/blog/harness-microsoft-fabric-ai-skill-to-unlock-context-rich-insights-from-your-data/)
# * [Background and Setup](https://learn.microsoft.com/en-us/fabric/data-science/data-agent-scenario)
# * [Use this with Copilot Studio and an MCP Server](https://learn.microsoft.com/en-us/microsoft-copilot-studio/agent-extend-action-mcp#microsoft-mcp-connectors-available-in-copilot-studio)
# * [How to do Data Source Instructions](https://blog.fabric.microsoft.com/en-US/blog/new-in-fabric-data-agent-data-source-instructions-for-smarter-more-accurate-ai-responses/)
# * [How to do evaluations](https://learn.microsoft.com/en-us/fabric/data-science/evaluate-data-agent)
# * https://learn.microsoft.com/en-us/fabric/data-science/ai-skill-scenario
# 


# MARKDOWN ********************

# ## Basic Setup Steps...

# CELL ********************

# populate lakehouse from scratch
# this doesn't need to be rerun if it already exists
import pandas as pd
from tqdm.auto import tqdm
base = "https://synapseaisolutionsa.blob.core.windows.net/public/AdventureWorks"

# load list of tables
df_tables = pd.read_csv(f"{base}/adventureworks.csv", names=["table"])

for table in (pbar := tqdm(df_tables['table'].values)):
    pbar.set_description(f"Uploading {table} to lakehouse")

    # download
    df = pd.read_parquet(f"{base}/{table}.parquet")

    # save as lakehouse table
    spark.createDataFrame(df).write.mode('overwrite').saveAsTable(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ...then New Item ...AI Skill
# 
# 
# see `ai_skill`
# 
# 2 problems: 
# * it only looks at one of the fact tables
# * it looks at order quantity and it probably should look at sales revenue
# 
# 
# Add these to the notes
# 
# ```
# Whenever I ask about "the most sold" products or items, the metric of interest is total sales revenue and not order quantity.
# 
# The primary table to use is FactInternetSales. Only use FactResellerSales if explicitly asked about resales or when asked about total sales.
# 
# ```
# 
# ...then add...
# 
# ```
# When asked about the impact of promotions, do so on the increase in sales revenue, not just the number of units sold.
# For customer insights, focus on the total sales amount per customer rather than the number of orders.
# Use DimDate to extract specific time periods (for example, year, month) when performing time-based analysis.
# When analyzing geographical data, prioritize total sales revenue and average sales per order for each region.
# For product category insights, always use DimProductCategory to group products accordingly.
# When comparing sales between regions, use DimSalesTerritory for accurate territory details.
# Use DimCurrency to normalize sales data if analyzing sales in different currencies.
# For detailed product information, always join FactInternetSales with DimProduct.
# Use DimPromotion to analyze the effectiveness of different promotional campaigns.
# For reseller performance, focus on total sales amount and not just the number of products sold.
# When analyzing trends over time, use FactInternetSales and join with DimDate to group data by month, quarter, or year.
# Always check for data consistency by joining FactInternetSales with the corresponding dimension tables.
# Use SUM for aggregating sales data to ensure you're capturing total values accurately.
# Prioritize sales revenue metrics over order quantity, to gauge the financial impact accurately.
# Always group by relevant dimensions (for example, product, customer, date) to get detailed insights.
# When asked about customer demographics, join DimCustomer with relevant fact tables.
# For sales by promotion, join FactInternetSales with DimPromotion and group by promotion name.
# Normalize sales figures using DimCurrency for comparisons involving different currencies.
# Use ORDER BY clauses to sort results by the metric of interest (for example, sales revenue, total orders).
# ListPrice in DimProduct is the suggested selling price, while UnitPrice in FactInternetSales and FactResellerSales is the actual price at which each unit was sold. For most use cases on revenue, the unit price should be used.
# Rank top resellers by sales amount.
# ```


# MARKDOWN ********************

# `How many active customers did we have June 1st, 2013` is ambiguous
# 
# "active customer" doesn't have a formal definition. 
# 
# `How many active customers did we have June 1,2010?`
# 
# ```sql 
# select count(distinct fis.CustomerKey) as ActiveCustomerCount
# FROM factinternetsales fis
# JOIN dimdate dd on fis.OrderDateKey = dd.DateKey
# where dd.FullDateAlternateKey between dateadd(month, -6, '2010-06-01') and '2010-06-01'
# group by fis.CustomerKey
# having count(fis.SalesOrderNumber) >= 2;
# ```
# 
# ...or...upload the json...
# 
# ```json
# {
#     "how many active customers did we have June 1st, 2010?": "SELECT COUNT(DISTINCT fis.CustomerKey) AS ActiveCustomerCount FROM factinternetsales fis JOIN dimdate dd ON fis.OrderDateKey = dd.DateKey WHERE dd.FullDateAlternateKey BETWEEN DATEADD(MONTH, -6, '2010-06-01') AND '2010-06-01' GROUP BY fis.CustomerKey HAVING COUNT(fis.SalesOrderNumber) >= 2;",
#     "which promotion was the most impactful?": "SELECT dp.EnglishPromotionName, SUM(fis.SalesAmount) AS PromotionRevenue FROM factinternetsales fis JOIN dimpromotion dp ON fis.PromotionKey = dp.PromotionKey GROUP BY dp.EnglishPromotionName ORDER BY PromotionRevenue DESC;",
#     "who are the top 5 customers by total sales amount?": "SELECT TOP 5 CONCAT(dc.FirstName, ' ', dc.LastName) AS CustomerName, SUM(fis.SalesAmount) AS TotalSpent FROM factinternetsales fis JOIN dimcustomer dc ON fis.CustomerKey = dc.CustomerKey GROUP BY CONCAT(dc.FirstName, ' ', dc.LastName) ORDER BY TotalSpent DESC;",
#     "what is the total sales amount by year?": "SELECT dd.CalendarYear, SUM(fis.SalesAmount) AS TotalSales FROM factinternetsales fis JOIN dimdate dd ON fis.OrderDateKey = dd.DateKey GROUP BY dd.CalendarYear ORDER BY dd.CalendarYear;",
#     "which product category generated the highest revenue?": "SELECT dpc.EnglishProductCategoryName, SUM(fis.SalesAmount) AS CategoryRevenue FROM factinternetsales fis JOIN dimproduct dp ON fis.ProductKey = dp.ProductKey JOIN dimproductcategory dpc ON dp.ProductSubcategoryKey = dpc.ProductCategoryKey GROUP BY dpc.EnglishProductCategoryName ORDER BY CategoryRevenue DESC;",
#     "what is the average sales amount per order by territory?": "SELECT dst.SalesTerritoryRegion, AVG(fis.SalesAmount) AS AvgOrderValue FROM factinternetsales fis JOIN dimsalesterritory dst ON fis.SalesTerritoryKey = dst.SalesTerritoryKey GROUP BY dst.SalesTerritoryRegion ORDER BY AvgOrderValue DESC;",
#     "what is the total sales amount by currency?": "SELECT dc.CurrencyName, SUM(fis.SalesAmount) AS TotalSales FROM factinternetsales fis JOIN dimcurrency dc ON fis.CurrencyKey = dc.CurrencyKey GROUP BY dc.CurrencyName ORDER BY TotalSales DESC;",
#     "which product had the highest sales revenue last year?": "SELECT dp.EnglishProductName, SUM(fis.SalesAmount) AS TotalRevenue FROM factinternetsales fis JOIN dimproduct dp ON fis.ProductKey = dp.ProductKey JOIN dimdate dd ON fis.ShipDateKey = dd.DateKey WHERE dd.CalendarYear = YEAR(GETDATE()) - 1 GROUP BY dp.EnglishProductName ORDER BY TotalRevenue DESC;",
#     "what are the monthly sales trends for the last year?": "SELECT dd.CalendarYear, dd.MonthNumberOfYear, SUM(fis.SalesAmount) AS TotalSales FROM factinternetsales fis JOIN dimdate dd ON fis.ShipDateKey = dd.DateKey WHERE dd.CalendarYear = YEAR(GETDATE()) - 1 GROUP BY dd.CalendarYear, dd.MonthNumberOfYear ORDER BY dd.CalendarYear, dd.MonthNumberOfYear;",
#     "how did the latest promotion affect sales revenue?": "SELECT dp.EnglishPromotionName, SUM(fis.SalesAmount) AS PromotionRevenue FROM factinternetsales fis JOIN dimpromotion dp ON fis.PromotionKey = dp.PromotionKey WHERE dp.StartDate >= DATEADD(MONTH, 0, GETDATE()) GROUP BY dp.EnglishPromotionName ORDER BY PromotionRevenue DESC;",
#     "which territory had the highest sales revenue?": "SELECT dst.SalesTerritoryRegion, SUM(fis.SalesAmount) AS TotalSales FROM factinternetsales fis JOIN dimsalesterritory dst ON fis.SalesTerritoryKey = dst.SalesTerritoryKey GROUP BY dst.SalesTerritoryRegion ORDER BY TotalSales DESC;",
#     "who are the top 5 resellers by total sales amount?": "SELECT TOP 5 dr.ResellerName, SUM(frs.SalesAmount) AS TotalSales FROM factresellersales frs JOIN dimreseller dr ON frs.ResellerKey = dr.ResellerKey GROUP BY dr.ResellerName ORDER BY TotalSales DESC;",
#     "what is the total sales amount by customer region?": "SELECT dg.EnglishCountryRegionName, SUM(fis.SalesAmount) AS TotalSales FROM factinternetsales fis JOIN dimcustomer dc ON fis.CustomerKey = dc.CustomerKey JOIN dimgeography dg ON dc.GeographyKey = dg.GeographyKey GROUP BY dg.EnglishCountryRegionName ORDER BY TotalSales DESC;",
#     "which product category had the highest average sales price?": "SELECT dpc.EnglishProductCategoryName, AVG(fis.UnitPrice) AS AvgPrice FROM factinternetsales fis JOIN dimproduct dp ON fis.ProductKey = dp.ProductKey JOIN dimproductcategory dpc ON dp.ProductSubcategoryKey = dpc.ProductCategoryKey GROUP BY dpc.EnglishProductCategoryName ORDER BY AvgPrice DESC;"
# }
# ```
# 
# 
# ...then publish...
# 
# https://api.fabric.microsoft.com/v1/workspaces/99328097-99f9-47f7-bb76-63191a1432c5/aiskills/b1192229-2cfd-4733-9017-addbc5cb9eb0/query/deployment


# MARKDOWN ********************

# ## Quick Test

# CELL ********************

import requests
import json
import pprint
from synapse.ml.mlflow import get_mlflow_env_config


# the URL could change if the workspace is assigned to a different capacity
url = "https://api.fabric.microsoft.com/v1/workspaces/99328097-99f9-47f7-bb76-63191a1432c5/aiskills/b1192229-2cfd-4733-9017-addbc5cb9eb0/query/deployment"

configs = get_mlflow_env_config()

headers = {
    "Authorization": f"Bearer {configs.driver_aad_token}",
    "Content-Type": "application/json; charset=utf-8"
}

question = "{userQuestion: \"who are the top 5 customers by total sales amount?\"}"

response = requests.post(url, headers=headers, data = question)

print("RESPONSE: ", response)

print("")

response = json.loads(response.content)

print(response["result"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generating Additional Queries
# Sometimes you need to know what related questions may be.
# 
# By rephrasing the original question in different ways, you can gather more comprehensive information and provide a deeper understanding of the topic at hand.
# 
# Below is a sample prompt that demonstrates how to generate multiple relevant questions based on your initial question. In the following function, you will see how to call a specific LLM model from OpenAI. The parameters, deployment ID, messages, temperature, and seed are part of the experimental setup.
# 
# In this case, the prompt and question are passed directly into the messages, the temperature is set to 0, the seed is fixed, and the deployment ID is set to one of the supported models.

# CELL ********************

prompt_extract_questions = """You are a helpful analyst. You are given a user query, using the query create 3 relevant questions that will give more information around the *question being asked*. 
For example: If the question is, what is the top selling product in 2019? The rephrased questions could be: 
what were the top selling 3 products in 2019? what were the top 3 products sold in 2018? or what were the top 3 best-selling products across all years? 
Note that these questions will be independently used to query a SQL table.  Don't enumerate the questions and put each question in a new line.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import openai

# Given a user question, generates 3 more questions that will be used to obtain more context about the original question
def openAI_service_multiple_questions(query):
    response = openai.ChatCompletion.create(
        deployment_id='gpt-4-32k',
        messages=[
            {"role": "system", "content": prompt_extract_questions},
            {"role": "user", "content": query},
        ],
        temperature=0,
        seed=40,
    )
    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = "What is the top selling product in 2013?"

response = openAI_service_multiple_questions(query)
answer = response.choices[0].message.content
print(f"{answer}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Call the AI Skill Function
# 
# https://blog.fabric.microsoft.com/en-us/blog/harness-microsoft-fabric-ai-skill-to-unlock-context-rich-insights-from-your-data/

# CELL ********************

context = 
question = ""
aiskill_url = "https://api.fabric.microsoft.com/v1/workspaces/99328097-99f9-47f7-bb76-63191a1432c5/aiskills/b1192229-2cfd-4733-9017-addbc5cb9eb0/query/deployment"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from synapse.ml.mlflow import get_mlflow_env_config
from tenacity import retry, stop_after_attempt
import requests
import json

@retry(stop=stop_after_attempt(3))  # Retry up to 3 times
def aiSkill(context, question, aiskill_url):

    configs = get_mlflow_env_config()   
    headers = {
        "Authorization": f"Bearer {configs.driver_aad_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    text = {
        'userQuestion': question, 
        'modelBehavior' : {
            'enableBlockAdditionalContextByLength': False,
        },
        'additionalContext': context # notes to the model
    }

    response = requests.post(aiskill_url, headers=headers, data = json.dumps(text))

    response = json.loads(response.content)

    # If the AISkill didn't generate a good SQL query, it will throw an error
    if "ResultRows" not in response.keys():
        raise ValueError(response["message"])

    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notes = """ \
- When answering about a product, make sure to include the Product Name in dimproduct in the answer. 
- Best selling product should be determined by sales volume, not sales amount. 
- If you answer questions about quantities, make sure to include the quantity. 
- If the user asks about promotion, note that "No Discount" appearing in dimpromotion is not actually a promotion. Always filter out the "No Discount" if user asks about promotion. 
- If the question is generic and involves no finite entities such as "top 5", use a reasonable number less than 10, so that answer is not too large.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run a loop over the generated questions and get an answer from AI Skill
def get_context_from_aiSkill(questions):
    ai_skill_response = []
    ai_skill_sql = []
    for question in questions:
        response = aiSkill(notes, question, aiskill_url)
        ai_skill_response.append(f"headers: {response['ResultHeaders']}, rows: {response['ResultRows']}")
        ai_skill_sql.append(response['executedSQL'])
    return ai_skill_response, ai_skill_sql
    questions.append(query)
    ai_skill_response, ai_skill_sql = get_context_from_aiSkill(questions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Adjust Pandas display options to prevent truncation of long strings
pd.set_option('display.max_colwidth', None)  # Set to 'None' to display all contents without truncation

def display_ai_skill_data(questions, ai_skill_sql, ai_skill_response):
    """
    This function takes questions, AI Skill SQL queries, and AI Skill responses, and 
    displays them in a table with three columns: 'Question', 'SQL Query', and 'AI Skill Response'.
    
    Args:
    questions (list): List of questions.
    ai_skill_sql (list): List of SQL queries generated by AI Skill.
    ai_skill_response (list): List of responses generated by AI Skill.
    
    Returns:
    DataFrame: A pandas DataFrame containing the questions, SQL queries, and responses.
    """
    # Create DataFrame
    df = pd.DataFrame({
        'Question': questions,
        'SQL Query': ai_skill_sql,
        'AI Skill Response': ai_skill_response
    })
    
    # Display the DataFrame
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_context_to_answer_with_llm(query, rephrased_query, ai_skill_response):
    context = ""
    for index, question in enumerate(questions):
        context += f"rephrased question: {question} \n" + f"header and rows of the table: {ai_skill_response[index]}\n" 

    context += f"user query: {query}"
    return context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context = get_context_to_answer_with_llm(query, questions, ai_skill_response)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
