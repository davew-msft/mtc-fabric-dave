# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b7792b7e-43fe-419e-8a88-523373759c36",
# META       "default_lakehouse_name": "mrk",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "b7792b7e-43fe-419e-8a88-523373759c36"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# * AI Foundry project: mrk
# * deployed gpt-40 (pretty good with multi-modal pdfs)
# 
# for content understanding:  
# * ai foundry:  cont-under-pjm-resource
# * project:  mrk-pdf


# CELL ********************

#vars
endpoint = "https://mrk-resource.cognitiveservices.azure.com/"
ky = "GaVU6jRRJo3DbBlj47h1kXRHU6BV1EvfkLgQ4X11vp3dDSJvTPQkJQQJ99BJACHYHv6XJ3w3AAAAACOGSPLU"
model_name = "gpt-4.1"
deployment = "gpt-4.1"
api_version = "2024-12-01-preview"

sysprompt = """
You are an expert at reading PDF documents for a global biopharm company.  
You have Contract Manufacturing Organizations that help you make you products.  
They said you "Certificate of Analysis" documents, in pdf format, sometimes scanned, sometimes text-based/digital.  
You need to extract pertinent information from these documents so I can ask questions about them.  

Most will have tables of data, usually with columns like 
* test name or test description
* acceptance criteria or SOP
* results
* the "measurement unit"

But not always.  
Your job is to extract the relevant information.  I will use this information to ask questions of the pdf and possibly load the data into a database.  
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!pip install PyMuPDF openai

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import fitz  # PyMuPDF
from openai import AzureOpenAI
import json
import io
import json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

client = AzureOpenAI(
    api_version=api_version,
    azure_endpoint=endpoint,
    api_key=ky,
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Example: Set a variable to the path of a file in your default lakehouse (mrk)

file_variable = '/lakehouse/mrk/Files/your_filename.ext'  # replace your_filename.ext with your actual file name

# Now you can use file_variable to refer to your file elsewhere in your notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

def gpt_analyze(input):
    response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": sysprompt,
            },
            {
                "role": "user",
                "content": input,
            }
        ],
        max_tokens=4096,
        temperature=1.0,
        top_p=1.0,
        model=deployment
    )
    print(response.choices[0].message.content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#file1 = '/lakehouse/default/Files/C1900143-final-CofAs.pdf'
#file2 = '/lakehouse/default/Files/CoA-G0035A.pdf'
file1 = '/lakehouse/default/Files/C1900143-CofAs.json'
file2 = '/lakehouse/default/Files/CoA-G0035A.json'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

def getJSON (file):
    with open(file, 'r', encoding='utf-8') as f:
        json_content = f.read()
    return json_content



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

answer_with_file = file2
json_content = getJSON(answer_with_file)
myprompt = f"""
Question:  

How many tests were executed?

Answer using the following json:  
{json_content}
"""


response = client.chat.completions.create(
    messages=[
        {
            "role": "system",
            "content": sysprompt,
        },
        {
            "role": "user",
            "content": myprompt,
        }
    ],
     max_completion_tokens=13107,
    temperature=1.0,
    top_p=1.0,
    frequency_penalty=0.0,
    presence_penalty=0.0,
    model=deployment
)

print(response.choices[0].message.content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

