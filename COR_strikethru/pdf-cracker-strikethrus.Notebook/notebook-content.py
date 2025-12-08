# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9a824d6f-11d9-40dd-a08b-0eade3dded86",
# META       "default_lakehouse_name": "cor",
# META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "9a824d6f-11d9-40dd-a08b-0eade3dded86"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# I have a version of this running locally too.  But this is easier.  
# 
# [mistral Docs](https://learn.microsoft.com/en-us/azure/ai-studio/how-to/deploy-models-mistral)
# 
# * `mistral-document-ai-2505` (based on mistral-ocr-2505)
# * can it handle strikethrough and white-out issues?
# * send pdfs/images to mistral (mistral-document-ai-2505), get back the results
# 
# You can setup mistral in AI Foundry.  Do that now.  

# CELL ********************

#vars
AZURE_ENDPOINT="https://cenc-resource.services.ai.azure.com/providers/mistral/azure/ocr"
AZURE_API_KEY="3vlbEHbb5gxVM5jftKDfVBEBxEeqAgUvgyF8iBIUIps1RwzJLrLTJQQJ99BJACHYHv6XJ3w3AAAAACOG6tBA"
REQUEST_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {AZURE_API_KEY}",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# from mistralai_azure import MistralAzure
# from mistralai_azure.models import ChatCompletionRequest
import httpx
import json
import base64
import requests
from IPython.display import Markdown, display
#client = MistralAzure(azure_endpoint=AZURE_ENDPOINT, azure_api_key=AZURE_API_KEY)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Helper Functions
def encode_image(image_path: str) -> str:
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")
    except FileNotFoundError:
        print(f"Error: The file {image_path} was not found.")
        return None


def replace_images_in_markdown(markdown_str: str, images_dict: dict) -> str:
    for img_name, base64_str in images_dict.items():
        markdown_str = markdown_str.replace(
            f"![{img_name}]({img_name})", f"![{img_name}]({base64_str})"
        )
    return markdown_str


def simple_combined_markdown(responsePage: dict) -> str:
    markdowns: list[str] = []
    image_data = {}
    for img in responsePage["images"]:
        image_data[img["id"]] = img["image_base64"]
    markdowns.append(replace_images_in_markdown(responsePage["markdown"], image_data))

    return "\n\n".join(markdowns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

filePolicyStrikethru = "/lakehouse/default/Files/17_Completed.pdf"
fileWhiteOut = "/lakehouse/default/Files/5_Multiple.pdf"
fileEE = "/lakehouse/default/Files/Scenario4EdwardElephant.pdf"
fileM = "/lakehouse/default/Files/mistral.pdf"
fileMM = "/lakehouse/default/Files/mistral.pdf"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# !chmod -R 770 /lakehouse/default/Files/

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# !ls -alFh /lakehouse/default/Files/

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# start by base64 encoding the file
encodedDocument = encode_image(fileEE)
# just print out the first 5 characters, having problems with permissions
print(encodedDocument[:5])
# print(encodedDocument)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# testing
# !wget https://raw.githubusercontent.com/mistralai/cookbook/refs/heads/main/mistral/ocr/mistral7b.pdf
# encodedDocument = encode_image("mistral7b.pdf")
# print(encodedDocument)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# this just gives text
documentPayload = {
    "model": "mistral-document-ai-2505",
    "document": {
        "type": "document_url",
        "document_url": f"data:application/pdf;base64,{encodedDocument}",
    },
}

documentResponse = requests.post(
    url=AZURE_ENDPOINT,
    json=documentPayload,
    headers=REQUEST_HEADERS,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# gimme the text
print(documentResponse.json()["pages"][0]["markdown"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# gimme the JSON
print(json.dumps(documentResponse.json(), indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Documents with Images Version


# CELL ********************

# this gives the images too
documentPayloadandImages = {
    "model": "mistral-document-ai-2505",
    "document": {
        "type": "document_url",
        "document_url": f"data:application/pdf;base64,{encodedDocument}",
    },
    "include_image_base64": "true",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

docWithImagesResponse = requests.post(
    url=AZURE_MISTRAL_DOCUMENT_AI_ENDPOINT,
    json=documentPayloadandImages,
    headers=REQUEST_HEADERS,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# gimme the JSON
print(json.dumps(documentResponse.json(), indent=4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# gimme the text
print(documentResponse.json()["pages"][0]["markdown"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# gimme the MD
display(Markdown(documentResponse.json()["pages"][0]["markdown"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ...but that only gives the "text".  Do we need images?  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## AI Foundry has examples for bbox and annotations.  Not sure we need that.  

# CELL ********************

## ignore everything below.  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

chat_response = client.chat.complete(
    messages=[
        {
            "role": "user",
            "content": "Who is the best French painter? Answer in one short sentence.",
        }
    ],
    max_tokens=50,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if chat_response:
    print(chat_response.choices[0].message.content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import httpx
import json

url = ""  # Add the URL to your mistral-small-2503 deployment here.

headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
payload = {
    "model": "mistral-small-2503",
    "messages": [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Describe this image in a short sentence."},
                {
                    "type": "image_url",
                    "image_url": {"url": "https://picsum.photos/id/237/200/300"},
                },
            ],
        }
    ],
}

resp = httpx.post(url=url, json=payload, headers=headers)
if resp:
    json_out = json.loads(resp.json()["choices"][0]["message"]["content"])
    print(json_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
