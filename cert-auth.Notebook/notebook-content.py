# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests

# Path to your certificate and private key files
cert_file = '/path/to/cert.pem'
key_file = '/path/to/key.pem'

# URL of the REST endpoint
url = 'https://your-rest-api-endpoint.com/resource'

# Any headers you want to send
headers = {
    'Content-Type': 'application/json'
}

# Optional: Any payload/data to send with the request
data = {
    'param1': 'value1',
    'param2': 'value2'
}

# Making a POST request with client certificate authentication
response = requests.post(
    url,
    json=data,
    headers=headers,
    cert=(cert_file, key_file),
    verify=True  # Set path to CA bundle if needed, or False to skip verification
)

print('Status Code:', response.status_code)
print('Response Body:', response.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
