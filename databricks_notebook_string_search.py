from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql import Row
import base64
import requests
import json

databricks_instance ="https://databricks"

url_list = f"{databricks_instance}/api/2.0/workspace/list"
url_export = f"{databricks_instance}/api/2.0/workspace/export"


payload = json.dumps({
  "path": "/Users"
})
headers = {
  'Authorization': 'Bearer <databricks-access-token>',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url_list, headers=headers, data=payload).json()
notebooks = []

# Getting the all notebooks list for given notebooks.

def list_notebooks(mylist):
  for element in mylist['objects']:
    if element['object_type'] == 'NOTEBOOK':
      notebooks.append(element)
    if element['object_type'] == 'DIRECTORY':
      payload_inner = json.dumps({
        "path": element['path']
      })
      response_inner = requests.request("GET", url_list, headers=headers, data=payload_inner).json()
      if len(response_inner) != 0:
        list_notebooks(response_inner)
  return notebooks

result = list_notebooks(response)
print(result[0])

#     
class BearerAuth(requests.auth.AuthBase):
      def __init__(self, token):
          self.token = token
      def __call__(self, r):
          r.headers["authorization"] = 'Bearer <databricks-access-token>'
          
          return r
# Define the function to get the permiss


var_current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()



for notebook_items in result : 
    var_notebook_path = notebook_items["path"]
    var_object_id     = notebook_items["object_id"]
    var_note_payload  =  json.dumps({"path": f"{var_notebook_path}"})
    var_response = requests.request("GET", url_export, headers=headers, data=var_note_payload).json()

    try:
        var_response_content=var_response['content']
    except:
        # this is limitation when we handle the notebook objects via rest api if object size more than 10 MB
        # Continue to next iteration as some time note book has some limitation {'error': 'DatabricksServiceException: BAD_REQUEST: content size (15395930) exceeded the limit 10485760'}
        continue
    var_response_content_str= base64.b64decode(var_response_content).decode("utf-8") 

     if var_response_content_str.find(var_response_content_str.find("<search_string") != -1 and var_notebook_path !=var_current_notebook_path :
       print(var_notebook_path)

