import requests
from azure.identity import DefaultAzureCredential, EnvironmentCredential, InteractiveBrowserCredential
import json
import base64
import os
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
import sys

load_dotenv()

def json_to_base64(json_data):
    # Serialize the JSON data to a string
    json_string = json.dumps(json_data)
    
    # Encode the JSON string as bytes
    json_bytes = json_string.encode('utf-8')
    
    # Encode the bytes as Base64
    base64_encoded = base64.b64encode(json_bytes).decode('utf-8')
    
    return base64_encoded

def base64_to_json(base64_data):
    # Decode the Base64-encoded string to bytes
    base64_bytes = base64_data.encode('utf-8')
    
    # Decode the bytes to a JSON string
    json_string = base64.b64decode(base64_bytes).decode('utf-8')
    
    # Deserialize the JSON string to a Python dictionary
    json_data = json.loads(json_string)
    
    return json_data

workspaceId = "b1a1dad3-61f0-4438-be14-1651717fcaf7"
mainExecutableFile = "StreamingSparkJob.py"
defaultLakehouseId = "ab88234c-5134-4115-8ff3-e780d45740bd"


def get_fabric_token():
    # Create a credential object using DefaultAzureCredential or EnvironmentCredential

    # If you EnvironmentCredential, make sure to set the following environment variables:
    # AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET are set in your environment
    # The AZURE_CLIENT_ID should be given contributor access to the Fabric workspace

    credential = EnvironmentCredential() # DefaultAzureCredential()

    # Get the token for the Fabric API
    fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token

    return fabric_token

fabric_token = get_fabric_token()


def create_spark_job_definition(sjdName: str):

    headers = {
        "Authorization": f"Bearer {fabric_token}", 
        "Content-Type": "application/json"  # Set the content type based on your request
    }

    payload = "eyJleGVjdXRhYmxlRmlsZSI6bnVsbCwiZGVmYXVsdExha2Vob3VzZUFydGlmYWN0SWQiOiIiLCJtYWluQ2xhc3MiOiIiLCJhZGRpdGlvbmFsTGFrZWhvdXNlSWRzIjpbXSwicmV0cnlQb2xpY3kiOm51bGwsImNvbW1hbmRMaW5lQXJndW1lbnRzIjoiIiwiYWRkaXRpb25hbExpYnJhcnlVcmlzIjpbXSwibGFuZ3VhZ2UiOiIiLCJlbnZpcm9ubWVudEFydGlmYWN0SWQiOm51bGx9"

    # Define the payload data for the POST request
    payload_data = {
        "displayName": sjdName,
        "Type": "SparkJobDefinition",
        "definition": {
            "format": "SparkJobDefinitionV1",
            "parts": [
                {
                    "path": "SparkJobDefinitionV1.json",
                    "payload": payload,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    # Make the POST request with Bearer authentication
    sjdCreateUrl = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/sparkJobDefinitions"
    response = requests.post(sjdCreateUrl, json=payload_data, headers=headers)

    if response.status_code == 201:
        # Request was successful
        print("Spark Job Definition created successfully.")
        sjdArtifactId = response.json().get("id")
        return sjdArtifactId
    else:
        # Handle the error response
        print(f"Error: {response.status_code}, {response.text}")
        return None






def upload_spark_job_definition_file(sjdArtifactId:str):

    # three steps are required: create file, append file, flush file

    onelakeEndPoint = f"https://onelake.dfs.fabric.microsoft.com/{workspaceId}/{sjdArtifactId}" # replace the id of workspace and artifact with the right one
     # the name of the main executable file
    mainSubFolder = "Main" # the sub folder name of the main executable file. Don't change this value

    credential = DefaultAzureCredential()
    onelakeStorageToken = credential.get_token("https://storage.azure.com/.default").token

    onelakeRequestMainFileCreateUrl = f"{onelakeEndPoint}/{mainSubFolder}/{mainExecutableFile}?resource=file" # the url for creating the main executable file via the 'file' resource type
    onelakePutRequestHeaders = {
        "Authorization": f"Bearer {onelakeStorageToken}", # the storage token can be achieved from the helper function above
    }

    onelakeCreateMainFileResponse = requests.put(onelakeRequestMainFileCreateUrl, headers=onelakePutRequestHeaders)
    if onelakeCreateMainFileResponse.status_code == 201:
        # Request was successful
        print(f"Main File '{mainExecutableFile}' was successfully created in onelake.")

    # with previous step, the main executable file is created in OneLake, now we need to append the content of the main executable file

    appendPosition = 0
    appendAction = "append"

    ### Main File Append.

    onelakeRequestMainFileAppendUrl = f"{onelakeEndPoint}/{mainSubFolder}/{mainExecutableFile}?position={appendPosition}&action={appendAction}"

    with open (mainExecutableFile, "rb") as file:
        mainFileContents = file.read() # read the content of the main executable file

    # if you want to use the content of the main executable file directly, you can uncomment the following line and comment the above line

    #mainFileContents = 
    mainExecutableFileSizeInBytes =  len(mainFileContents) # the size of the main executable file in bytes, this value should match the size of the mainFileContents

    onelakePatchRequestHeaders = {
        "Authorization": f"Bearer {onelakeStorageToken}",
        "Content-Type" : "text/plain"
    }

    onelakeAppendMainFileResponse = requests.patch(onelakeRequestMainFileAppendUrl, data = mainFileContents, headers=onelakePatchRequestHeaders)
    if onelakeAppendMainFileResponse.status_code == 202:
        # Request was successful
        print(f"Successfully Accepted Main File '{mainExecutableFile}' append data.")

    # with previous step, the content of the main executable file is appended to the file in OneLake, now we need to flush the file

    flushAction = "flush"

    ### Main File flush
    onelakeRequestMainFileFlushUrl = f"{onelakeEndPoint}/{mainSubFolder}/{mainExecutableFile}?position={mainExecutableFileSizeInBytes}&action={flushAction}"
    print(onelakeRequestMainFileFlushUrl)
    onelakeFlushMainFileResponse = requests.patch(onelakeRequestMainFileFlushUrl, headers=onelakePatchRequestHeaders)
    if onelakeFlushMainFileResponse.status_code == 200:
        print(f"Successfully Flushed Main File '{mainExecutableFile}' contents.")
    else:
        print(onelakeFlushMainFileResponse.json())



def update_sjd(sjdArtifactId:str, sjdName: str):
    mainAbfssPath = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{sjdArtifactId}/Main/{mainExecutableFile}" # the workspaceId and sjdartifactid are the same as previous steps, the mainExecutableFile is the name of the main executable file
    #libsAbfssPath = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{sjdartifactid}/Libs/{libsFile}"  # the workspaceId and sjdartifactid are the same as previous steps, the libsFile is the name of the libs file
    

    updateRequestBodyJson = {
        "executableFile":mainAbfssPath,
        "defaultLakehouseArtifactId":defaultLakehouseId,
        "mainClass":"",
        "additionalLakehouseIds":[],
        "retryPolicy":None,
        "commandLineArguments":"",
        "additionalLibraryUris": "",#[libsAbfssPath],
        "language":"Python",
        "environmentArtifactId": "1e8bc771-85ce-496f-8f9a-e253b67b04be"
        }

    # Encode the bytes as a Base64-encoded string
    base64EncodedUpdateSJDPayload = json_to_base64(updateRequestBodyJson)

    # Print the Base64-encoded string
    #print("Base64-encoded JSON payload for SJD Update:")
    #print(base64EncodedUpdateSJDPayload)

    # Define the API URL
    updateSjdUrl = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{sjdArtifactId}/updateDefinition"

    updatePayload = base64EncodedUpdateSJDPayload
    payloadType = "InlineBase64"
    path = "SparkJobDefinitionV1.json"
    format = "SparkJobDefinitionV1"
    Type = "SparkJobDefinition"
    
    headers = {
        "Authorization": f"Bearer {fabric_token}", 
        "Content-Type": "application/json"  # Set the content type based on your request
    }

    # Define the payload data for the POST request
    payload_data = {
        "displayName": sjdName,
        "Type": Type,
        "definition": {
            "format": format,
            "parts": [
                {
                    "path": path,
                    "payload": updatePayload,
                    "payloadType": payloadType
                }
            ]
        }
    }


    # Make the POST request with Bearer authentication
    response = requests.post(updateSjdUrl, json=payload_data, headers=headers)
    if response.status_code == 200:
        print("Successfully updated SJD.")
    else:
        print(response.json())
        print(response.status_code)



def submit_sjd(sjdArtifactId:str):
    sdjurl = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/sparkJobDefinitions/{sjdArtifactId}/jobs/instances?jobType=sparkjob"
    
    headers = {
        "Authorization": f"Bearer {fabric_token}", 
        "Content-Type": "application/json"  # Set the content type based on your request
    }
    response = requests.post(sdjurl,headers=headers)

    if response.status_code == 202:
        print(f"response: {response.url}")
        print("Spark job submitted successfully.")
    else:
        print(f"Failed to submit Spark job. Status code: {response.status_code}, Response: {response.text}")


def cancel_sjd(sjdArtifactId:str):
    sdjurl = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/sparkJobDefinitions/{sjdArtifactId}"
   
    headers = {
        "Authorization": f"Bearer {fabric_token}", 
        "Content-Type": "application/json"  # Set the content type based on your request
    }
    response = requests.delete(sdjurl,headers=headers)

    if response.status_code == 200:

        print("Spark job cancelled successfully.")
    else:
        print(f"Failed to cancel Spark job. Status code: {response.status_code}, Response: {response.text}")




def submit_job(sjdName):
    
    sjdArtifactId = create_spark_job_definition(sjdName=sjdName)
    print(f"SJD Artifact ID: {sjdArtifactId}")
    upload_spark_job_definition_file(sjdArtifactId)
    update_sjd(sjdArtifactId, sjdName=sjdName)
    submit_sjd(sjdArtifactId)
    return sjdArtifactId



def get_app_only_token(tenant_id, client_id, client_secret, audience):
    """
    Get an app-only access token for a Service Principal using OAuth 2.0 client credentials flow.

    Args:
        tenant_id (str): The Azure Active Directory tenant ID.
        client_id (str): The Service Principal's client ID.
        client_secret (str): The Service Principal's client secret.
        audience (str): The audience for the token (e.g., resource-specific scope).

    Returns:
        str: The access token.
    """
    try:
        # Define the authority URL for the tenant
        authority = f"https://login.microsoftonline.com/{tenant_id}"

        # Create a ConfidentialClientApplication instance
        app = ConfidentialClientApplication(
            client_id = client_id,
            client_credential = client_secret,
            authority = authority
        )

        # Acquire a token using the client credentials flow
        result = app.acquire_token_for_client(scopes = [audience])

        # Check if the token was successfully retrieved
        if "access_token" in result:
            return result["access_token"]
        else:
            raise Exception("Failed to retrieve token: {result.get('error_description', 'Unknown error')}")
    except Exception as e:
        print(f"Error retrieving token: {e}", fil = sys.stderr)
        sys.exit(1)


livy_token = InteractiveBrowserCredential().get_token("https://api.fabric.microsoft.com/.default").token

def get_livy_sessions(sjdArtifactId:str):
  
    sjdurl = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/sparkJobDefinitions/{sjdArtifactId}/livySessions"
 
    headers = {
        "Authorization": f"Bearer {livy_token}", 
        "Content-Type": "application/json"  # Set the content type based on your request
    }
    response = requests.get(sjdurl,headers=headers)

    if response.status_code == 200:
        livyId = response.json()['value'][0]['livyId']
        print(f"livyId: {livyId}")
        return livyId
    else:
        print(f"Failed fetch livy session. Status code: {response.status_code}, Response: {response.text}")
        return None


def cancel_livy_session(livyId:str):

    sjdurl = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses/{defaultLakehouseId}/livyapi/versions/2023-12-01/sessions/{livyId}"
    headers = {"Authorization": "Bearer " + livy_token}
    headers = {
        "Authorization": f"Bearer {fabric_token}",
        "Content-Type": "application/json"  # Set the content type based on your request
    }
    response = requests.delete(sjdurl,headers=headers)
    if response.status_code == 200:
        print("Livy session cancelled successfully.")
    else:
        print(f"Failed to cancel Livy session. Status code: {response.status_code}, Response: {response.text}")



if __name__ == "__main__":
    sjdName = "anildwaSJD15"
    
    #sjdArtifactId = submit_job(sjdName)
    
    sjdArtifactId = "ca0bad85-52df-419b-bdb9-2d0bbd4a7021"
    #livyId = get_livy_sessions(sjdArtifactId)
    livyId = "c4119203-c37f-41e9-a1de-49ca6b55a4d9"
    cancel_livy_session(livyId)

    ### Create a new Spark Job Definition (SJD) and upload the main executable file to OneLake
   
    ### Upload the main executable file to the SJD in OneLake
    #upload_spark_job_definition_file(sjdArtifactId)

    

    ### Update the SJD with the main executable file and other parameters
    #update_sjd(sjdArtifactId, sjdName="anildwaSJD4")


    ### Submit the SJD to run the Spark job
    #submit_sjd(sjdArtifactId)

    ### Cancel the SJD and delete the Spark Job Definition artifact 
    ## This will cancel the SJD and delete the Spark Job Definition artifact from OneLake
    ## To submit the SDJ again, you need to create a new SJD and upload the main executable file again
    #sjdArtifactId = "da9ab9c1-5a3b-4d0e-b5d2-ef7b1d1a5370"
    #cancel_sjd(sjdArtifactId)