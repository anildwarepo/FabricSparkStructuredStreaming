import requests
from azure.identity import DefaultAzureCredential
import json
import base64

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
#sjdArtifactId = ""

def create_spark_job_definition(sjdName: str):


    credential = DefaultAzureCredential()
    fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token  # replace this token with the real AAD token

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
    sjdCreateUrl = f"https://api.fabric.microsoft.com//v1/workspaces/{workspaceId}/items"
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
    defaultLakehouseId = 'ab88234c-5134-4115-8ff3-e780d45740bd'; # replace this with the real default lakehouse id

    updateRequestBodyJson = {
        "executableFile":mainAbfssPath,
        "defaultLakehouseArtifactId":defaultLakehouseId,
        "mainClass":"",
        "additionalLakehouseIds":[],
        "retryPolicy":None,
        "commandLineArguments":"",
        "additionalLibraryUris": "",#[libsAbfssPath],
        "language":"Python",
        "environmentArtifactId":None}

    # Encode the bytes as a Base64-encoded string
    base64EncodedUpdateSJDPayload = json_to_base64(updateRequestBodyJson)

    # Print the Base64-encoded string
    #print("Base64-encoded JSON payload for SJD Update:")
    #print(base64EncodedUpdateSJDPayload)

    # Define the API URL
    updateSjdUrl = f"https://api.fabric.microsoft.com//v1/workspaces/{workspaceId}/items/{sjdArtifactId}/updateDefinition"

    updatePayload = base64EncodedUpdateSJDPayload
    payloadType = "InlineBase64"
    path = "SparkJobDefinitionV1.json"
    format = "SparkJobDefinitionV1"
    Type = "SparkJobDefinition"


    credential = DefaultAzureCredential()
    fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token  # replace this token with the real AAD token

    
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
    
    credential = DefaultAzureCredential()
    fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token  # replace this token with the real AAD token

    sdjurl = f"https://api.fabric.microsoft.com//v1/workspaces/{workspaceId}/sparkJobDefinitions/{sjdArtifactId}/jobs/instances?jobType=sparkjob"
    
    headers = {
        "Authorization": f"Bearer {fabric_token}", 
        "Content-Type": "application/json"  # Set the content type based on your request
    }
    response = requests.post(sdjurl,headers=headers)

    if response.status_code == 202:
        print("Spark job submitted successfully.")
    else:
        print(f"Failed to submit Spark job. Status code: {response.status_code}, Response: {response.text}")


def cancel_sjd(sjdArtifactId:str):
    
    credential = DefaultAzureCredential()
    fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token  # replace this token with the real AAD token

    sdjurl = f"https://api.fabric.microsoft.com//v1/workspaces/{workspaceId}/sparkJobDefinitions/{sjdArtifactId}"
    
    headers = {
        "Authorization": f"Bearer {fabric_token}", 
        "Content-Type": "application/json"  # Set the content type based on your request
    }
    response = requests.delete(sdjurl,headers=headers)

    if response.status_code == 200:
        print("Spark job cancelled successfully.")
    else:
        print(f"Failed to cancel Spark job. Status code: {response.status_code}, Response: {response.text}")


# Example usage
## Comm

if __name__ == "__main__":
    
    
    ### Create a new Spark Job Definition (SJD) and upload the main executable file to OneLake
    
    #sjdArtifactId = create_spark_job_definition(sjdName="anildwaSJD4")


    #print(f"SJD Artifact ID: {sjdArtifactId}")
    
    ### Upload the main executable file to the SJD in OneLake
    #upload_spark_job_definition_file(sjdArtifactId)

    sjdArtifactId = "b71c901b-a63c-4c40-a9fc-4c1bf3a59402"

    ### Update the SJD with the main executable file and other parameters
    #update_sjd(sjdArtifactId, sjdName="anildwaSJD4")


    ### Submit the SJD to run the Spark job
    #submit_sjd(sjdArtifactId)

    ### Cancel the SJD and delete the Spark Job Definition artifact 
    ## This will cancel the SJD and delete the Spark Job Definition artifact from OneLake
    ## To submit the SDJ again, you need to create a new SJD and upload the main executable file again
    cancel_sjd(sjdArtifactId)