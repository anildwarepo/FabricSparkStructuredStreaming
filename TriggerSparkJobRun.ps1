
# This script triggers either a pipeline or a spark job in Microsoft Fabric using the REST API.
# It retrieves an access token using the Azure CLI and then makes a GET request to the Fabric API to list workspaces.

# It uses the current logged-in Azure account to get the access token. 
# The logged-in account must have the necessary permissions to access the Fabric Workspace and trigger jobs.
# If Service Principal is used, it needs the SparkJobDefinition.Execute.All or Item.Execute.All delegated permission.

# The scripts required workspaceId, pipelineId or spark_job_id to be defined in the script.
# Use  az login to login to Azure account before running the script.

$accessToken = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken --output tsv


$headers = @{
    "Authorization" = "Bearer $accessToken"
    "Content-Type"  = "application/json"
}

$url = "https://api.fabric.microsoft.com/v1/workspaces"

$response = Invoke-RestMethod -Uri $url -Method Get -Headers $headers
$response | ConvertTo-Json

# Uncomment the following to submit a pipeline job
#Write-Output "----------------------------------------"
#Write-Output "Running Pipeline...."
#Write-Output "----------------------------------------"
#$workspaceId = "b1a1dad3-61f0-4438-be14-1651717fcaf7"
#$pipelineId = "7d20bdcc-18ef-4cd2-b292-e2437b8c0fa0" #StructuredStreamingPipeline
#$pipe_line_url = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$pipelineId/jobs/instances?jobType=Pipeline"



Write-Output "----------------------------------------"
Write-Output "Running Spark job...."
Write-Output "----------------------------------------"

$headers = @{
    "Authorization" = "Bearer $accessToken"
    "Content-Type"  = "application/json"
}



$spark_job_id = "93d378bf-614e-4eea-a2bb-7eaccb39d67c"
$sjd_url = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/sparkJobDefinitions/$spark_job_id/jobs/instances?jobType=sparkjob"
$response = Invoke-RestMethod -Uri $sjd_url -Method Post -Headers $headers
$response | ConvertTo-Json
