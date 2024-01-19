from azure.storage.blob import ContainerClient
from azure.identity import ClientSecretCredential
import csv

tenant_id="tenant id"
client_id="service principal id"
client_secret="service principal secret"
datalake_name = "datalake name"
container_name = "container name"

credential = ClientSecretCredential(tenant_id,client_id,client_secret)
ct_client = ContainerClient(account_url=f"https://{datalake_name}.blob.core.windows.net", container_name=container_name,credential=credential)
blobList = ct_client.list_blob_names()
csvDict = {}

#reads blob name to csvDict.keys() and blob contents to csvDict.values()
for blob in blobList:
    content = ct_client.download_blob(blob).readall().decode()
    csvDict[blob] = content

#printing all blobs and their contents
for key in csvDict:
    print(f"{key}:\n{csvDict[key]}\n\n")
