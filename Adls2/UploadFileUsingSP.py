#Upload a file to ADLS2 using a Service Principal

from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import os

#azure related params
tenant_id="tenant_id_here"
client_id="service_principal_app_id"
client_secret="service_principal_secret"
datalake_name="adls2_name"
container_name="container_name"
directory_name="folder_to_push_file_into"
file_name="file_name"

#local file related params
local_directory_name="local_folder_to_read_file_from"
local_file_name=file_name #update this if the file is named differently in local FS

#connect to adls2, create file
credential = ClientSecretCredential(tenant_id,client_id,client_secret)
service_client = DataLakeServiceClient(account_url=f"https://{datalake_name}.dfs.core.windows.net/",credential=credential)
container_client = service_client.get_file_system_client(file_system=container_name)
directory_client = container_client.get_directory_client(directory_name)
file_client = directory_client.create_file(file_name)

#read local file, write to adls2
with open(os.path.join(local_directory_name,local_file_name), "rb") as f:
    file_contents=f.read()
file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
file_client.flush_data(len(file_contents))
