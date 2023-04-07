# Imports necessary for all functionality.
import argparse
import pandas as pd
import warnings
import pickle

# Imports necessary to read files installed on azure.
from azure.identity import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import DataLakeFileClient
from azure.storage.filedatalake import FileSystemClient

# Allows pyarrow to treat Azure like a pyarrow Filesystem.
# See https://arrow.apache.org/docs/python/filesystems.html

# import pyarrow.fs
# from pyarrow import Table
# import pyarrow.parquet as pq
# import pyarrowfs_adlgen2

def get_azure_file(file_path):
    storage_account_name = 'datalakebatchtest'
    mi_credential = ManagedIdentityCredential(
      client_id="446eb5aa-e9a3-4469-9cad-c09590d14b34"
    )
    credential = ChainedTokenCredential(mi_credential)
    destination = "mta-data"
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=credential)
    file_system_client = service_client.get_file_system_client(file_system=destination)
    local_file = open("file-to-download.txt",'wb')
    dscs_file = file_system_client.get_file_client(file_path)
    local_file.write(dscs_file.download_file().readall())
    file_system_client.create_file(file="test.cer")
    file_system_client.create_directory(directory="myoutput")
    new_file = file_system_client.get_file_client("myoutput/test.cer")
    new_file.upload_data(data=dscs_file.download_file().readall(),overwrite=True)
    local_file.close()
    #return dscs_file.download_file().readall()

get_azure_file("SQL-Root.cer")