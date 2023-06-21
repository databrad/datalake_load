import os
import zipfile
#from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient

conn_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
print(conn_string)

# Set your Azure Blob Storage account connection string
conn_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
# Set the name for your container and the local folder path
container_name = 'blob-container-01'
local_folder_path = './data'

def upload_files_to_blob_storage(connection_string, container_name, local_folder_path):
    try:
        # Create the BlobServiceClient object
        #blob_service_client = BlobServiceClient.from_connection_string(connection_string, connection_timeout=120,socket_timeout=600,)

        # Get all files from the local folder and its subfolders
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                blob_name = file_path.replace(local_folder_path, "").replace("\\", "/").lstrip("/")

                # Upload regular files to blob storage
                if not file.endswith('.zip'):
                    # Create a blob client
                    #blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                    blob_client = BlobClient.from_connection_string(
                        connection_string,
                        container_name=container_name,
                        blob_name=blob_name,
                        connection_timeout=120,
                        socket_timeout=600,
                    )
                    #.replace(".txt","").replace(root,"")

                    # Upload the file to blob storage
                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)

                    print(f"Uploaded: {file_path}")

                # Unzip files and upload their contents to blob storage
                else:
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_files = zip_ref.namelist()
                        for zip_file in zip_files:
                            zip_file_path = os.path.join(file_path, zip_file)
                            zip_blob_name = zip_file_path.replace(local_folder_path, "").replace("\\", "/").lstrip("/")

                            # Create a blob client
                            #blob_client = blob_service_client.get_blob_client(container=container_name, blob=zip_blob_name)
                            blob_client = BlobClient.from_connection_string(
                                connection_string,
                                container_name=container_name,
                                blob_name=zip_blob_name,
                                connection_timeout=120,
                                socket_timeout=600,
                            )
                            # Upload the file to blob storage
                            with zip_ref.open(zip_file) as data:
                                blob_client.upload_blob(data, overwrite=True)

                            print(f"Uploaded: {zip_file_path}")

        print("All files uploaded successfully!")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Call the function to upload files
upload_files_to_blob_storage(conn_string, container_name, local_folder_path)
