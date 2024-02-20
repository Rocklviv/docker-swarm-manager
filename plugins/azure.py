import os
from core.storage import ObjectStorageInterface
from azure.storage.blob import BlobServiceClient


class AzureStorage(ObjectStorageInterface):
    
    def __init__(self):
        conn_string = os.getenv("AZURE_CONNECTION_STRING")
        self.container = os.getenv("AZURE_CONTAINER_NAME")
        self.azure_client = BlobServiceClient.from_connection_string(conn_string)
    
    def get_object(self, name: str) -> str:
        """
        """
        blob_client = self.azure_client.get_blob_client(
            self.container, blob=name
        )
        return blob_client.download_blob().readall().decode("utf-8")
    
    def put_object(self, name: str, data: str) -> None:
        """
        """
        blob_client = self.azure_client.get_container_client(
            container=self.container
        )
        blob_client.upload_blob(name=name, data=data)
    
    def del_object(self, name: str) -> None:
        """
        """
        return self.azure_client.get_blob_client(
            container=self.container, blob=name
        ).delete_blob()
    
    def list_object(self, path: str) -> list:
        """
        """
        blob_client = self.azure_client.get_container_client(
            container=self.container
        )
        return blob_client.list_blobs(path)
    
    def is_exists(self, name: str) -> bool:
        """
        """
        blob_client = self.azure_client.get_blob_client(
            container=self.container, blob=name
        )
        if blob_client.exists():
            return True
        return False
    