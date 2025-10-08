import time
import logging
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError


class AzureBase(object):

    def __init__(self, account_name, account_key):
        """
        Initialize Azure Base class for authentication and session management.
        
        :param account_name: Azure Storage account name
        :param account_key: Azure Storage account key
        """
        self.blob_service_client = None
        logging.basicConfig()
        self.logger = logging.getLogger("Azure_Util")
        self.logger.setLevel(logging.INFO)
        
        self.account_name = account_name
        self.account_key = account_key
        
        self.create_session()

    def create_session(self):
        """
        Create a session to Azure using the account name and account key.
        """
        try:
            account_url = f"https://{self.account_name}.blob.core.windows.net"
            self.blob_service_client = BlobServiceClient(
                account_url=account_url, 
                credential=self.account_key)
                
        except Exception as e:
            self.logger.error(f"Failed to create Azure session: {e}")
            raise


class Azure(AzureBase):

    def __init__(self, account_name, account_key):
        """
        Initialize Azure Blob Storage client for container operations.
        
        :param account_name: Azure Storage account name
        :param account_key: Azure Storage account key
        """
        super(Azure, self).__init__(account_name, account_key)

    def create_container(self, container_name, public_access=None, metadata=None):
        """
        Create an Azure Blob Storage container.

        :param container_name: Name of the container to create
        :param public_access: Public access level ('container', 'blob', or None)
        :param metadata: Optional metadata dictionary
        :return: True if container created successfully, else False
        """
        try:
            self.logger.info(f"Creating container: {container_name}")
            
            container_client = self.blob_service_client.get_container_client(container_name)
            
            # Create container with optional public access and metadata
            container_client.create_container(
                public_access=public_access,
                metadata=metadata
            )
            
            self.logger.info(f"Container '{container_name}' created successfully")
            return True
            
        except ResourceExistsError:
            self.logger.warning(f"Container '{container_name}' already exists")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create container '{container_name}': {e}")
            return False

    def delete_container(self, container_name, max_retry=5, retry_attempt=0):
        """
        Delete an Azure Blob Storage container.
        
        :param container_name: Name of the container to delete
        :param max_retry: Maximum number of retry attempts
        :param retry_attempt: Current retry attempt number
        :return: True if container deleted successfully, else False
        """
        try:
            self.logger.info(f"Attempting to delete container: {container_name}")
            
            if container_name in self.list_existing_containers():
                container_client = self.blob_service_client.get_container_client(container_name)
                
                # Delete the container
                container_client.delete_container()
                
                self.logger.info(f"Container '{container_name}' deleted successfully")
                return True
            else:
                self.logger.warning(f"Container '{container_name}' does not exist")
                return True
                
        except ResourceNotFoundError:
            self.logger.warning(f"Container '{container_name}' not found")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete container '{container_name}': {e}")
            if retry_attempt < max_retry:
                self.logger.info(f"Retrying deletion of container '{container_name}' (attempt {retry_attempt + 1}/{max_retry})")
                time.sleep(5)
                return self.delete_container(container_name, max_retry, retry_attempt + 1)
            return False

    def list_existing_containers(self):
        """
        List all existing containers in the Azure Storage account.
        
        :return: List of container names
        """
        try:
            self.logger.info("Listing existing containers")
            containers = []
            
            container_list = self.blob_service_client.list_containers()
            for container in container_list:
                containers.append(container['name'])
                
            self.logger.info(f"Found {len(containers)} containers")
            return containers
            
        except Exception as e:
            self.logger.error(f"Failed to list containers: {e}")
            return []

    def container_exists(self, container_name):
        """
        Check if a container exists.
        
        :param container_name: Name of the container to check
        :return: True if container exists, else False
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            container_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            return False
        except Exception as e:
            self.logger.error(f"Error checking if container '{container_name}' exists: {e}")
            return False

    def get_container_properties(self, container_name):
        """
        Get properties of a container.
        
        :param container_name: Name of the container
        :return: Container properties dictionary or None if not found
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            properties = container_client.get_container_properties()
            
            return {
                'name': properties.name,
                'last_modified': properties.last_modified,
                'etag': properties.etag,
                'lease_status': properties.lease.status if properties.lease else None,
                'public_access': properties.public_access,
                'metadata': properties.metadata
            }
            
        except ResourceNotFoundError:
            self.logger.warning(f"Container '{container_name}' not found")
            return None
        except Exception as e:
            self.logger.error(f"Failed to get properties for container '{container_name}': {e}")
            return None

    def empty_container(self, container_name):
        """
        Delete all blobs in a container.
        
        :param container_name: Name of the container to empty
        :return: True if container emptied successfully, else False
        """
        try:
            self.logger.info(f"Emptying container: {container_name}")
            
            container_client = self.blob_service_client.get_container_client(container_name)
            
            # List and delete all blobs in the container
            blob_list = container_client.list_blobs()
            for blob in blob_list:
                blob_client = container_client.get_blob_client(blob.name)
                blob_client.delete_blob()
                self.logger.debug(f"Deleted blob: {blob.name}")
                
            self.logger.info(f"Container '{container_name}' emptied successfully")
            return True
            
        except ResourceNotFoundError:
            self.logger.warning(f"Container '{container_name}' not found")
            return False
        except Exception as e:
            self.logger.error(f"Failed to empty container '{container_name}': {e}")
            return False

    def list_objects_in_azure_container(self, container_name):
        """
        List all objects (blobs) in an Azure Blob Storage container.
        
        :param container_name: Name of the container to list objects from
        :return: List of blob names in the container
        """
        try:
            self.logger.info(f"Listing objects in container: {container_name}")
            
            container_client = self.blob_service_client.get_container_client(container_name)
            
            # List all blobs in the container
            blob_list = container_client.list_blobs()
            objects = []
            
            for blob in blob_list:
                objects.append(blob.name)
                
            self.logger.info(f"Found {len(objects)} objects in container '{container_name}'")
            return objects
            
        except ResourceNotFoundError:
            self.logger.warning(f"Container '{container_name}' not found")
            return []
        except Exception as e:
            self.logger.error(f"Failed to list objects in container '{container_name}': {e}")
            return []

    def download_file_from_azure_container(self, container_name, blob_name, dest_path):
        """
        Download a file from an Azure Blob Storage container.
        
        :param container_name: Name of the container
        :param blob_name: Name of the blob to download
        :param dest_path: Local path where the file should be saved
        :return: True if download successful, else False
        """
        try:
            self.logger.info(f"Downloading blob '{blob_name}' from container '{container_name}' to '{dest_path}'")
            
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Download the blob to local file
            with open(dest_path, "wb") as download_file:
                download_stream = blob_client.download_blob()
                download_file.write(download_stream.readall())
                
            self.logger.info(f"Successfully downloaded blob '{blob_name}' to '{dest_path}'")
            return True
            
        except ResourceNotFoundError:
            self.logger.error(f"Blob '{blob_name}' not found in container '{container_name}'")
            return False
        except Exception as e:
            self.logger.error(f"Failed to download blob '{blob_name}' from container '{container_name}': {e}")
            return False
