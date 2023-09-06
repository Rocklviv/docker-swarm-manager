import os
import sys
import json
import docker
import logging
import urllib3
import requests
import azure.core.exceptions
from azure.storage.blob import BlobServiceClient
from logging import StreamHandler, Formatter


class LogHandler(StreamHandler):
    """LogHandler class manages log formatting and log output

    Args:
        StreamHandler (class): Class to set logs output
    """

    def __init__(self) -> None:
        output_type = os.getenv("LOGGING_TYPE") or sys.stdout
        StreamHandler.__init__(self, output_type)
        output_format = "%(asctime)s [%(threadName)10s][%(module)10s][%(lineno)4s][%(levelname)8s] %(message)s"
        format_date = "%Y-%m-%dT%H:%M:%S%Z"
        formatter = Formatter(output_format, format_date)
        self.setFormatter(formatter)


class BlobStorage:
    """BlobStorage class provides an interface to communicate with Azure to get, put, delete blobs from Azure Blob Storage Container"""

    def __init__(self) -> None:
        # Azure Blob Storage related init
        conn_string = os.getenv("AZURE_BLOB_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_BLOB_CONTAINER_NAME")
        self.blob_name = os.getenv("AZURE_CONTAINER_BLOB_NAME")
        self.blob_svc_client = BlobServiceClient.from_connection_string(conn_string)

    def is_exists(self, name) -> bool:
        """Checks if object exists in blob storage container

        Args:
            name (str): Name of blob object

        Returns:
            bool: True if exists and False if not
        """
        blob_client = self.blob_svc_client.get_blob_client(
            container=self.container_name, blob=name
        )
        if blob_client.exists():
            return True
        return False

    def get_object(self, name) -> str:
        """Gets blob object and returs its content

        Args:
            name (str): blob object bane

        Returns:
            str: content of blob object
        """
        blob_client = self.blob_svc_client.get_blob_client(
            container=self.container_name, blob=name
        )
        data = blob_client.download_blob().readall().decode("utf-8")
        return data

    def put_object(self, name: str, data: any) -> None:
        """Uploads blob object into Azure Blob Storage Container

        Args:
            name (str): Blob name
            data (any): Blob data
        """
        blob_client = self.blob_svc_client.get_container_client(
            container=self.container_name
        )
        blob_client.upload_blob(name=name, data=data)

    def del_object(self, name: str) -> None:
        """Deletes blob from Azure Blob Storage Container

        Args:
            name (str): Blob name
        """
        blob_client = self.blob_svc_client.get_blob_client(
            container=self.container_name, blob=name
        )
        blob_client.delete_blob()


class Discovery(BlobStorage):
    def __init__(self) -> None:
        super().__init__()
        self.log = logging.getLogger("Discovery")
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(LogHandler())
        self.lock_file = "statelock/lock"
        self.leader_file = "cluster/leader"
        self.list_managers = "cluster/managers"

    def set_lock(self, ip: str) -> None:
        """Sets lock file in blob storage.
        Lock file used to secure

        Args:
            ip (str): _description_
        """
        self.log.info(f"Set lock by {ip} request")
        self.put_object(self.lock_file, ip)

    def state_exists(self) -> bool:
        """Simple check if lock file exists

        Returns:
            bool: True if exists and False if not
        """
        if self.is_exists(self.lock_file):
            return True
        return False

    def set_leader(self, ip: str, token: str) -> None:
        """Sets leader data

        Args:
            ip (str): IP address
            token (str): Manager join token
        """
        data = {"ip": ip, "token": token}
        self.put_object(self.leader_file, json.dumps(data))

    def get_leader(self) -> object:
        """Gets leader data

        Returns:
            object: dict that contains ip and token
        """
        try:
            data = self.get_object(self.leader_file)
            js = json.loads(data)
            return js
        except azure.core.exceptions.ResourceNotFoundError as exc:
            self.log.error(exc)
            return {}

    def get_managers(self) -> None:
        raise NotImplementedError()

    def remove_lock(self) -> None:
        raise NotImplementedError()

    def register(self, ip: str) -> None:
        raise NotImplementedError()


class DockerSwarm(Discovery):
    """DockerSwarm singleton"""

    def __init__(self) -> None:
        super().__init__()
        # Setup logging
        self.log = logging.getLogger("DockerSwarm")
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(LogHandler())
        self.log.info("Docker Swarm Cluster manager initialized")
        try:
            # Docker client init
            self.docker_client = docker.from_env()

        except FileNotFoundError as exc:
            self.log.critical(exc)
            return
        except urllib3.exceptions.ProtocolError as exc:
            self.log.critical(exc)
            return
        except requests.exceptions.ConnectionError as exc:
            self.log.critical(exc)
            return
        except docker.errors.DockerException as exc:
            self.log.critical("Failed to connect to docker socket")
            self.log.critical(exc)
            return

    def join(self, token: str, address: str) -> None:
        """_summary_

        Args:
            token (str): _description_
            address (str): _description_
        """
        try:
            self.log.info("Attempting to join node to Docker Swarm cluster")
            is_conn = self.docker_client.api.join_swarm(address, token)
            if not is_conn:
                self.log.error("Failed to join node to Docker Swarm Cluster")
                return

        except docker.errors.APIError as exc:
            self.log.critical("Failed to send join request")
            self.log.critical(exc)
            return


class Manager(DockerSwarm):
    """_summary_

    Args:
        DockerSwarm (_type_): _description_
    """

    def __init__(self) -> None:
        super().__init__()
        self.address = None
        self.ip = os.getenv("HOST_IP")

    def init(self) -> bool:
        """_summary_

        Args:
            manager_address (str): _description_
        """
        try:
            if not self.state_exists():
                self.set_lock(self.ip)
                self.log.info("Attempting to init Docker Swarm Cluster")
                self.docker_client.api.init_swarm()
                token = self._get_token()
                self.set_leader(self.ip, token)
                return True
            self.log.info("Docker Swarm Cluster already set, nothing to init")
            return False
        except docker.errors.APIError as exc:
            self.log.critical(exc)
            return

    def update_state(self) -> None:
        raise NotImplementedError()

    def join_manager(self) -> None:
        raise NotImplementedError()

    def _get_token(self) -> str:
        """Gets Docker Swarm Join token for manager

        Returns:
            str: join token
        """
        res = None
        try:
            res = self.docker_client.api.inspect_swarm()
            manager_join_token = res.get("JoinTokens").get("Manager")
            self.log.info("Got docker swarm join token for manager")
            return manager_join_token
        except docker.errors.APIError as exc:
            self.log.critical("Failed to get join token")
            self.log.critical(exc)
            return

    def _check_address(self) -> None:
        raise NotImplementedError()


class Worker(DockerSwarm):
    """
    Worker class to manage Docker Swarm Workers in cluster
    """

    pass


if __name__ == "__main__":
    manager = Manager()
    res = manager.init()
    if not res:
        managers = manager.get_leader()
