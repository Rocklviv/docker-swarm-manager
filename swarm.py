import os
import sys
import docker
import logging
import urllib3
import requests
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


class DockerSwarm:
    """DockerSwarm singleton"""

    def __init__(self) -> None:
        # Azure Blob Storage related init
        conn_string = os.getenv("AZURE_BLOB_CONNECTION_STRING")
        container_name = os.getenv("AZURE_BLOB_CONTAINER_NAME")
        self.blob_name = os.getenv("AZURE_CONTAINER_BLOB_NAME")
        blob_svc_client = BlobServiceClient.from_connection_string(conn_string)
        self.blob_client = blob_svc_client.get_blob_client(
            container=container_name, blob=self.blob_name
        )
        # Setup logging
        self.log = logging.getLogger("docker_swarm_helper")
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

    def check_state(self) -> bool:
        """_summary_

        Returns:
            bool: _description_
        """
        self.log.info("Check if state exists")
        if self.blob_client.exists:
            content = self.blob_client.download_blob(self.blob_name).readall()
            if content:
                self.log.debug(content)
                return True
            return True
        return False

    def join(self, token: str, address: str) -> None:
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

    def init(self) -> None:
        """_summary_"""
        try:
            if not self.check_state:
                self.docker_client.api.init_swarm()
                self._get_token()
        except docker.errors.APIError as exc:
            self.log.critical(exc)
            return

    def _get_token(self) -> str:
        """Gets Docker Swarm Join token for manager

        Returns:
            str: join token
        """
        try:
            res = self.docker_client.api.inspect_swarm()
            manager_join_token = res.get("JoinTokens").get("Manager")
            self.log.info("Got docker swarm join token for manager")
            return manager_join_token
        except docker.errors.APIError as exc:
            self.log.critical("Failed to get join token")
            self.log.critical(exc)
            return

    def join(self) -> None:
        pass

    def update_state(self) -> None:
        pass


class Worker(DockerSwarm):
    """
    Worker class to manage Docker Swarm Workers in cluster
    """

    pass


if __name__ == "__main__":
    manager = Manager()
    manager.init()
