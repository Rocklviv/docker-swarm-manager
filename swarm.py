import os
import sys
import json
import time
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
        output_format = "%(asctime)s [%(threadName)10s][%(module)10s][%(lineno)4s]\
        [%(levelname)8s] %(message)s"
        format_date = "%Y-%m-%dT%H:%M:%S%Z"
        formatter = Formatter(output_format, format_date)
        self.setFormatter(formatter)


class BlobStorage:
    """
    BlobStorage class provides an interface to communicate with Azure to get,
    put, delete blobs from Azure Blob Storage Container
    """

    def __init__(self) -> None:
        # Azure Blob Storage related init
        conn_string = os.getenv("AZURE_BLOB_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_BLOB_CONTAINER_NAME")
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

    def list_objects(self, path: str) -> list:
        """Returns liss of blob objects based on name prefix

        Args:
            path (str): Blob path or name prefix

        Returns:
            list: List of blob objects
        """
        blob_client = self.blob_svc_client.get_container_client(
            container=self.container_name
        )
        list_objects = blob_client.list_blobs(path)
        return list_objects


class Discovery(BlobStorage):
    """Discovery class controls discovery process to set cluster leader and
    register nodes

    Args:
        BlobStorage (class): BlobStorage class
    """

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
        self.log.info("Set lock by %s request", ip)
        self.put_object(self.lock_file, ip)

    def state_exists(self) -> bool:
        """Simple check if lock file exists

        Returns:
            bool: True if exists and False if not
        """
        if self.is_exists(self.lock_file):
            return True
        return False

    def set_leader(self, ip: str, manager_token: str, worker_token: str) -> None:
        """Sets leader data

        Args:
            ip (str): IP address
            manager_token (str): Manager join token
            worker_token (str): Worker join token
        """
        data = {"ip": ip, "manager_token": manager_token, "worker_token": worker_token}
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
            return None

    def get_managers(self) -> list:
        """Gets list of registered managers

        Returns:
            list: list registered IP's
        """
        try:
            list_managers = []
            data = self.list_objects(self.list_managers)
            for k in data:
                list_managers.append(
                    k.get("name").replace(self.list_managers + "/", "")
                )
            return list_managers
        except azure.core.exceptions.ResourceNotFoundError as exc:
            self.log.error(exc)
            return None

    def remove_lock(self) -> None:
        """Removes state lock from object storage"""
        try:
            self.del_object(self.lock_file)
        except azure.core.exceptions.HttpResponseError as exc:
            self.log.critical(exc)
            return

    def register(self, ip: str) -> None:
        """Register manager node as available manager to join

        Args:
            ip (str): IP of manager node
        """
        try:
            self.put_object(f"{self.list_managers}/{ip}", ip)
        except azure.core.exceptions.HttpResponseError as exc:
            self.log.critical(exc)
            return


class DockerSwarm(Discovery):
    """DockerSwarm implements Docker Client interface

    Args:
        Discovery (class): Discovery Class
    """

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
        """Join node to Docker Swarm Cluster using Leader/Manager IP and token

        Args:
            token (str): Docker Swarm join token
            address (str): Docker Swarm Leader/Manager IP
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
    """Manager class manages Docker Swarm Managers.
    This class init cluster, join managers

    Args:
        DockerSwarm (class): Base class that implements Docker Client
    """

    def __init__(self) -> None:
        super().__init__()
        self.address = None
        self.ip = os.getenv("HOST_IP")

    def init(self) -> bool:
        """Initialize Docker Swarm Cluster

        Returns:
            bool: True if cluster initialized, False if already inited
        """
        try:
            if not self.state_exists():
                self.set_lock(self.ip)
                self.log.info("Attempting to init Docker Swarm Cluster")
                self.docker_client.api.init_swarm(advertise_addr=self.ip)
                manager_token, worker_token = self._get_tokens()
                self.set_leader(self.ip, manager_token, worker_token)
            return False
        except docker.errors.APIError as exc:
            self.log.critical(exc)
            self.remove_lock()
            return False

    def check_if_leader_exists(self) -> bool:
        """Check if leader exists

        Returns:
            bool: True if leader exists and False if not
        """
        return bool(self.get_leader())

    def check_if_member(self) -> bool:
        """Checks if manager IP is part of cluster or leader

        Returns:
            bool: Returns True if exists and False when not
        """
        leader = self.get_leader()
        if leader:
            for _, v in leader.items():
                if self.ip in v:
                    self.log.info("Manager with IP %s is cluster leader", self.ip)
                    return True
        members = self.get_managers()
        self.log.debug("List of registered members: %s", members)
        if self.ip in members:
            self.log.info("Node %s already is part of cluster", self.ip)
            return True
        return False

    def join_manager(self) -> None:
        """Sends request to join manager to existing Docker Cluster"""
        leader = self.get_leader()
        try:
            if not leader:
                self.log.error(
                    "Failed to find leader data."
                    + "Lock might be set on unexisting cluster"
                )
                return None
            self.log.info("Attempting to join cluster as manager")
            self.docker_client.api.join_swarm(
                [leader.get("ip")], leader.get("manager_token")
            )
            self.log.info("Joined Docker Swarm Cluster")
            self.register(self.ip)
            self.log.info("%s registered as Docker Swarm Cluster manager", self.ip)
            return None
        except docker.errors.APIError as exc:
            self.log.critical("Failed to join as manager to cluster")
            self.log.critical(exc)
            return None
        except AttributeError as exc:
            self.log.error(
                "Docker Swarm Cluster is not inited, but lock file might be exists"
            )
            self.log.error(exc)
            return None

    def _get_tokens(self) -> (str, str):
        """Gets Docker Swarm Join token for manager

        Returns:
            str: manager join token
            str: worker join token
        """
        res = None
        try:
            res = self.docker_client.api.inspect_swarm()
            manager_join_token = res.get("JoinTokens").get("Manager")
            worker_join_token = res.get("JoinTokens").get("Worker")
            self.log.info("Got docker swarm join tokens for managers and workers")
            return manager_join_token, worker_join_token
        except docker.errors.APIError as exc:
            self.log.critical("Failed to get join token")
            self.log.critical(exc)
            return None


class Worker(DockerSwarm):
    """Worker class to manage Docker Swarm Workers in cluster"""

    def __init__(self) -> None:
        super().__init__()
        self.ip = os.getenv("HOST_IP")
        self.retry = 0

    def join_worker(self) -> None:
        """Join Worker node to Docker Swarm Cluster"""
        try:
            leader = self.get_leader()

            if leader and self.retry <= 0:
                self.log.info(
                    "Attempting to join Worker %s to Docker \
                            Swarm Cluster with leader %s",
                    self.ip,
                    leader.get("ip"),
                )
                self.docker_client.api.join_swarm(
                    [leader.get("ip")], leader.get("worker_token")
                )
            else:
                self.retry += 1
                self.log.info("No leader(s) were found. Retry in 30 seconds")
                time.sleep(30)
                self.join_worker()
        except docker.errors.APIError as exc:
            self.log.critical("Worker %s failed to join Docker Swarm Cluster", self.ip)
            self.log.critical(exc)
            return


def exception_factory(exception, exception_message):
    """Function takes an exception class and an error message as arguments

    Args:
        exception (class): Exception Class
        exception_message (str): Error Message to return

    Returns:
        class: Exception class
    """
    return exception(exception_message)


if __name__ == "__main__":
    role = os.getenv("ROLE")
    if role == "manager":
        manager = Manager()
        is_member = manager.check_if_member()
        if manager.check_if_leader_exists() and not is_member:
            manager.join_manager()
        else:
            manager.init()
    elif role == "worker":
        worker = Worker()
        worker.join_worker()
    else:
        raise exception_factory(ValueError, "Environment variable ROLE should be set")
