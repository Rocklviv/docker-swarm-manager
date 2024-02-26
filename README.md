# Docker Swarm Cluster automation script

[![DeepSource](https://app.deepsource.com/gh/Rocklviv/docker-swarm-manager.svg/?label=active+issues&show_trend=true&token=JULlUCXgAt7VBaEAew6Dj6Va)](https://app.deepsource.com/gh/Rocklviv/docker-swarm-manager/?ref=repository-badge)

Docker Swarm Cluster automation - is Python based script that automates creation of Docker Swarm Cluster.
Script provides a possibility to create Docker Swarm cluster and provides a mechanism to automatically add new nodes as managers and workers.
Automation makes sure that Docker Swarm cluster is not inited twice, stores some sort of state lock and saves cluster information on Blob Storage.

## Prerequisites

Swarm Cluster automation script requires to have a Ptyhon3 installation available on system.

Requirements:

- Python3
- Pip
- Python packages
  - azure-storage-blob
  - docker

## Usage

Python script uses a Azure Blob Storage as backend so it requires to have a Azure Blob Storage Connection string set.
Script requires next Envrionment variables:

- `AZURE_BLOB_CONNECTION_STRING` - Connection string to access Azure Blob Storage
- `AZURE_BLOB_CONTAINER_NAME` - Container name in Blob Storage that will hold state file and cluster information
- `HOST_IP` - IP Address of node where script is running
- `ROLE` - Node role. Accept `manager` and `worker`

Make sure script is running in user-data so that any Autoscalling process will be handled in right way and nodes will be added to Docker Swarm Cluster.

## Versions
- `0.1.2` - Fixed issue with retry mechanism for Docker Swarm Workers
- `0.1.1` - Added support for Python 3.10
- `0.1.0` - Initial release that handles Docker Swarm creation with locks, cluster info saved on Blob Storage and join mechanism for managers and workers
