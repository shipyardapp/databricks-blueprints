""" Databricks - Terminate Cluster

User Inputs:
- Authentication
- Cluster ID

- terminates a single cluster.
"""
import argparse
from helpers import DatabricksClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--instance-id', dest='instance_id', required=True)
    parser.add_argument('--cluster-id', dest='cluster_id', required=True)
    args = parser.parse_args()
    return args


def terminate_cluster(client, cluster_id):
    """
    Terminates a Databricks cluster given its ID.
    see: https://docs.databricks.com/dev-tools/api/latest/clusters.html#delete-terminate
    """
    terminate_cluster_endpoint = "/clusters/delete"
    payload = {"cluster_id": cluster_id}
    client.post(terminate_cluster_endpoint,
                           data=payload)
    print(f"Cluster termination for id: {cluster_id} in progress...")
    

def main():
    args = get_args()
    token = args.access_token
    instance_id = args.instance_id
    cluster_id = args.cluster_id
    # initialize databricks client
    client = DatabricksClient(token, instance_id)
    # run terminate cluster
    terminate_cluster(client, cluster_id)
    