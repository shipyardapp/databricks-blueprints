""" Databricks - Terminate Cluster

User Inputs:
- Authentication
- Cluster ID

- terminates a single cluster.
"""
import argparse
import sys
import shipyard_utils as shipyard
try:
    import errors
    from helpers import DatabricksClient
except BaseException:
    from . import errors


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
    try:
        termination_response = client.post(terminate_cluster_endpoint,
                                           data=payload)
        base_folder_name = shipyard.logs.determine_base_artifact_folder(
            'databricks')
        artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
            base_folder_name)
        response_file_name = shipyard.files.combine_folder_and_file_name(
            artifact_subfolder_paths['responses'],
            f'cluster_termination_{cluster_id}_response.json')
        shipyard.files.write_json_to_file(
            termination_response.json(), response_file_name)
    except BaseException as e:
        print(
            f"Ran into an error while trying to terminate cluster {cluster_id}. Please check your instance ID and try again.")
        print(e)
        sys.exit(errors.EXIT_CODE_INVALID_INSTANCE)
    determine_status(termination_response, cluster_id)


def determine_status(termination_response, cluster_id):
    if termination_response.status_code == 200:
        print(f"Cluster termination for id: {cluster_id} has started...")
        sys.exit(errors.EXIT_CODE_TERMINATION_SUCCESSFULLY_STARTED)
    elif termination_response.status_code == 400:  # Cluster in RESTARTING state
        if "does not exist" in termination_response.json()[
                'message']:
            print(
                f"Cluster: {cluster_id} does not exist. Check for typos or that you have access to this cluster.")
            sys.exit(errors.EXIT_CODE_INVALID_CLUSTER)
        else:
            throw_generic_error(termination_response, cluster_id)
    elif termination_response.status_code == 403:
        if "Invalid access token" in termination_response.json()[
                'message']:
            print(
                f"The access key provided is not valid. Check for typos.")
            sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)
        else:
            throw_generic_error(termination_response, cluster_id)
    else:
        throw_generic_error(termination_response, cluster_id)


def throw_generic_error(termination_response, cluster_id):
    print(f"Failed to start Cluster: {cluster_id}",
          f"HTTP Status code: {termination_response.status_code} ",
          f"and Response: {termination_response.text}")
    sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)


def main():
    args = get_args()
    token = args.access_token
    instance_id = args.instance_id
    cluster_id = args.cluster_id
    # initialize databricks client
    client = DatabricksClient(token, instance_id)
    # run terminate cluster
    terminate_cluster(client, cluster_id)


if __name__ == "__main__":
    main()
