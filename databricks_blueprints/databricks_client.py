import sys
import requests
from pathlib import Path
import shipyard_utils as shipyard
try:
    import errors
except BaseException:
    from . import errors

# Create Artifacts folders
base_folder_name = shipyard.logs.determine_base_artifact_folder(
    'databricks')
artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
    base_folder_name)
shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)


class DatabricksClient(object):
    "Databricks connection object for use in running queries"

    def __init__(self, token, instance_url):
        self.token = token
        self.base_url = f"https://{instance_url}/api/2.0"
        self.request = requests.Session() # add HTTP persistence

    def get_headers(self):
        return {
            'Authorization': f"Bearer {self.token}",
            'Content-Type': 'application/json'
        }

    def get(self, endpoint, params={}):
        api_headers = self.get_headers()
        endpoint_url = self.base_url + endpoint
        try:
            response = self.request.get(endpoint_url,
                                    headers=api_headers,
                                    params=params)
            if response.status_code == 403:  # invalid account token
                print(f"Invalid Access Token: {self.token}")
                sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)
        except Exception as e:
            print(f"Error sending request to {endpoint}: {e}")
            sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
        return response

    def post(self, endpoint, data={}):
        api_headers = self.get_headers()
        endpoint_url = self.base_url + endpoint
        try:
            response = self.request.post(endpoint_url,
                                     headers=api_headers,
                                     json=data)
            if response.status_code == 403:  # invalid account token
                print(f"Invalid Access Token: {self.token}")
                sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)
        except Exception as e:
            print(f"Error sending request to {endpoint}: {e}")
            sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
        return response

    def stream(self, endpoint, json={}):
        api_headers = self.get_headers()
        endpoint_url = self.base_url + endpoint
        try:
            response = self.request.post(endpoint_url,
                                     headers=api_headers,
                                     json=json,
                                     stream=True)
        except Exception as e:
            print(f"Error sending request to {endpoint}: {e}")
            sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
        if response.status_code == 403:  # invalid account token
            print(f"Invalid Access Token: {self.token}")
            sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)
        return response


def start_cluster(token, instance_id, cluster_id):
    """
    Starts a Databricks Cluster and saves the status in the artifacts folder.
    Ideally this function should be run first whenever someone tries to
    interact with Databricks using our CLI applications.
    see: https://docs.databricks.com/dev-tools/api/latest/clusters.html#start
    """
    databricks_client = DatabricksClient(token, instance_id)
    start_cluster_endpoint = "/clusters/start"
    payload = {"cluster_id": cluster_id}
    start_response = databricks_client.post(
        start_cluster_endpoint, data=payload)
    # save response to shipyard logs
    response_file_name = shipyard.files.combine_folder_and_file_name(
        artifact_subfolder_paths['responses'],
        f'cluster_start_{cluster_id}_response.json')
    shipyard.files.write_json_to_file(
        start_response.json(), response_file_name)

    # handle response data
    if start_response.status_code == requests.codes.ok:
        print(f"Cluster: {cluster_id} started successfully")
    elif start_response.status_code == 400:  # Cluster in RESTARTING state
        if "unexpected state Running" in start_response.json()['message']:
            print(f"Cluster: {cluster_id} is already running")
            sys.exit(errors.EXIT_CODE_CLUSTER_STATUS_RUNNING)
        elif "unexpected state Restarting" in start_response.json()['message']:
            print(
                f"Cannot start Cluster: {cluster_id}. Cluster is currently RESTARTING")
            sys.exit(errors.EXIT_CODE_CLUSTER_STATUS_RESTARTING)
        elif "unexpected state Terminating" in start_response.json()['message']:
            print(
                f"Cannot start Cluster: {cluster_id}. Cluster is currently RESTARTING")
            sys.exit(errors.EXIT_CODE_CLUSTER_STATUS_TERMINATING)
        else:
            print(f"Failed to start Cluster: {cluster_id}",
                  f"HTTP Status code: {start_response.status_code} ",
                  f"and Response: {start_response.text}")
            sys.exit(errors.EXIT_CODE_CLUSTER_STATUS_ERRORED)
    else:
        print(f"Failed to start Cluster: {cluster_id}",
              f"HTTP Status code: {start_response.status_code} ",
              f"and Response: {start_response.text}")
        sys.exit(errors.EXIT_CODE_CLUSTER_STATUS_ERRORED)


def list_dbfs_files(client, folder_path):
    """ retrieves a list of all the DBFS files with the names intact"""
    params = {"path": folder_path}
    response = client.get('/dbfs/list', params=params)
    if response.status_code == 200:
        files = response.json()
    else:
        print(f"error: {response.status_code}")
    # get all the files within the stated base path
    base_dir = files
    file_list = []
    # loop through the base path and get retrieve all the folders
    if base_dir
        for file in base_dir['files']:
            if file['is_dir'] == True:
                file_list += list_dbfs_files(client, file['path'])
            else:
                file_list.append(file['path'])
    return file_list