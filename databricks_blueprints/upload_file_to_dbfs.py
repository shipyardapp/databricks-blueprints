"""
Requirements:
Automatically tries to start the cluster if it's not already active.
If destination file name is blank, use the original file name
If destination file name has a value and regex was selected, enumerate the file name.
If destination folder name is blank, use /FileStore/. (https://docs.databricks.com/data/filestore.html)
If local folder name is blank, use cwd.
Must create the appropriate directory structure in Databricks if it doesn't exist.
Use the method prescribed by Databricks to chunk the upload.

"""
import argparse
import base64
import os
import requests
import shipyard_utils as shipyard
try:
    import helpers
    import errors
except BaseException:
    from . import helpers
    from . import errors
    

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--instance-id', dest='instance_id', required=True)
    parser.add_argument('--local-file-name', dest='local_file_name', required=True)
    parser.add_argument('--local-folder-name', dest='local_folder_name', required=False)
    parser.add_argument('--dest-file-name', dest='dest_file_name', required=False)
    parser.add_argument('--dest-folder-name', dest='dest_folder_name', required=False)
    args = parser.parse_args()
    return args


def upload_file_to_dbfs(client, local_file_path, dest_file_path):
    # create handle for stream upload chunking (blocks)
    create_handle_endpoint = "/dbfs/create"
    payload = {
        "path": dest_file_path,
        "overwrite": "true"
    }
    handle_response = client.post(create_handle_endpoint, data=payload)
    handle = handle_response.json()['handle']
    # upload file
    with open(local_file_path) as file:
        while True:
            # A block can be at most 1MB
            block = file.read(1 << 20)
            if not block:
                break
            if type(block) == 'str':
                block = bytes(block, 'utf-8')
            data = base64.standard_b64encode(block)
            client.stream('/dbfs/add-block', json={"handle": handle, "data": data})
        print(f"finished uploading file:{local_file_path} to {dest_file_path}")
    # close the handle to finish uploading
    client.stream("/dbfs/close", {"handle": handle})
    print("file stream supposedly closed")


def main():
    args = get_args()
    access_token = args.access_token
    instance_id = args.instance_id
    local_file_name = args.local_file_name
    local_folder_name = args.local_folder_name
    dest_file_name = args.dest_file_name
    dest_folder_name = args.dest_folder_name
    # create client
    client = helpers.DatabricksClient(access_token, instance_id)
    # create file paths
    if not local_folder_name:
        local_folder_name = os.getcwd()
    local_file_path = shipyard.files.combine_folder_and_file_name(
        local_folder_name,
        local_file_name
    )
    if not dest_file_name:
        dest_file_name = local_file_name
    if not dest_folder_name:
        dest_folder_name = '/FileStore/'
    destination_file_path = shipyard.files.combine_folder_and_file_name(
        dest_folder_name,
        dest_file_name
    )
    upload_file_to_dbfs(client, local_file_path, destination_file_path)


if __name__ == "__main__":
    main()