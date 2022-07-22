import argparse
import os
import sys
import base64
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
    parser.add_argument('--source-file-name', dest='source_file_name', required=True)
    parser.add_argument('--source-folder-name', dest='source_folder_name', required=False)
    parser.add_argument('--dest-file-name', dest='dest_file_name', required=False)
    parser.add_argument('--dest-folder-name', dest='dest_folder_name', required=False)
    args = parser.parse_args()
    return args


def download_file_from_dbfs(client, source_file_path, dest_file_path):
    read_handle_endpoint = "/dbfs/read"
    with open(dest_file_path, "w+") as file:
        offset = 0
        while True:
            payload = {
                "path": source_file_path,
                "offset": offset, 
                "length": 1024 # 1MB of data 
            }
            try:
                read_response = client.get(read_handle_endpoint, params=payload)
            except Exception as e:
                print(f'Connection Error: Failed to reach server at {read_handle_endpoint}')
                sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)

            if read_response.status_codes == requests.codes.ok:
                response_data = read_response.json()
                data = base64.b64decode(response_data['data'])
                file.write(data.decode('utf-8'))
                # check if there is still more data left to read
                if response_data['bytes_read'] == 1024:
                    offset += 1024
                else: # that was the last of the data in the file
                    break
            else:
                sys.exit(errors.EXIT_CODE_DBFS_READ_ERROR)
    print(f"finished downloading file:{source_file_path} as {dest_file_path}")
    

def main():
    args = get_args()
    access_token = args.access_token
    instance_id = args.instance_id
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    dest_file_name = args.dest_file_name
    dest_folder_name = args.dest_folder_name
    # create client
    client = helpers.DatabricksClient(access_token, instance_id)
    # create file paths
    if not source_folder_name:
        source_folder_name = '/FileStore/'
    source_file_path = shipyard.files.combine_folder_and_file_name(
        source_folder_name,
        source_file_name
    )
    if not dest_file_name:
        dest_file_name = source_file_name
    if not dest_folder_name:
        dest_folder_name = os.getcwd()
    destination_file_path = shipyard.files.combine_folder_and_file_name(
        dest_folder_name,
        dest_file_name
    )
    download_file_from_dbfs(client, source_file_path, destination_file_path)


if __name__ == "__main__":
    main()