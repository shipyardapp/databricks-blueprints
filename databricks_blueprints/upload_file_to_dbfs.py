import sys
import argparse
import base64
import os
import re
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
    parser.add_argument('--source-file-name-match-type', dest='source_file_name_match_type',
                        choices={'exact_match', 'regex_match'}, required=True)
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
    if handle_response.status_code == requests.codes.ok:
        handle = handle_response.json()['handle']
        print(handle)
        # upload file
        with open(local_file_path, 'rb') as file:
            while True:
                # A block can be at most 1MB
                block = file.read(1 << 20)
                if not block:
                    break
                data = base64.standard_b64encode(block)
                data = data.decode('utf-8')
                block_resp = client.stream('/dbfs/add-block', 
                                           json={"handle": handle, "data": data}
                                           )
                if block_resp != requests.codes.ok:
                    print(f"adding block of {len(data)/1024}KB failed")
            print(f"finished uploading file:{local_file_path} to {dest_file_path}")
        # close the handle to finish uploading
        client.post("/dbfs/close", data={"handle": handle})
        print("file stream closed")
    else: # encountered an error
        error_code = handle_response.json()['error_code']
        message = handle_response.json()['message']
        print(f"Error uploading file: {error_code} - {message}")
        sys.exit(errors.EXIT_CODE_BAD_REQUEST)


def main():
    args = get_args()
    access_token = args.access_token
    instance_id = args.instance_id
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    dest_file_name = args.dest_file_name
    dest_folder_name = args.dest_folder_name
    source_file_name_match_type = args.source_file_name_match_type
    # create client
    client = helpers.DatabricksClient(access_token, instance_id)
    # create file paths
    if not source_folder_name:
        source_folder_name = os.getcwd()
    source_file_path = shipyard.files.combine_folder_and_file_name(
        source_folder_name,
        source_file_name
    )
    if not dest_file_name:
        dest_file_name = source_file_name
    if not dest_folder_name:
        dest_folder_name = '/FileStore/'

    if source_file_name_match_type == 'regex_match':
        files = helpers.list_dbfs_files(client, source_folder_name)
        matching_file_names = shipyard.files.find_all_file_matches(files,
                                            re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing upload...')
        for index, file_name in enumerate(matching_file_names):
            source_file_path = shipyard.files.combine_folder_and_file_name(
                            source_folder_name, file_name)
            dest_file_path = shipyard.files.combine_folder_and_file_name(
                            dest_folder_name, file_name)
            upload_file_to_dbfs(client, source_file_path, dest_file_path)
    else:
        destination_file_path = shipyard.files.combine_folder_and_file_name(
                                    dest_folder_name, dest_file_name)
        upload_file_to_dbfs(client, source_file_path, destination_file_path)


if __name__ == "__main__":
    main()