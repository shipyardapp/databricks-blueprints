import sys
import argparse
import base64
import os
import re
import requests
import shipyard_utils as shipyard
try:
    import databricks_client
    import errors
except BaseException:
    from . import databricks_client
    from . import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--instance-url', dest='instance_url', required=True)
    parser.add_argument('--source-file-name',
                        dest='source_file_name',
                        required=True)
    parser.add_argument('--source-folder-name',
                        dest='source_folder_name',
                        default='',
                        required=False)
    parser.add_argument('--dest-file-name',
                        dest='dest_file_name',
                        required=False)
    parser.add_argument('--dest-folder-name',
                        dest='dest_folder_name',
                        default='/FileStore/',
                        required=False)
    parser.add_argument('--source-file-name-match-type',
                        dest='source_file_name_match_type',
                        choices={'exact_match', 'regex_match'},
                        required=True)
    args = parser.parse_args()
    return args


def upload_file_to_dbfs(client, local_file_path, dest_file_path):
    # create handle for stream upload chunking (blocks)
    create_handle_endpoint = "/dbfs/create"
    payload = {"path": dest_file_path, "overwrite": "true"}
    try:
        handle_response = client.post(create_handle_endpoint, data=payload)
    except Exception as e:
        if 'nodename nor servname provided' in str(e):
            print(
                "Invalid or wrong databricks instance url provided. Please check and try again"
            )
            sys.exit(errors.EXIT_CODE_INVALID_INSTANCE)
        else:
            print(f"Error occurred when trying move request: {e}")
            sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    if handle_response.status_code == requests.codes.ok:
        handle = handle_response.json()['handle']
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
                                           json={
                                               "handle": handle,
                                               "data": data
                                           })
                if block_resp.status_code != requests.codes.ok:
                    print(f"adding block of {len(data)/1024}KB failed")
            print(
                f"finished uploading file:{local_file_path} to {dest_file_path}"
            )
        # close the handle to finish uploading
        client.post("/dbfs/close", data={"handle": handle})
        print("file stream closed")
    else:  # encountered an error
        error_code = handle_response.json()['error_code']
        message = handle_response.json()['message']
        print(f"Error uploading file: {error_code} - {message}")
        sys.exit(errors.EXIT_CODE_BAD_REQUEST)


def main():
    args = get_args()
    print(args)
    access_token = args.access_token
    instance_url = args.instance_url
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    dest_file_name = args.dest_file_name
    dest_folder_name = args.dest_folder_name
    source_file_name_match_type = args.source_file_name_match_type
    # create client
    client = databricks_client.DatabricksClient(access_token, instance_url)
    # create file paths

    source_file_path = shipyard.files.combine_folder_and_file_name(
        source_folder_name, source_file_name)
    dest_folder_name = '/' + shipyard.files.clean_folder_name(
        dest_folder_name) + '/'

    if source_file_name_match_type == 'regex_match':
        all_local_files = shipyard.files.find_all_local_file_names(
            source_folder_name)
        matching_file_names = shipyard.files.find_all_file_matches(
            all_local_files, re.compile(source_file_name))
        for index, file_name in enumerate(matching_file_names):
            source_file_path = shipyard.files.combine_folder_and_file_name(
                source_folder_name, file_name)
            dest_file_path = shipyard.files.determine_destination_full_path(
                destination_folder_name=dest_folder_name,
                destination_file_name=args.dest_file_name,
                source_full_path=source_file_path,
                file_number=None if len(matching_file_names) == 1 else index + 1)
            upload_file_to_dbfs(client, source_file_path, dest_file_path)
    else:
        if not dest_file_name:
            dest_file_name = source_file_name
        destination_file_path = shipyard.files.combine_folder_and_file_name(
            dest_folder_name, dest_file_name)
        upload_file_to_dbfs(client, source_file_path, destination_file_path)


if __name__ == "__main__":
    main()
