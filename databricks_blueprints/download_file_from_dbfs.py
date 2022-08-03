import argparse
import os
import sys
import base64
import requests
import re
import shipyard_utils as shipyard
try:
    import databricks_client
    import errors
except BaseException:
    from . import databricks_client
    from . import errors

ONE_MB = 1024*1024


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--instance-url', dest='instance_url', required=True)
    parser.add_argument('--source-file-name', dest='source_file_name', required=True)
    parser.add_argument('--source-folder-name', dest='source_folder_name', required=False)
    parser.add_argument('--dest-file-name', dest='dest_file_name', required=False)
    parser.add_argument('--dest-folder-name', dest='dest_folder_name', required=False)
    parser.add_argument('--source-file-name-match-type', dest='source_file_name_match_type',
                        choices={'exact_match', 'regex_match'}, required=True)
    args = parser.parse_args()
    return args


def download_file_from_dbfs(client, source_file_path, dest_file_path):
    read_handle_endpoint = "/dbfs/read"
    # check if filetype works with binary or not
    if ('.csv' in source_file_path) or ('.txt' in source_file_path):
        is_binary = False
        write_mode = "w+"
    else:
        is_binary = True
        write_mode = 'wb+'
    with open(dest_file_path, write_mode) as file:
        offset = 0
        while True:
            payload = {
                "path": source_file_path,
                "offset": offset, 
                "length": ONE_MB # 1MB of data 
            }
            try:
                read_response = client.get(read_handle_endpoint, params=payload)
            except Exception:
                print(f'Connection Error for {read_handle_endpoint}')
                print(f"Data downloaded: {offset/1024}MB")
                sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)

            if read_response.status_code == requests.codes.ok:
                response_data = read_response.json()
                data = base64.b64decode(response_data['data'])
                if is_binary:
                    file.write(data)
                else:
                    file.write(data.decode('utf-8'))
                # check if there is still more data left to read
                if response_data['bytes_read'] == ONE_MB:
                    offset += ONE_MB
                else: # that was the last of the data in the file
                    break
            elif read_response.status_code == 404:
                print(f"Error: No file located at {source_file_path}")
                sys.exit(errors.EXIT_CODE_DBFS_INVALID_FILEPATH)
            elif read_response.status_code == 400: # Bad Request
                error = read_response.json()
                code = error['error_code']
                message = error['message']
                print(f"Error: {code} {message}")
                sys.exit(errors.EXIT_CODE_DBFS_READ_ERROR)
            else:
                print(f"Failed to read data: {read_response.status_code} {read_response.text}")
                sys.exit(errors.EXIT_CODE_DBFS_READ_ERROR)
    print(f"finished downloading file:{source_file_path} as {dest_file_path}")
    

def main():
    args = get_args()
    access_token = args.access_token
    instance_url = args.instance_url
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    dest_file_name = args.dest_file_name
    dest_folder_name = args.dest_folder_name
    source_file_name_match_type = args.source_file_name_match_type
    # create client
    client = databricks_client.DatabricksClient(access_token, instance_url)
    
    if not source_folder_name:
        source_folder_name = '/FileStore/'
    else:
        # get the cleaned file name
        source_folder_name = '/' + shipyard.files.clean_folder_name(source_folder_name) + '/'

    if not dest_file_name:
        dest_file_name = source_file_name

    if not dest_folder_name:
        dest_folder_name = os.getcwd()
        
    # get list of all potential file matches
    if source_file_name_match_type == 'regex_match':
        files = databricks_client.list_dbfs_files(client, source_folder_name)
        matching_file_names = shipyard.files.find_all_file_matches(files,
                                            re.compile(source_file_name))
        num_matches = len(matching_file_names)
        if num_matches == 0:
            print(f"No regex matches found for {source_file_name}")
            sys.exit(errors.EXIT_CODE_DBFS_INVALID_FILEPATH)
        print(f'{num_matches} files found. Preparing to download...')
        for index, file_name in enumerate(matching_file_names):
            source_file_path = file_name
            # create destination file name
            dest_file_name = shipyard.files.extract_file_name_from_source_full_path(file_name)
            dest_file_name = shipyard.files.enumerate_destination_file_name(dest_file_name, index)
            destination_file_path = shipyard.files.combine_folder_and_file_name(
                            dest_folder_name, dest_file_name)
            print(f'Downloading file {index+1} of {len(matching_file_names)}')
            try:
                download_file_from_dbfs(client, source_file_path, destination_file_path)
            except Exception as e:
                print(f'Failed to download {file_name}...{e}')
    # otherwise download the file normally
    else:
        source_file_path = shipyard.files.combine_folder_and_file_name(
                        source_folder_name, source_file_name)
        destination_file_path = shipyard.files.combine_folder_and_file_name(
                            dest_folder_name, dest_file_name)
        download_file_from_dbfs(client, source_file_path, destination_file_path)


if __name__ == "__main__":
    main()