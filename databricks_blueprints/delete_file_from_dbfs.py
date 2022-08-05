import argparse
import sys
import requests
import re
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
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        required=True)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='/FileStore/',
        required=False)
    parser.add_argument(
        '--source-file-name-match-type',
        dest='source_file_name_match_type',
        choices={
            'exact_match',
            'regex_match'},
        required=True)
    args = parser.parse_args()
    return args


def check_file_status(client, file_path_and_name):
    """ Check if file exists and get the status """
    check_endpoint = '/dbfs/get-status'
    params = {
        'path': file_path_and_name
    }
    response = client.get(check_endpoint, params=params)
    status_code = response.status_code
    if status_code == requests.codes.ok:  # File found
        print(f"file {file_path_and_name} still in DBFS")
    elif status_code == 404:  # File not found
        print(f"file {file_path_and_name} is no longer in DBFS")
    else:
        print(
            f"Unknown error whilst trying to find file: {file_path_and_name}",
            f"error: {response.text}")


def delete_file_from_dbfs(client, file_path_and_name):
    delete_endpoint = "/dbfs/delete"
    data = {
        'path': file_path_and_name
    }
    print(f"Start delete for {file_path_and_name}")
    try:
        delete_response = client.post(delete_endpoint, data=data)
        print(delete_response.text)
    except Exception as e:
        # Check if incorrect url provided first
        if 'nodename nor servname provided' in str(e):
            print(
                "Invalid or wrong databricks instance url provided. Please check and try again")
            sys.exit(errors.EXIT_CODE_INVALID_INSTANCE)
        else:
            print(f"Error occurred when trying move request: {e}")
            sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    if delete_response.status_code == requests.codes.ok:
        print(f"DBFS File: {file_path_and_name} delete function started...")
        check_file_status(client, file_path_and_name)
    elif delete_response.status_code == 503:  # Too many files left to delete
        delete_json = delete_response.json()
        error_code = delete_json['error_code']
        error_message = delete_json['message']
        if error_code == 'PARTIAL_DELETE':
            print(error_message)
            sys.exit(errors.EXIT_CODE_DBFS_FILE_OVERFLOW)
        else:  # This is a regular Service unavailable error
            print("File delete error: Service currently unavailable")
            sys.exit(errors.EXIT_CODE_DBFS_SERVICE_UNAVAILABLE)
    else:
        print(f"File delete failed. Response: {delete_response.text}")
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)


def main():
    args = get_args()
    access_token = args.access_token
    instance_url = args.instance_url
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_file_name_match_type = args.source_file_name_match_type
    source_folder_name = '/' + \
        shipyard.files.clean_folder_name(source_folder_name) + '/'

    # create client
    client = databricks_client.DatabricksClient(access_token, instance_url)
    # get list of all potential file matches
    if source_file_name_match_type == 'regex_match':
        files = databricks_client.list_dbfs_files(client, source_folder_name)
        matching_file_names = shipyard.files.find_all_file_matches(
            files, re.compile(source_file_name))
        num_matches = len(matching_file_names)
        if num_matches == 0:
            print(f"No files matching the regex: {source_file_name}")
            sys.exit()
        print(f'{num_matches} files found. Preparing to delete...')
        # create delete file path
        for file_path_and_name in matching_file_names:
            delete_file_from_dbfs(client, file_path_and_name)
    else:
        file_path_and_name = shipyard.files.combine_folder_and_file_name(
            source_folder_name, source_file_name)
        delete_file_from_dbfs(client, file_path_and_name)


if __name__ == "__main__":
    main()
