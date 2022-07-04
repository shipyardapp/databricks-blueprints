"""

"""
import argparse
import requests
import shipyard_utils as shipyard
import helpers
import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--instance-id', dest='instance_id', required=True)
    parser.add_argument('--source-file-name', dest='source_file_name', required=True)
    parser.add_argument('--source-folder-name', dest='source_folder_name', required=True)
    args = parser.parse_args()
    return args


def delete_file_from_dbfs(client, file_path_and_name):
    delete_endpoint = "/dbfs/delete"
    data = {
        'path': file_path_and_name
    }
    print(f"Start delete for {file_path_and_name}")
    delete_response = client.post(delete_endpoint, data=data)
    if delete_response.status_code == requests.codes.ok:
        print(f"DBFS File: {file_path_and_name} delete function completed successfully")
    elif delete_response.status_code == 503: # Too many files left to delete
        delete_json = delete_response.json()
        error_code = delete_json['error_code']
        error_message = delete_json['message']
        if error_code == 'PARTIAL_DELETE':
            print(error_message)
        else: # This is a regular Service unavailable error
            print("File delete error: Service currently unavailable")
    else:
        print(f"File delete failed. Response: {delete_response.text}")
        

def main():
    args = get_args()
    access_token = args.access_token
    instance_id = args.instance_id
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    # create client
    client = helpers.DatabricksClient(access_token, instance_id)
    # create delete file path
    file_path_and_name = shipyard.combine_folder_and_file_name(source_folder_name,
                                                               source_file_name)
    delete_file_from_dbfs(client, file_path_and_name)
    
    
if __name__ == "__main__":
    main()
