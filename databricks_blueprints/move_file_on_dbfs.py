import argparse
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
    parser.add_argument('--source-folder-name', dest='source_folder_name', required=True)
    parser.add_argument('--dest-file-name', dest='dest_file_name', required=True)
    parser.add_argument('--dest-folder-name', dest='dest_folder_name', required=True)
    args = parser.parse_args()
    return args


def dbfs_move_file(client, source_file_path, destination_file_path):
    move_endpoint = "/dbfs/move"
    payload = {
        "source_path": source_file_path, 
        "destination_path": destination_file_path
        }
    move_response = client.post(move_endpoint, data=payload)
    if move_response.status_code == requests.codes.ok:
        print(f"File: {source_file_path} moved successfully to {destination_file_path}")
    elif move_response.status_code == 401:
        print("File: {source_file_path} does not exist")
    else:
        print(f"DBFS {source_file_path} to {destination_file_path} failed.",
              "response: {move_response.text}")


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
    source_file_path = shipyard.combine_folder_and_file_name(
        source_folder_name,
        source_file_name
    )
    destination_file_path = shipyard.combine_folder_and_file_name(
        dest_folder_name,
        dest_file_name
    )
    dbfs_move_file(client, source_file_path, destination_file_path)


if __name__ == "__main__":
    main()