"""
Databricks - Upload File to DBFS

User Inputs:
Authorization
Cluster ID
Source File Name Match Type
Source File Name
Source Folder Name
Destination File Name
Destination Folder Name
CLI option to spin down cluster after run

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
import sys
try:
	import helpers
	import errors
except BaseException:
	from . import helpers
	from . import errors



def upload_file_to_dbfs(client, cluser_id, source_file_name, 
	source_folder_name, dest_file_name, dest_folder_name):
	""" Upload file to dbfs using chunking"""