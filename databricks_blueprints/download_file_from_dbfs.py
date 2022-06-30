"""
Databricks - Download File from DBFS
User Inputs:

Authorization
Source File Name Match Type
Source File Name
Source Folder Name
Destination File Name
Destination Folder Name
CLI option to spin down cluster after run

Requirements:

Automatically tries to start the cluster if it's not already active
If destination file name is blank, use the original file name
If destination file name has a value and regex was selected, enumerate the file name.
If source folder name is blank, use /FileStore/. (https://docs.databricks.com/data/filestore.html)
If destination folder name is blank, use cwd.
Use the method prescribed by Databricks to chunk the download.
"""