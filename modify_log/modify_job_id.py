import re
import fileinput
import os
import json
import requests

log_directory = "/home/ankeruser/aws/aws_workspace_config/try1/"
def translate_job_id_toazure(log_dir,key_value_pairs):      
    logs_to_update = ["jobs.log"]
      # log files to adapt instance pools attributes
    for logfile in logs_to_update:
        with fileinput.FileInput(log_dir + logfile, inplace=True) as fp:
            for line in fp:
                for old, new in key_value_pairs.items():
                    line = line.replace(old, new)
                # update log file with new values
                print(line, end="")     

key_value_pairs = {}
file_path = "/home/ankeruser/aws/aws_workspace_config/try1/job_id_map.log"
with open(file_path, 'r') as file:
    json_objects = file.read().strip().split('\n')
    for json_str in json_objects:
        try:
            json_data = json.loads(json_str)
            old_id = json_data.get('old_id')
            _newid = json_data.get('new_id')
            key_value_pairs[old_id]=  _newid
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

print(key_value_pairs)    
translate_job_id_toazure(log_directory,key_value_pairs)
 