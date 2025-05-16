import argparse
import configparser
import re
import fileinput
import os
import json
import requests

def get_login_credentials(creds_path='~/.databrickscfg', profile='DEFAULT'):
    config = configparser.ConfigParser()
    abs_creds_path = os.path.expanduser(creds_path)
    config.read(abs_creds_path)
    try:
        current_profile = dict(config[profile])
        if not current_profile:
            raise ValueError(f"Unable to find a defined profile to run this tool. Profile \'{profile}\' not found.")
        return current_profile
    except KeyError:
        raise ValueError(
            'Unable to find credentials to load for profile. Profile only supports tokens.')
# from modify_clustersize_attributes import *
parser = argparse.ArgumentParser(description='Run different functoin based on arguments.')

parser.add_argument('--profile_of_oldWS',required=True, action='store', default='DEFAULT',
                    help='Profile to parse the credentials')
parser.add_argument('--profile_of_newWS',required=True, action='store', default='DEFAULT',
                    help='Profile to parse the credentials')
parser.add_argument('--set-export-dir',required=True, action='store', default='DEFAULT',
                    help='set a export dir')

parser.add_argument('--session',required=True, action='store', default='DEFAULT',
                    help='Session')
args = parser.parse_args()
export_dir = args.set_export_dir
log_directory = os.path.join(export_dir, args.session)

old_ws_login_credentials = get_login_credentials(profile=args.profile_of_oldWS)
new_ws_login_credentials = get_login_credentials(profile=args.profile_of_newWS)
databricks_domain = old_ws_login_credentials['host']
access_token = old_ws_login_credentials['token']
_databricks_domain_target = new_ws_login_credentials['host']
_access_token_target = new_ws_login_credentials['token']

#print(_databricks_domain_target) 
# change attribute from aws to azure                
def translate_instance_pool_attributes_toazure(log_dir):
    # log files to adapt instance pools attributes
    logs_to_update = ["instance_pools.log", "jobs.log","clusters.log","cluster_policies.log"]
      # log files to adapt instance pools attributes
    for logfile in logs_to_update:
        with fileinput.FileInput(log_dir + logfile, inplace=True) as fp:
            for line in fp:
                line = line.replace("aws_attributes", "azure_attributes")
                # remove Azure suffix
                line = line.replace("_AWS", "")
                # adapt spot bid price configuration to AWS syntax with 
                # default value of 100%
                line = re.sub(
                    '"spot_bid_max_price": -?\d*\.?\d*',
                    '"spot_bid_price_percent": 100',
                    line,
                )
                # update log file with new values
                print(line, end="")      

def translate_cluster_id_toazure(log_dir,key_value_pairs):      
    logs_to_update = ["jobs.log"]
      # log files to adapt instance pools attributes
    for logfile in logs_to_update:
        with fileinput.FileInput(os.path.join(log_dir,logfile), inplace=True) as fp:
            for line in fp:
                for old, new in key_value_pairs.items():
                    line = line.replace(old, new)
                # update log file with new values
                print(line, end="")     
#get querynewid
def get_querynewid_byoldname(databricks_domain,access_token,oldname):
    api_version = "2.1"
    _databricks_domain = databricks_domain
    _access_token = access_token
    sqlwarehouse_newid =""
    headers = {
    "Authorization": f"Bearer {_access_token}",
    "Content-Type": "application/json"}
    init_flag = False
    # 构建 GET API请求URL
    url = f"{_databricks_domain}/api/{api_version}/clusters/list"
    while True:
        if init_flag:
            response = requests.get(url, headers=headers, params={'page_token':data.get('next_page_token')})
        else:
            response = requests.get(url, headers=headers)
        data = response.json()
        init_flag=True
        # 检查响应状态码
        if response.status_code == 200 and data.get('clusters') != '' and data.get('clusters') != None:
            contents = data["clusters"]
            for con in contents:
                if con['cluster_name'] == oldname:
                    sqlwarehouse_newid = con["cluster_id"]             
        else:
            print(f"Error: {response.status_code} - {response.text}")

        if data.get('next_page_token') == None or data.get('next_page_token') == '':
            break
    return sqlwarehouse_newid  

key_value_pairs = {}
file_path = os.path.join(log_directory,"clusters.log")
#print(_databricks_domain_target)
with open(file_path, 'r') as file:
    json_objects = file.read().strip().split('\n')
    for json_str in json_objects:
        try:
            json_data = json.loads(json_str)
            _oldname=""
            _newid =""
            _oldname =json_data.get('cluster_name',None)
            old_id = json_data.get('cluster_id')
            if _oldname is not None: 
                _newid=get_querynewid_byoldname(databricks_domain=_databricks_domain_target,access_token=_access_token_target,oldname=_oldname)     
                key_value_pairs[old_id]=  _newid
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
print(key_value_pairs)    
translate_cluster_id_toazure(log_directory,key_value_pairs)
 
