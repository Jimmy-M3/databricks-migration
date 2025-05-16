import re
import fileinput
import os
import json
import requests

log_directory = "/home/ankeruser/aws/aws_workspace_config/try1/"  
#file_path = 'C:\\work\\log\\jobs.log'
databricks_domain = ""
access_token = ""
_databricks_domain_target = ""
_access_token_target = ""
script_dir = os.path.dirname(os.path.abspath(__file__))
#file_path = os.path.join(script_dir, 'config.txt')
file_path ="/home/ankeruser/aws/migration_api/config.txt"
with open(file_path, 'r') as file:
    # 读取所有行到一个列表中
    config_lines = file.readlines()
    for line in config_lines:
        parts = line.strip().split('=', 1)
        if len(parts) == 2:
            key, value = parts
            key = key.strip()
            value = value.strip()
            if key == 'databricks_domain':
                databricks_domain = value   
            if key == 'access_token':
                access_token = value
            if key == 'target_databricks_domain':
                _databricks_domain_target = value   
            if key == 'target_access_token':
                _access_token_target = value
                break  
    
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
        with fileinput.FileInput(log_dir + logfile, inplace=True) as fp:
            for line in fp:
                for old, new in key_value_pairs.items():
                    line = line.replace(old, new)
                # update log file with new values
                print(line, end="")     
#get 在azure中的pipeline的ID
def get_pipelinesnewid_byoldname(databricks_domain,access_token,oldname):
    api_version = "2.0"
    _databricks_domain = databricks_domain
    _access_token = access_token
    pipeline_newid =""
    headers = {
    "Authorization": f"Bearer {_access_token}",
    "Content-Type": "application/json"}
    init_flag = False
    # 构建 GET API请求URL
    url = f"{_databricks_domain}/api/{api_version}/pipelines"
    while True:
        if init_flag:
            response = requests.get(url, headers=headers, params={'page_token':data.get('next_page_token')})
        else:
            response = requests.get(url, headers=headers)
        data = response.json()
        init_flag=True
        # 检查响应状态码
        if response.status_code == 200 and data.get('statuses') != '' and data.get('statuses') != None:
            contents = data["statuses"]
            for con in contents:
                if con['name'] == oldname:
                    pipeline_newid = con["pipeline_id"]           
        else:
            print(f"Error: {response.status_code} - {response.text}")
        if data.get('next_page_token') == None or data.get('next_page_token') == '':
            break
    return pipeline_newid  
#get 在aws中的pipeline名称
def get_pipelinesname_byoldid(databricks_domain,access_token,oldid):
    api_version = "2.0"
    _databricks_domain = databricks_domain
    _access_token = access_token
    sqlwarehouse_oldname =""
    headers = {
    "Authorization": f"Bearer {_access_token}",
    "Content-Type": "application/json"}
    # 构建 GET API请求URL
    url = f"{_databricks_domain}/api/{api_version}/pipelines/{oldid}"
    response = requests.get(url, headers=headers)
    data = response.json()
    if response.status_code == 200 :
            sqlwarehouse_oldname=data['name']                  
    else:
         print(f"Error: {response.status_code} - {response.text}")  
    return sqlwarehouse_oldname  

# 
key_value_pairs = {}
file_path = log_directory + "jobs.log"
with open(file_path, 'r') as file:
    json_objects = file.read().strip().split('\n')
    for json_str in json_objects:
        try:
            json_data = json.loads(json_str)
            #print(json_data['settings']['pipeline_task']['pipeline_id'])
            _oldname=""
            _newid =""
            if 'settings' in json_data and 'tasks' in json_data['settings']:
                for task in json_data['settings']['tasks']:
                    if  task.get('pipeline_task') is not None:
                        if 'pipeline_id' in task.get('pipeline_task'):
                            warehouse_id = task.get('pipeline_task').get('pipeline_id')
                            if len(warehouse_id)>1:
                                _oldname=get_pipelinesname_byoldid(databricks_domain=databricks_domain,access_token=access_token,oldid=warehouse_id)
                                #print(_oldname)
                                _newid=get_pipelinesnewid_byoldname(databricks_domain=_databricks_domain_target,access_token=_access_token_target,oldname=_oldname)
                                #print(_newid)
                                key_value_pairs[warehouse_id] = _newid
            if 'settings' in json_data and 'pipeline_task' in json_data['settings'] and 'pipeline_id' in json_data['settings']['pipeline_task']:
                pipelines_id = json_data['settings']['pipeline_task']['pipeline_id']
                print(pipelines_id)
                if len(pipelines_id)>1:
                    _oldname=get_pipelinesname_byoldid(databricks_domain=databricks_domain,access_token=access_token,oldid=pipelines_id)
                    _newid=get_pipelinesnewid_byoldname(databricks_domain=_databricks_domain_target,access_token=_access_token_target,oldname=_oldname)
                key_value_pairs[pipelines_id] = _newid
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
print(key_value_pairs)    
translate_cluster_id_toazure(log_directory,key_value_pairs)
 
