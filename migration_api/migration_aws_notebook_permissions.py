import requests
import json
import pandas as pd
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
#file_path = os.path.join(script_dir, 'log\\acl_dashboard.log')
# 列出工作区中的所有笔记本
def list_notebooks(workspace_url,token,path="/"):
    url = f"{workspace_url}/api/2.0/workspace/list"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, params={"path": path})
    response.raise_for_status()
    return response.json().get("objects", [])

# 遍历工作区中的所有笔记本并读取内容
def read_all_notebooks(workspace_url,token,path="/"):
    notebooks = list_notebooks(workspace_url, token, path)
    notebook_contents = []
    for notebook in notebooks:
        if notebook["object_type"] == "NOTEBOOK":
            notebook_contents.append({"object_id": notebook["object_id"],"path": notebook["path"]})   
        elif notebook["object_type"] == "DIRECTORY":
            notebook_contents.extend(read_all_notebooks(workspace_url,token,notebook["path"]))
        
    return notebook_contents
    

     
def create_permissions(access_token_target, permissions_data,object_id,_databricks_domain_target):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    _api_version_target = "2.0"
    #update 目标权限
    _url_permission = f"{_databricks_domain_target}/api/{_api_version_target}/permissions/notebooks/{object_id}"
    if  "IS_OWNER" in json.dumps(permissions_data):
                    response_permission = requests.put(
                        _url_permission,
                        headers=headers,
                        json=json.loads(json.dumps(permissions_data).replace('anker.com','anker-in.com'))
                    )
    else:
                    response_permission = requests.patch(
                        _url_permission,
                        headers=headers,
                        json=json.loads(json.dumps(permissions_data).replace('anker.com','anker-in.com'))
                    )
    if response_permission.ok:
                    print("更新权限成功")
    else:
                    print("更新权限失败：", response_permission.status_code, response_permission.text)

     

#end 构造创建dlt函数
def call_migration_acl_notebook(target_host,target_token):
    _databricks_domain_target = target_host
    _access_token_target = target_token
    name_ids ={}
    file_path ='/home/ankeruser/aws/aws_workspace_config/try1/acl_notebooks.log'
    notebook_contents=read_all_notebooks(workspace_url=_databricks_domain_target,token=_access_token_target)
    for i in notebook_contents:
        name_ids[i.get('path')]= i.get('object_id')

    with open(file_path, encoding="utf-8") as txtfile:
        for line in txtfile:
            df_data = json.loads(line)  
            notebook_name =df_data.get('name')
            _notebook_id = name_ids.get(notebook_name)
            if _notebook_id is not None:
                access_control_list = df_data.get('access_control_list')
                # print(access_control_list)
                for item in access_control_list:
                    new_format = {
                    'access_control_list': []}
                    #user_name = '"user_name": {}'.format(item.get('user_name', ''))
                    if  item['all_permissions'][0]['inherited'] :
                        # 根据原始数据中的信息创建新格式的条目
                        if item.get('user_name', '') !='':
                            new_item = {   
                                "user_name": item.get('user_name', ''),
                                "permission_level": item['all_permissions'][0]['permission_level']
                            }
                            # 将新条目添加到新格式的列表中
                            new_format['access_control_list'].append(new_item)
                            create_permissions(access_token_target= _access_token_target, permissions_data=new_format,object_id=_notebook_id,_databricks_domain_target=_databricks_domain_target)
                            #create_permissions( warehouses_data=new_format,warehouse_name=_pipeline_name)
                        if item.get('group_name', '') !='':
                                groupname= f"{item.get('group_name', '')}"
                                new_item = {   
                                    "group_name": groupname,
                                    "permission_level": item['all_permissions'][0]['permission_level']
                                }
                                # 将新条目添加到新格式的列表中
                                new_format['access_control_list'].append(new_item) 
                                create_permissions(access_token_target= _access_token_target, permissions_data=new_format,object_id=_notebook_id,_databricks_domain_target=_databricks_domain_target)
                        if item.get('service_principal_name', '') !='':
                                new_item = {   
                                    "service_principal_name": item.get('service_principal_name', ''),
                                    "permission_level": item['all_permissions'][0]['permission_level']
                                }
                                # 将新条目添加到新格式的列表中
                                new_format['access_control_list'].append(new_item) 
                                create_permissions(access_token_target= _access_token_target, permissions_data=new_format,object_id=_notebook_id,_databricks_domain_target=_databricks_domain_target)
                        #txtfile.write("pipeline_name:"+object_id+","+str(new_format)+ '\n')      
# call_migration_acl_notebook(source_host="https://dbc-1c15bce2-47da.cloud.databricks.com",source_token="dapif68be4994c246f0cb09c54e1faa337dc",
#                      target_host="https://adb-4145924773612620.0.azuredatabricks.net",target_token="dapic4097acb93f44191d25257cb8127c038")     
            
                              
                      

        

 
