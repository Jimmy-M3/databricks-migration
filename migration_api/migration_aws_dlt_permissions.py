import requests
import json
import pandas as pd
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
#file_path = os.path.join(script_dir, 'log/acl_dlt.log')
file_path ='/home/ankeruser/aws/aws_workspace_config/try1_api/acl_dlt.log'
def get_target_dlt_name_ids(url_target, access_token_target):
    target_header = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    _api_version_target = "2.0"
    _url_target = f"{url_target}/api/{_api_version_target}/pipelines"
    p_data = requests.get(url=_url_target,headers=target_header)
    sps = p_data.json().get('statuses', None)
    name_ids = {}
    if sps is not None:
        for sp in sps:
            name_ids[sp['name']] = sp['pipeline_id'] 
    return name_ids
def create_permissions( access_token_target, permissions_data,pipeline_id,_databricks_domain_target):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    _api_version_target = "2.0"
    #update 目标权限
    _url_permission = f"{_databricks_domain_target}/api/{_api_version_target}/permissions/pipelines/{pipeline_id}"
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
    permissions_data=permissions_data.get('access_control_list')
    if response_permission.ok:
        print(str(permissions_data)+":更新权限成功")
    else:
        print(str(permissions_data)+":更新权限失败：", response_permission.status_code, response_permission.text)

    return 
#end 构造创建dlt函数
def call_migratoin_dlt_permissions(source_domain,source_token,domain_target,token_target):
    databricks_domain = source_domain
    access_token = source_token
    _databricks_domain_target = domain_target
    _access_token_target = token_target
    api_version = "2.0"
    _api_version_target = "2.0"
    # 构建API请求URL
    url = f"{databricks_domain}/api/{api_version}/pipelines"
    # 设置请求头，包括访问令牌
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    name_ids =get_target_dlt_name_ids(url_target=_databricks_domain_target,access_token_target=_access_token_target)
    init_flag = False
    with open(file_path, "w", newline="") as txtfile: 
        # 发送GET请求
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
                for content in contents:
                    df_tmp = requests.get(f"{databricks_domain}/api/{api_version}/permissions/pipelines/{content['pipeline_id']}", headers=headers).json()
                    # print(df_tmp.get('inherited'))     
                    with open('df_tmp.json', 'w') as f:
                        json.dump(df_tmp, f, indent=4)
                    #access_control_list = df_tmp["access_control_list"]
                    is_owner = next((entry for entry in df_tmp["access_control_list"] if any(perm["permission_level"] == "IS_OWNER" for perm in entry.get("all_permissions", []))), None)
                    if is_owner:
                        access_control_list = [is_owner] + [entry for entry in df_tmp["access_control_list"] if entry != is_owner]
                    else:
                        access_control_list = data["access_control_list"]
                    pipeline_name = content['name']
                    _pipeline_name = pipeline_name
                    _pipeline_id = name_ids[_pipeline_name]
                    df_tmp['name']=pipeline_name
                    dmp = json.dumps(df_tmp)
                    txtfile.write(str(dmp)+ '\n')     
                    #print(access_control_list)
                    for item in access_control_list:
                        new_format = {
                        'access_control_list': []}
                        #user_name = '"user_name": {}'.format(item.get('user_name', ''))
                        if not item['all_permissions'][0]['inherited'] :
                            # 根据原始数据中的信息创建新格式的条目
                            if item.get('user_name', '') !='':
                                new_item = {   
                                    "user_name": item.get('user_name', ''),
                                    "permission_level": item['all_permissions'][0]['permission_level']
                                }
                                # 将新条目添加到新格式的列表中
                                new_format['access_control_list'].append(new_item)
                                #print(new_format) 
                                create_permissions(access_token_target= _access_token_target, permissions_data=new_format,pipeline_id=_pipeline_id,_databricks_domain_target=_databricks_domain_target)
                                #create_permissions( warehouses_data=new_format,warehouse_name=_pipeline_name)
                            if item.get('group_name', '') !='':
                                    groupname= f"{item.get('group_name', '')}"
                                    print(groupname)
                                    new_item = {   
                                        "group_name": groupname,
                                        "permission_level": item['all_permissions'][0]['permission_level']
                                    }
                                    # 将新条目添加到新格式的列表中
                                    new_format['access_control_list'].append(new_item) 
                                
                                    create_permissions(ccess_token_target= _access_token_target, permissions_data=new_format,pipeline_id=_pipeline_id,_databricks_domain_target=_databricks_domain_target)
                            if item.get('service_principal_name', '') !='':
                                    new_item = {   
                                        "service_principal_name": item.get('service_principal_name', ''),
                                        "permission_level": item['all_permissions'][0]['permission_level']
                                    }
                                    # 将新条目添加到新格式的列表中
                                    new_format['access_control_list'].append(new_item) 
                                    create_permissions(access_token_target= _access_token_target, permissions_data=new_format,pipeline_id=_pipeline_id,_databricks_domain_target=_databricks_domain_target)
                            #txtfile.write("pipeline_name:"+pipeline_name+","+str(new_format)+ '\n')        

            else:
                print(f"Error: {response.status_code} - {response.text}")

            if data.get('next_page_token') == None or data.get('next_page_token') == '':
                break

 
