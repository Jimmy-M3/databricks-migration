import requests
import json
import pandas as pd
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
# file_path = os.path.join(script_dir, 'config.txt')
 
# with open(file_path, 'r') as file:
#     # 读取所有行到一个列表中
#     config_lines = file.readlines()
#     for line in config_lines:
#         parts = line.strip().split('=', 1)
#         if len(parts) == 2:
#             key, value = parts
#             key = key.strip()
#             value = value.strip()
#             if key == 'databricks_domain':
#                 databricks_domain = value   
#             if key == 'access_token':
#                 access_token = value
#             if key == '_databricks_domain':
#                 _databricks_domain_target = value   
#             if key == '_access_token':
#                 _access_token_target = value
#                 break  

# 构造创建权限函数
def get_target_sqlwarehoure_name_ids(url_target, access_token_target):
    target_header = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    _api_version_target = "2.0"
    _url_target = f"{url_target}/api/{_api_version_target}/sql/warehouses"
    p_data = requests.get(url=_url_target,headers=target_header)
    sps = p_data.json().get('warehouses', None)
    name_ids = {}
    if sps is not None:
        for sp in sps:
            name_ids[sp['name']] = sp['id'] 
    return name_ids
def create_permissions(warehouses_data,warehouse_id,domain_target,token_target):
    _databricks_domain_target =  domain_target
    _access_token_target =  token_target
    _api_version_target = "2.0"
    _warehouse_id =warehouse_id
    headers = {
        "Authorization": f"Bearer {_access_token_target}",
        "Content-Type": "application/json"
    }
     
    if  "IS_OWNER" in json.dumps(warehouses_data):
                    _url_permission = f"{_databricks_domain_target}/api/{_api_version_target}/permissions/warehouses/{_warehouse_id}"
                    response_permission = requests.put(
                        _url_permission,
                        headers=headers,
                        json=json.loads(json.dumps(warehouses_data).replace('anker.com','anker-in.com'))
                    )
    else:
                    _url_permission = f"{_databricks_domain_target}/api/{_api_version_target}/permissions/warehouses/{_warehouse_id}"
                    response_permission = requests.patch(
                        _url_permission,
                        headers=headers,
                        json=json.loads(json.dumps(warehouses_data))
                    )
                    
                    #response_permission = requests.patch(
                    #    _url_permission,
                    #    headers=headers,
                    #    json=json.loads(json.dumps(warehouses_data).replace('anker.com','anker-in.com'))
                    #)

    if response_permission.ok:
                    print("更新warehouses_permissions权限成功")
    else:
                    print("更新warehouses_permissions权限失败：")
 
    return 
#end 构造创建权限函数
def call_migratoin_sql_permission(source_domain,source_token,domain_target,token_target,args):
    api_version = "2.0"
    databricks_domain = source_domain
    access_token = source_token
    _databricks_domain_target = domain_target
    _access_token_target = token_target
    api_version = "2.0"
    # 设置请求头，包括访问令牌
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    # 构建API请求URL
    url = f"{databricks_domain}/api/{api_version}/sql/warehouses"
    init_flag = False
    file_name ="log/acl_warehouses.log"
    #file_path = os.path.join(script_dir, file_name)
    export_dir = args.set_export_dir
    file_path = os.path.join(export_dir, f'{args.session}_api/acl_warehouses.json')
    # file_path ='/home/lorealuser/Jimmy/cn3_01/try2_api/acl_warehouses.log'
    name_ids = get_target_sqlwarehoure_name_ids(url_target=_databricks_domain_target,access_token_target=_access_token_target)
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
            if response.status_code == 200 and data.get('warehouses') != None:
                contents = data["warehouses"]
                for content in contents:
                    df_tmp = requests.get(f"{databricks_domain}/api/{api_version}/permissions/warehouses/{content['id']}", headers=headers).json()
                    #print(df_tmp)     
                    with open('df_tmp.json', 'w') as f:
                        json.dump(df_tmp, f, indent=4)
                    #access_control_list = df_tmp["access_control_list"]
                    is_owner = next((entry for entry in df_tmp["access_control_list"] if any(perm["permission_level"] == "IS_OWNER" for perm in entry.get("all_permissions", []))), None)
                    if is_owner:
                        access_control_list = [is_owner] + [entry for entry in df_tmp["access_control_list"] if entry != is_owner]
                    else:
                        access_control_list = data["access_control_list"]
                    warehouse_name = content['name']
                    _warehouse_name = warehouse_name
                    df_tmp['name'] = warehouse_name
                    txtfile.write(str(json.dumps(df_tmp))+ '\n')
                    _warehouse_id =name_ids.get(_warehouse_name)
                    # 初始化新格式的字典
                    for item in access_control_list:
                        new_format = {
                        'access_control_list': []}
                        #user_name = '"user_name": {}'.format(item.get('user_name', ''))
                        # 根据原始数据中的信息创建新格式的条目
                        if item.get('user_name', '') !='':
                            new_item = {   
                                "user_name": item.get('user_name', '').replace('anker.com','anker-in.com'),
                                "permission_level": item['all_permissions'][0]['permission_level']
                            }
                            # 将新条目添加到新格式的列表中
                            new_format['access_control_list'].append(new_item)
                            #print(new_format) 
                            create_permissions( warehouses_data=new_format,warehouse_id=_warehouse_id,domain_target=_databricks_domain_target,token_target=_access_token_target)
                        if item.get('group_name', '') !='':
                                groupname= f"{item.get('group_name', '')}"
                                new_item = {   
                                    "group_name": groupname,
                                    "permission_level": item['all_permissions'][0]['permission_level']
                                }
                                # 将新条目添加到新格式的列表中
                                new_format['access_control_list'].append(new_item) 
                            
                                create_permissions( warehouses_data=new_format,warehouse_id=_warehouse_id,domain_target=_databricks_domain_target,token_target=_access_token_target)
                        if item.get('service_principal_name', '') !='':
                                new_item = {   
                                    "service_principal_name": item.get('service_principal_name', ''),
                                    "permission_level": item['all_permissions'][0]['permission_level']
                                }
                                # 将新条目添加到新格式的列表中
                                new_format['access_control_list'].append(new_item) 
                                
                                create_permissions( warehouses_data=new_format,warehouse_id=_warehouse_id,domain_target=_databricks_domain_target,token_target=_access_token_target)
                        #txtfile.write("warehouse_name:"+_warehouse_name+","+str(new_format)+ '\n')        
                    #print(json.loads(_create_json))
                    #print("--------")
                    # 构建API请求URL
                    
            else:
                print(f"Error: {response.status_code} - {response.text}")

            if data.get('next_page_token') == None or data.get('next_page_token') == '':
                break



 
