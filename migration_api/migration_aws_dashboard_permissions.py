import requests
import json
import pandas as pd
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
#file_path = os.path.join(script_dir, 'log\\acl_dashboard.log')

def get_target_dashboard_name_ids(url_target, access_token_target):
    target_header = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    _api_version_target = "2.0"
    _url_target = f"{url_target}/api/{_api_version_target}/lakeview/dashboards"
    p_data = requests.get(url=_url_target,headers=target_header)
    sps = p_data.json().get('dashboards', None)
    name_ids = {}
    if sps is not None:
        for sp in sps:
            name_ids[sp['display_name']] = sp['dashboard_id'] 
    return name_ids
def create_permissions( access_token_target, permissions_data,object_id,_databricks_domain_target):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    _api_version_target = "2.0"
    #update 目标权限
    _url_permission = f"{_databricks_domain_target}/api/{_api_version_target}/permissions/dashboards/{object_id}"
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

    return 
def get_resources_list(databricks_domain, access_token, api_context, api_version='2.0', objectType='Resources'):
    url = f"{databricks_domain}/api/{api_version}/{api_context}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    clusters_ = []
    page_size = 100  # 设置每页大小
    next_page_token = ""  # 初始化分页令牌
    # 发送初始请求
    response = requests.get(url, headers=headers, params={"page_size": page_size})
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return clusters_
    # 解析初始响应
    data = response.json()
    next_page_token = data.get('next_page_token')
    clusters_.extend(data.get(objectType, []))
    # 循环获取后续页面
    while next_page_token:
        params = {"page_token": next_page_token, "page_size": page_size}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break
        data = response.json()
        next_page_token = data.get('next_page_token')
        clusters_.extend(data.get(objectType, []))
    return clusters_
#end 构造创建dlt函数
def call_migration_acl_dashboard(source_host,source_token,target_host,target_token,args):
    databricks_domain = source_host
    access_token = source_token
    _databricks_domain_target = target_host
    _access_token_target = target_token
    source_headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"}
    name_ids =get_target_dashboard_name_ids(url_target=_databricks_domain_target,access_token_target=_access_token_target)
    df_data =get_resources_list(databricks_domain, access_token, 'lakeview/dashboards', api_version='2.0', objectType='dashboards')
    if len(df_data)>1 :
        export_dir = args.set_export_dir
        file_path = os.path.join(export_dir, f'{args.session}_api/acl_dashboard.log')
        with open(file_path, "w", newline="") as txtfile:
            for _source_item in df_data:     
                dashboard_id =_source_item.get('dashboard_id')
                dashboard_name =_source_item.get('display_name')
                df_source = requests.get(f"{databricks_domain}/api/2.0/permissions/dashboards/{dashboard_id}", headers=source_headers).json()
                df_source['name'] =dashboard_name
                txtfile.write(str(json.dumps(df_source))+ '\n')  
                _dashboard_id = name_ids.get(dashboard_name)
                access_control_list = df_source.get('access_control_list')
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
                            create_permissions(access_token_target= _access_token_target, permissions_data=new_format,object_id=_dashboard_id,_databricks_domain_target=_databricks_domain_target)
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
                                create_permissions(ccess_token_target= _access_token_target, permissions_data=new_format,pipeline_id=_dashboard_id,_databricks_domain_target=_databricks_domain_target)
                        if item.get('service_principal_name', '') !='':
                                new_item = {   
                                    "service_principal_name": item.get('service_principal_name', ''),
                                    "permission_level": item['all_permissions'][0]['permission_level']
                                }
                                # 将新条目添加到新格式的列表中
                                new_format['access_control_list'].append(new_item) 
                                create_permissions(access_token_target= _access_token_target, permissions_data=new_format,pipeline_id=_dashboard_id,_databricks_domain_target=_databricks_domain_target)
                        #txtfile.write("pipeline_name:"+object_id+","+str(new_format)+ '\n')      
                      

        

 
