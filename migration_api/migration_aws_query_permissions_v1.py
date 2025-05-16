import requests
import json
import pandas as pd
import os
# script_dir = os.path.dirname(os.path.abspath(__file__))
#file_path = os.path.join(script_dir, 'log\\acl_query.log')
# file_path = '/home/lorealuser/Jimmy/cn3_01/try2_api/acl_query.log'
def create_permissions( access_token_target, permissions_data,object_id,_databricks_domain_target):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    _api_version_target = "2.0"
    #update 目标权限
    _url_permission = f"{_databricks_domain_target}/api/{_api_version_target}/preview/sql/permissions/queries/{object_id}"
  
    response_permission = requests.post(
        _url_permission,
        headers=headers,
        #json=(permissions_data.replace('anker.com','anker-in.com'))
        json=(json.loads(json.dumps(permissions_data).replace('anker.com','anker-in.com')))
    )
    #print(permissions_data,object_id)
    if response_permission.ok:
                    print("更新成功")
    else:
                    print("更新权限失败：", response_permission.status_code, response_permission.text)

    return 

#end 构造创建dlt函数
def call_migration_acl_query(source_host,source_token,target_host,target_token,args):
    databricks_domain = source_host
    access_token = source_token
    _databricks_domain_target = target_host
    _access_token_target = target_token
    source_headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"}
   
    # map_path = "/home/lorealuser/Jimmy/cn3_01/try2_api/query_id_map.log"
    #file_path = os.path.join(script_dir, 'log\\acl_query.log')
    export_dir = args.set_export_dir
    map_path = os.path.join(export_dir, f'{args.session}_api/query_id_map.log')
    file_path = os.path.join(export_dir, f'{args.session}_api/acl_query.log')
    with open(map_path, "r", newline="") as fp,open(file_path,'w', newline="") as fp_log:
        json_objects = fp.read().strip().split('\n')
        for json_str in json_objects:
            json_data = json.loads(json_str)
            old_id = json_data.get('old_id')
            _newid = json_data.get('new_id') 

            df_source = requests.get(f"{databricks_domain}/api/2.0/preview/sql/permissions/queries/{old_id}", headers=source_headers).json()
            fp_log.write(str(json.dumps(df_source))+ '\n')  
            access_control_list = df_source.get('access_control_list')
            new_format = {
                 'access_control_list': access_control_list}
            create_permissions(access_token_target= _access_token_target, permissions_data=new_format,object_id=_newid,_databricks_domain_target=_databricks_domain_target)
            # print(_query_id)
            '''for item in access_control_list:
                new_format = {
                'access_control_list': []}
                #user_name = '"user_name": {}'.format(item.get('user_name', ''))
                if  len(item) !="":
                    # 根据原始数据中的信息创建新格式的条目
                    if item.get('user_name', '') !='':
                        new_item = {   
                            "user_name": item.get('user_name', '').replace('anker.com','anker-in.com'),
                            "permission_level": item['permission_level']
                        }
                        # 将新条目添加到新格式的列表中
                        new_format['access_control_list'].append(new_item)
                        #print(new_format) 
                        create_permissions(access_token_target= _access_token_target, permissions_data=new_format,object_id=_newid,_databricks_domain_target=_databricks_domain_target)
                        
                    if item.get('group_name', '') !='':
                            groupname= f"{item.get('group_name', '')}"
                            new_item = {   
                                "group_name": groupname,
                                "permission_level": item['permission_level']
                            }
                            # 将新条目添加到新格式的列表中
                            new_format['access_control_list'].append(new_item) 
                            create_permissions(access_token_target= _access_token_target, permissions_data=new_format,object_id=_newid,_databricks_domain_target=_databricks_domain_target)
                    if item.get('service_principal_name', '') !='':
                            new_item = {   
                                "service_principal_name": item.get('service_principal_name', ''),
                                "permission_level": item['permission_level']
                            }
                            # 将新条目添加到新格式的列表中
                            new_format['access_control_list'].append(new_item) 
                            create_permissions(access_token_target= _access_token_target, permissions_data=new_format,object_id=_newid,_databricks_domain_target=_databricks_domain_target)
                else:
                    print('ignore inherited acl')            '''
                        # txtfile.write("pipeline_name:"+object_id+","+str(new_format)+ '\n') 
# source_host="https://dbc-1c15bce2-47da.cloud.databricks.com"
# source_token="dapif68be4994c246f0cb09c54e1faa337dc"
# target_token="dapic4097acb93f44191d25257cb8127c038"
# target_host="https://adb-4145924773612620.0.azuredatabricks.net"                      
# call_migration_acl_query(source_host,source_token,target_host,target_token)                        
                             
                      

        

 
