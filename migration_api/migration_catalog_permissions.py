import requests
import json
import pandas as pd

import os

# 替换为你的Databricks域名、访问令牌和版本号
# databricks_domain = ""
# access_token = ""
# _databricks_domain_target = ""
# _access_token_target = ""
# script_dir = os.path.dirname(os.path.abspath(__file__))
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
# _databricks_domain_target = "https://adb-620185867754831.3.databricks.azure.cn"
# _access_token_target = "dapi8ef5cfe4ab5cd8c7a8b9bd3303f2a9b4"

script_dir = os.path.dirname(os.path.abspath(__file__))
catalog_file_path = os.path.join(script_dir, 'log/acl_catalog.log')
schema_file_path = os.path.join(script_dir, 'log/acl_schemas.log')
table_file_path = os.path.join(script_dir, 'log/acl_tables.log')
catalogs_acl=[]
schemas_acl =[]
tables_acl = []
# 创建
def create_permissions(url_target, access_token_target, permissions_data):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    #print(permissions_data)
    #更新权限
    response_permission = requests.patch(
                    url_target,
                    headers=headers,
                    json=json.loads(json.loads(json.dumps(permissions_data).replace('anker.com','anker-in.com')))
                )
    data_pipeline = response_permission.json()
    if response_permission.ok:
        print("更新权限成功",url_target)
    else:
        print("更新权限失败：", response_permission.status_code, response_permission.text,url_target)
    return 


def get_source_sp_ids(source_domain,source_token):
        # return a dict of sp displayName and id mappings
        souce_url = f"{source_domain}/api/2.0/preview/scim/v2/ServicePrincipals"
        source_header = {
        "Authorization": f"Bearer {source_token}",
        "Content-Type": "application/json"
    }
        sp_data = requests.get(url=souce_url,headers=source_header)
        sp_ids = {}
        if sp_data.status_code==200:  
            sps = sp_data.json().get('Resources', None)
            for sp in sps:
                sp_ids[sp['applicationId']] = sp['displayName']
        else:
            print(source_domain,sp_data.text)        
        return sp_ids
def get_target_sp_ids(target_domain,target_token):
        # return a dict of sp displayName and id mappings
        target_url = f"{target_domain}/api/2.0/preview/scim/v2/ServicePrincipals"
        target_header = {
        "Authorization": f"Bearer {target_token}",
        "Content-Type": "application/json"
    }
        sp_data = requests.get(url=target_url,headers=target_header)
        sps = sp_data.json().get('Resources', None)
        sp_ids = {}
        for sp in sps:
            sp_ids[sp['displayName']] = sp['applicationId']
        return sp_ids    
#end 构造创建permissions函数

def call_migration_catalog_permissions(source_domain,source_token,domain_target,token_target):
    databricks_domain = source_domain
    access_token = source_token
    _databricks_domain_target = domain_target
    _access_token_target = token_target
   
    _api_version_target = "2.1"
    source_sps = get_source_sp_ids(source_domain= databricks_domain,source_token=access_token)
    target_sps = get_target_sp_ids(target_domain= _databricks_domain_target,target_token=_access_token_target)
     
    # 构建REST API请求
    catalog_url = f"{databricks_domain}/api/2.1/unity-catalog/catalogs"
    schema_url = f"{databricks_domain}/api/2.1/unity-catalog/schemas"
    table_url = f"{databricks_domain}/api/2.1/unity-catalog/tables"
    source_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(catalog_url, headers=source_headers)

    # 检查响应状态码
    if response.status_code == 200 and 'catalogs' in response.json():
        # 解析响应数据
        data = response.json()
        catalogs = data['catalogs']
        # 遍历每个catalog并获取其下的schema
        for catalog in catalogs:
            if "full_name" in catalog:
                catalog_fullname = catalog["full_name"]
                # if catalog_fullname  in ('hive_metastore','samples','main','system','airbyte_test','bdec'):
                if catalog_fullname =="test_migration":
                    response_catalog = requests.get(f"{databricks_domain}/api/2.1/unity-catalog/permissions/catalog/{catalog_fullname}", headers=source_headers)
                    if response_catalog.status_code == 200 and 'privilege_assignments' in response_catalog.json():
                        data_catalogs = response_catalog.json().get('privilege_assignments')
                        #print(response_catalog.json())
                        for data_catalog in data_catalogs:
                            principal = data_catalog['principal']
                            old_name = source_sps.get(principal)
                            new_principal = target_sps.get(old_name,None)
                            if new_principal is not None:
                                principal = new_principal
                            privileges = data_catalog['privileges']
                            create_json = f"""
                                    {{
                                        "changes": [
                                            {{
                                                "principal": "{principal}",
                                                "add": {json.dumps(privileges)}
                                            }}
                                        ]
                                    }}"""
                            _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/unity-catalog/permissions/catalog/{catalog_fullname}"
                            create_permissions(url_target=_url_target, access_token_target=_access_token_target, permissions_data=create_json)
                            #print(data_catalog)
                            alc_value = "catalog_fullname:"+catalog_fullname+str(create_json)
                            catalogs_acl.append(alc_value)
                            print('------data_catalog--OK')
                    init_flag = False
                    while True:
                        if init_flag:
                            response_schema = requests.get(schema_url, headers=source_headers, params={'catalog_name':catalog_fullname, 'page_token':data_schema.get('next_page_token')})
                        else:
                            response_schema = requests.get(schema_url, headers=source_headers, params={'catalog_name':catalog_fullname})
                        init_flag=True
                        # 检查响应状态码
                        if response_schema.status_code == 200 and 'schemas' in response_schema.json():
                            data_schema = response_schema.json()
                            contents = data_schema['schemas']
                            for content in contents:
                                if 'full_name' in content:
                                    schema_fullname = content['full_name']
                                    schema_name = content["name"]
                                    if schema_name not in ('information_schema','default'):
                                        url =f"{databricks_domain}/api/2.1/unity-catalog/permissions/schema/{schema_fullname}"
                                        response_schema = requests.get(url, headers=source_headers)
                                        if response_schema.status_code == 200 and 'privilege_assignments' in response_schema.json():
                                            permission_schemas = response_schema.json().get('privilege_assignments')
                                            
                                            for permission_schema in permission_schemas:
                                                principal = permission_schema['principal']
                                                old_name = source_sps.get(principal)
                                                new_principal = target_sps.get(old_name,None)
                                                if new_principal is not None:
                                                    principal = new_principal
                                                privileges = permission_schema['privileges']
                                                create_json = f"""
                                                    {{
                                                        "changes": [
                                                            {{
                                                                "principal": "{principal}",
                                                                "add": {json.dumps(privileges)}
                                                            }}
                                                        ]
                                                    }}""" 
                                                _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/unity-catalog/permissions/schema/{schema_fullname}"
                                                create_permissions(url_target=_url_target, access_token_target=_access_token_target, permissions_data=create_json)
                                                alc_value = "schema_fullname:"+schema_fullname+str(create_json)
                                                schemas_acl.append(alc_value)
                                                print('------permission_schema---OK')
                                        # loop table
                                        init_flag_table = False
                                        while True:
                                            # 构建获取table的URL
                                            if init_flag_table:
                                                response_table = requests.get(table_url, headers=source_headers, json={"catalog_name":catalog_fullname,"schema_name":schema_name,'page_token':data_schema.get('next_page_token')})
                                            else:
                                                response_table = requests.get(table_url, headers=source_headers, json={"catalog_name":catalog_fullname,"schema_name": schema_name})
                                            init_flag_table=True
                                            # 检查响应状态码 
                                            data_table = response_table.json()
                                            if response_table.status_code == 200  and "tables" in response_table.json():
                                                for table in data_table.get("tables"):
                                                    table_fullname=table.get("full_name")
                                                    #print(table_fullname)
                                                    if 'databricks_internal' not in table_fullname: 
                                                        response_table = requests.get(f"{databricks_domain}/api/2.1/unity-catalog/permissions/table/{table_fullname}", headers=source_headers)
                                                        #print(response_table.json())
                                                        if response_table.status_code == 200 and 'privilege_assignments' in response_schema.json():
                                                            permission_tables = response_table.json().get('privilege_assignments')
                                                            if permission_tables is not None:
                                                                for permission_table in permission_tables:
                                                                    principal = permission_table['principal']
                                                                    old_name = source_sps.get(principal)
                                                                    new_principal = target_sps.get(old_name,None)
                                                                    if new_principal is not None:
                                                                        principal = new_principal
                                                                    privileges = permission_table['privileges']
                                                                    create_json = f"""
                                                                        {{
                                                                            "changes": [
                                                                                {{
                                                                                    "principal": "{principal}",
                                                                                    "add": {json.dumps(privileges)}
                                                                                }}
                                                                            ]
                                                                        }}"""
                                                                    _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/unity-catalog/permissions/table/{table_fullname}"
                                                                    create_permissions(url_target=_url_target, access_token_target=_access_token_target, permissions_data=create_json)
                                                                    alc_value = "tables_fullname:"+table_fullname+str(create_json)
                                                                    tables_acl.append(alc_value)
                                                    print('------permission_table---OK',table_fullname)
                                                        
                                                    
                                                else:
                                                    print(f"Error: {response_table.status_code} - {response_table.text}")
                                                response_table.close()
                                            if data_table.get('next_page_token') == None or data_table.get('next_page_token') == '':
                                                    break
                                            # end lop table    
                        else:
                            print(f"Error: {response_schema.status_code} - {response_schema.text}")
                        response_schema.close()
                        if data_schema.get('next_page_token') == None or data_schema.get('next_page_token') == '':
                            break
    
    else:
        print("Failed to retrieve catalogs. Error:", response.text)
        
    with open(catalog_file_path, "w", newline="") as txtfile:
        txtfile.write(str(catalogs_acl))
    with open(schema_file_path, "w", newline="") as txtfile:
        txtfile.write(str(schemas_acl))
    with open(table_file_path, "w", newline="") as txtfile:
        txtfile.write(str(tables_acl))                
