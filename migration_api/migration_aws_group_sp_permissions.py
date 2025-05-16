import requests
import json
import pandas as pd
import os
script_dir = os.path.dirname(os.path.abspath(__file__))

def get_target_group_sp_name_ids(url_target, access_token_target,type="Groups"):
    target_header = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    #/api/2.0/preview/scim/v2/Groups
    if type.lower() =='groups':
        type ="Groups"
    else :
        type ="ServicePrincipals"
    _api_version_target = "2.0"
    _url_target = f"{url_target}/api/{_api_version_target}/preview/scim/v2/{type}"
    p_data = requests.get(url=_url_target,headers=target_header)
    names =[]
    ids ={}
    if p_data.status_code==200:  
        sps = p_data.json().get('Resources', None)
        if sps is not None:
            for sp in sps:
                if type.lower() =='groups':
                    ids[sp['displayName']] = sp['id'] 
                else:
                    ids[sp['displayName']] = sp['applicationId'] 
                names.append(sp['displayName'])  
    else:
        print(_url_target,p_data.text)                 
    return  names,ids
def get_source_group_sp_name_ids(url_target, access_token_target,type ="Groups"):
    target_header = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    if type.lower() =='groups':
        type ="Groups"
    else :
        type ="ServicePrincipals"
    _api_version_target = "2.0"
    _url_target = f"{url_target}/api/{_api_version_target}/preview/scim/v2/{type}"
    p_data = requests.get(url=_url_target,headers=target_header)
    names =[]
    ids ={}
    if p_data.status_code==200: 
        sps = p_data.json().get('Resources', None)
        if sps is not None:
            for sp in sps:
                if type.lower() =='groups':
                    ids[sp['displayName']] = sp['id'] 
                else:
                    ids[sp['displayName']] = sp['applicationId'] 
                names.append(sp['displayName'])   
    else:
        print(_url_target,p_data.text)
                
    return  names,ids

def create_permissions( access_token_target, permissions_data,_databricks_domain_target):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    _api_version_target = "2.0"
    #update 目标权限
    _url_permission = f"{_databricks_domain_target}/api/{_api_version_target}/preview/accounts/access-control/rule-sets"
    print(permissions_data)
    response_permission = requests.put(
                        _url_permission,
                        headers=headers,
                        json=json.loads(json.dumps(permissions_data).replace('anker.com','anker-in.com'))
                                        )
    if response_permission.ok:
                    print("更新权限成功",response_permission.json())
    elif response_permission.status_code ==409:
        print(response_permission.text,permissions_data)
    else:
        print("更新权限失败：", response_permission.status_code, response_permission.text) 
#end 构造创建dlt函数
def call_migratoin_group_sp_permissions(
        source_domain,source_token,domain_target,token_target,account_id_source,account_id_target,type ,args):

    databricks_domain = source_domain
    access_token = source_token
    _databricks_domain_target = domain_target
    _access_token_target = token_target
    api_version = "2.0"
    file_name = f"log/acl_{type}.log"
    export_dir = args.set_export_dir
    # file_path =f"/home/lorealuser/Jimmy/cn3_01/try2/acl_{type}.log"
    file_path = os.path.join(export_dir, f'{args.session}/acl_{type}.log')
    #file_path = os.path.join(script_dir, file_name)
    url_source = f"{databricks_domain}/api/{api_version}/preview/accounts/access-control/rule-sets"
    url_target = f"{_databricks_domain_target}/api/{api_version}/preview/accounts/access-control/rule-sets"
    # 设置请求头，包括访问令牌
    headers_source = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    headers_target = {
        "Authorization": f"Bearer {token_target}",
        "Content-Type": "application/json"
    }
    source_names,source_name_ids =get_source_group_sp_name_ids(url_target=databricks_domain,access_token_target=access_token,type=type)
    target_names,target_name_ids =get_target_group_sp_name_ids(url_target=_databricks_domain_target,access_token_target=_access_token_target,type =type)
    with open(file_path, "w", newline="") as txtfile:
        for sourcegroup_name in source_names:
            sourcegroup_id = source_name_ids.get(sourcegroup_name)
            params_source = {
                "name": f"accounts/{account_id_source}/groups/{sourcegroup_id}/ruleSets/default",
                "etag": ""
            }
            if type.lower() =="serviceprincipals":
                params_source = {
                "name": f"accounts/{account_id_source}/servicePrincipals/{sourcegroup_id}/ruleSets/default",
                "etag": ""
            } 
               
            response_source = requests.get(url_source, headers=headers_source,params=params_source)
            data_permission = response_source.json()
            # 检查响应状态码
            if response_source.status_code == 200:
                    target_group_id =target_name_ids.get(sourcegroup_name,None) 
                    params_target = {
                            "name": f"accounts/{account_id_target}/groups/{target_group_id}/ruleSets/default",
                            "etag": ""
                        } 
                    if type.lower() =="serviceprincipals":
                            params_target = {
                            "name": f"accounts/{account_id_target}/servicePrincipals/{target_group_id}/ruleSets/default",
                            "etag": ""
                        } 
                    if target_group_id is not None:
                        principals =[]
                        role =""
                        grant_rules=data_permission.get('grant_rules',None)
                        if grant_rules is not None:
                            for i in grant_rules:
                                principals=i.get('principals')
                                role=i.get('role')               
                        if grant_rules is not None:
                            get_roleset = requests.get(
                                                    url_target,
                                                    headers=headers_target,
                                                    params=params_target)
                            etag=get_roleset.json().get('etag','')
                            rule_name =f"accounts/{account_id_target}/groups/{target_group_id}/ruleSets/default"
                            if type.lower() =="serviceprincipals":
                                rule_name =f"accounts/{account_id_target}/servicePrincipals/{target_group_id}/ruleSets/default"
                            
                            data = {
                                    "name": rule_name,
                                    "rule_set": {
                                        "name": rule_name,
                                        "grant_rules": [
                                        {
                                            "principals":principals,
                                            "role": role
                                        }
                                        ],
                                        "etag": etag,
                                        "description": ""
                                    }
                                    }
                            create_permissions(access_token_target= _access_token_target, permissions_data=data,_databricks_domain_target=_databricks_domain_target )   
                            txtfile.write(f"{type}_name:"+sourcegroup_name+","+str(data)+ '\n')        

            else:
                print(f"Error1: {response_source.status_code} - {response_source.text}")
#call_migratoin_group_sp_permissions("https://dbc-1c15bce2-47da.cloud.databricks.com","dapi62958dd57a824a0fe6c80f027bbd3b3c","https://adb-4145924773612620.0.azuredatabricks.net","dapic6d4c1d9850a23087423bafdb3b3f4db","9de57428-7fd6-4d45-bdd3-ba37d8688364","48573fef-e064-4a42-9437-41eee5fdaa96","groups")            

         

 
