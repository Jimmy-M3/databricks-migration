import requests
import json
import pandas as pd
import configparser
import os
import base64
from concurrent.futures import ThreadPoolExecutor

def get_sqlwarehouse_ids(source_domain,source_token):
        # return a dict of sp displayName and id mappings
        souce_url = f"{source_domain}/api/2.0/sql/warehouses"
        source_header = {
        "Authorization": f"Bearer {source_token}",
        "Content-Type": "application/json"
    }
        sp_data = requests.get(url=souce_url,headers=source_header)
        sps = sp_data.json().get('warehouses', None)
        sp_ids = {}
        if sps is not None:
            for sp in sps:
                sp_ids[sp['id']] = sp['name']
        return sp_ids
def get_target_sqlwarehouse_ids(target_domain,target_token):
        # return a dict of sp displayName and id mappings
        target_url = f"{target_domain}/api/2.0/sql/warehouses"
        target_header = {
        "Authorization": f"Bearer {target_token}",
        "Content-Type": "application/json"
    }
        sp_data = requests.get(url=target_url,headers=target_header)
        sps = sp_data.json().get('warehouses', None)
        sp_ids = {}
        for sp in sps:
            sp_ids[sp['name']] = sp['id']
        return sp_ids     
def check_target_query(target_domain,target_token):
        # return a dict of sp displayName and id mappings
        target_url = f"{target_domain}/api/2.0/sql/queries"
        target_header = {
        "Authorization": f"Bearer {target_token}",
        "Content-Type": "application/json"
    }
        sp_data = requests.get(url=target_url,headers=target_header)
        sps = sp_data.json().get('results', None)
        query_ids = {}
        if sps is not None:
            for sp in sps:
                query_ids[sp['display_name']] = sp['id']
            # query_ids['update_mask'] = sp['update_mask']
        return query_ids     
def create_query(url_target, access_token_target, query_data,type,update_mask =None):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    if type ==0:
        response_query = requests.post(
            url_target,
            headers=headers,
            json=json.loads(json.loads(json.dumps(query_data).replace('anker.com','anker-in.com')))
        )
    if type ==1:
        
        response_query = requests.patch(
            url_target,
            headers=headers,
            json=(json.loads(json.dumps(query_data).replace('anker.com','anker-in.com')))
        )
    query_name=json.loads(json.dumps(query_data).replace('anker.com','anker-in.com')).get('query').get('display_name')
    if response_query.ok:
        print(query_name+":Query迁移")
    else:
        print("请求失败：", response_query.status_code, response_query.text)
    return 
#end 构造创建dlt函数
def call_migration_query(source_domain,source_token,domain_target,token_target):
    databricks_domain = source_domain
    access_token = source_token
    _databricks_domain_target = domain_target
    _access_token_target = token_target
    api_version = "2.0"
    _api_version_target = "2.0"
    url = f"{databricks_domain}/api/{api_version}/sql/queries"
    _api_version_target = "2.0"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'log/query.log')
    # 构建API请求URL
    _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/sql/queries/"
    # 设置请求头，包括访问令牌
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    source_sqlwarehouse_ids = get_sqlwarehouse_ids(source_domain=databricks_domain,source_token=access_token)
    target_sqlwarehouse_ids = get_target_sqlwarehouse_ids(target_domain=_databricks_domain_target,target_token=_access_token_target)
    query_ids = check_target_query(target_domain=_databricks_domain_target,target_token=_access_token_target)
    init_flag = False
    # 发送GET请求
    with open(file_path, "w", newline="") as txtfile: 
        while True:
            if init_flag:
                response = requests.get(url, headers=headers, params={'page_token':data.get('next_page_token')})
            else:
                response = requests.get(url, headers=headers)
            data = response.json()
            init_flag=True
            # 检查响应状态码
            if response.status_code == 200 and data.get('results') != '' and data.get('results') != None:
                contents = data["results"]
                with ThreadPoolExecutor(max_workers=10) as executor:
                    for content in contents:  
                        parent_path =f"/Workspace/Users/"+str(content['owner_user_name']).replace('anker.com','anker-in.com')
                        warehouse_id = content.get('warehouse_id')
                        warehouse_name = source_sqlwarehouse_ids.get(warehouse_id)
                        new_warehouse_id = target_sqlwarehouse_ids.get(warehouse_name,None)
                        # query =content.get('query_text').encode('utf-8').decode('unicode_escape')
                        # _query=json.dumps(query, indent=4)
                        _query =f"""{content.get('query_text')}"""
                        if new_warehouse_id is not None:
                            warehouse_id = new_warehouse_id
                        create_json = {
                        "query": {
                            "description": content.get('description', ''),
                            "tags": content.get('tags', []),
                            "display_name": content.get('display_name', ''),
                            "parent_path": parent_path,
                            "query_text": _query,
                            "parameters": content.get('parameters'),
                            "warehouse_id": warehouse_id,
                            "run_as_mode": content.get('run_as_mode', '')
                        }
                    }
                        _create_json = json.dumps(create_json, indent=2)
                        txtfile.write(str(_create_json)+ '\n')
                        query_id = query_ids.get(content.get('display_name'),None)
                        if query_id is not None:
                            _url_target = f"{_databricks_domain_target}/api/2.0/sql/queries/{query_id}"
                            _create_json =json.loads(_create_json)
                            _create_json["update_mask"]="description,display_name,query_text"
                            future=executor.submit(create_query, _url_target, _access_token_target, _create_json, 1)
                            response = future.result()
                            #create_query(url_target=_url_target,access_token_target= _access_token_target, query_data=_create_json,type =1)
                        else:
                            _url_target = f"{_databricks_domain_target}/api/2.0/sql/queries"
                            future=executor.submit(create_query, _url_target, _access_token_target, _create_json, 0)
                            response = future.result()
                            #create_query(url_target=_url_target,access_token_target= _access_token_target, query_data=_create_json,type=0)
            else:
                print(f"Error: {response.status_code} - {response.text}")

            if data.get('next_page_token') == None or data.get('next_page_token') == '':
                break

 
