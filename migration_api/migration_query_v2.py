import requests
import json
import pandas as pd
import configparser
import os
import base64
from datetime import datetime 
def write_log(databricks_host,token,file_path):
    databricks_host = databricks_host
    token = token      
    headers ={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }           
    url_job=f"{databricks_host}/api/2.0/sql/queries"
    job_cnt =0
    export_cnt =0
    # script_dir = os.path.dirname(os.path.abspath(__file__))
    # file_path = os.path.join(script_dir, 'log\\query.log')
    file_path =file_path
    stime =datetime.now()
    # query_ids = {}
    print(f'开始导出:'+str(datetime.now()))
    init_flag = False
    data_schema =None
    with open(file_path, "w",newline="", encoding='utf-8') as txtfile: 
        while True:
            if init_flag:
                response_schema = requests.get(url_job, headers=headers, params={'page_token':data_schema.get('next_page_token'),'page_size':100})
            else:
                response_schema = requests.get(url_job, headers=headers,params={'page_token':'','page_size':100})
            init_flag=True
            if response_schema.status_code == 200 :
                data_schema = response_schema.json()
                get_jobs = data_schema.get('results',None)
                if get_jobs is not None:
                    job_cnt = job_cnt+len(get_jobs)
                    for sp in get_jobs:
                        # query_ids[sp['display_name']] = sp['id']
                        query=json.dumps(sp)
                        txtfile.write(query+ '\n')
                        export_cnt=export_cnt+1
                        #print("query名称:",sp.get('display_name'))
            if data_schema== None or data_schema.get('next_page_token') == None or data_schema.get('next_page_token') == '':
                break
    etime =datetime.now()          
    print(f'完成导出:'+str(etime))
    print(f'用时:'+str(etime-stime)+",共有:"+str(job_cnt),"导出"+str(export_cnt))
    return job_cnt
def get_source_sqlwarehouse_ids(source_domain,source_token):
        # return a dict of sp displayName and id mappings
        souce_url = f"{source_domain}/api/2.0/sql/warehouses"
        source_header = {
        "Authorization": f"Bearer {source_token}",
        "Content-Type": "application/json"
    }
        sp_data = requests.get(url=souce_url,headers=source_header)
        sps = sp_data.json().get('warehouses', None)
        sp_ids = {}
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
        sps = sp_data.json().get('warehouses', [])
        sp_ids = {}
        for sp in sps:
            sp_ids[sp['name']] = sp['id']
        return sp_ids 
#def check_target_query(target_domain,target_token):
        # return a dict of sp displayName and id mappings
#        target_url = f"{target_domain}/api/2.0/sql/queries"
#        target_header = {
#        "Authorization": f"Bearer {target_token}",
#        "Content-Type": "application/json"
#    }
#        sp_data = requests.get(url=target_url,headers=target_header)
#        sps = sp_data.json().get('results', None)
#        query_ids = {}
#        if sps is not None:
#            for sp in sps:
#                query_ids[sp['display_name']] = sp['id']
            # query_ids['update_mask'] = sp['update_mask']
#        return query_ids     
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
def check_target_query(target_domain,target_token):
        sps = get_resources_list(target_domain,target_token,"sql/queries",'2.0',"results")
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
    res_context=""
    if type ==0:
        response_query = requests.post(
            url_target,
            headers=headers,
            json=json.loads(json.loads(json.dumps(query_data).replace('anker.com','anker-in.com')))
        )
        res_context='迁移完成'
    if type ==1:
        response_query = requests.patch(
            url_target,
            headers=headers,
            json=(json.loads(json.dumps(query_data).replace('anker.com','anker-in.com')))
        )
        res_context='更新完成'
    #print(query_data)   
    query_name=""
    #query_name=json.loads(json.dumps(query_data).replace('anker.com','anker-in.com')).get('query').get('display_name')
    if response_query.ok:
        print(query_name+res_context)
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
    #file_path = os.path.join(script_dir, 'log/query.log')
    file_path ='/home/ankeruser/aws/aws_workspace_config/try1_api/query.log'
    #file_path ="/Workspace/DBR_AutoTest/metalog/checkquery.log"
    # 构建API请求URL
    _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/sql/queries/"
    # 设置请求头，包括访问令牌
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    source_sqlwarehouse_ids = get_source_sqlwarehouse_ids(source_domain=databricks_domain,source_token=access_token)
    target_sqlwarehouse_ids = get_target_sqlwarehouse_ids(target_domain=_databricks_domain_target,target_token=_access_token_target)
    query_ids = check_target_query(target_domain=_databricks_domain_target,target_token=_access_token_target)
    source_cnt = write_log(databricks_host=databricks_domain,token=access_token,file_path=file_path)
    # 发送GET请求
    with open(file_path, 'r', encoding='utf-8') as txtfile: 
            # 检查响应状态码
                for line in txtfile:  
                    content=json.loads(line)
                #     print(content)
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
                    # txtfile.write(str(_create_json)+ '\n')
                    query_id = query_ids.get(content.get('display_name'),None)
                    if query_id is not None:
                        _url_target = f"{_databricks_domain_target}/api/2.0/sql/queries/{query_id}"
                        _create_json =json.loads(_create_json)
                        _create_json["update_mask"]="description,display_name,query_text"
                        create_query(url_target=_url_target,access_token_target= _access_token_target, query_data=_create_json,type =1)
                    else:
                        _url_target = f"{_databricks_domain_target}/api/2.0/sql/queries"
                        create_query(url_target=_url_target,access_token_target= _access_token_target, query_data=_create_json,type=0)                
# call_migration_query(source_domain,source_token,target_domain,target_token)         
     
    

 
