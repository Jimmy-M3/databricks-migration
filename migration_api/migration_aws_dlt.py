import requests
import json
import pandas as pd
import os

# # 替换为你的Databricks域名、访问令牌和版本号
# databricks_domain = ""
# access_token = ""
# _databricks_domain_target = ""
# _access_token_target = ""
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

target_cluster_type = "Standard_D8s_v3"
azure_aws_type_mapping = {
"r6i.large":target_cluster_type,
        "r5d.large":"Standard_D4d_V4",
        "m5a.xlarge":"Standard_D4s_v3",
        "m4.xlarge":target_cluster_type,
        "m4.2xlarge":"Standard_D8_V3",
        "m5.large":"Standard_F4",
        "m4.large":target_cluster_type,
        "r5dn.large":target_cluster_type,
        "r5dn.12xlarge":target_cluster_type,
        "r5d.4xlarge":"Standard_E16as_v4",
        "r5a.xlarge":target_cluster_type,
        "c5.2xlarge":"Standard_F8s_v2",
        "c5.xlarge":"Standard_F4",
        "c5d.4xlarge":"Standard_F16",
        "c5d.2xlarge":"Standard_F8",
        "d3en.2xlarge":"Standard_D8d_V4",
        "m5d.2xlarge":"Standard_D8ds_V4",
        "m5d.xlarge":target_cluster_type,
        "m5d.large":"Standard_F4",
        "m6i.large":"Standard_F4",
        "i3.xlarge":"Standard_E4ds_v4",
        "i3.2xlarge":"Standard_E8ds_v4",
        "i3.4xlarge":target_cluster_type,
        "i3en.xlarge":target_cluster_type,
        "g4dn.xlarge":target_cluster_type,
        "r5a.large":"Standard_D3_V2",
        "r5d.xlarge":"Standard_E4d_V4",
        "r6id.xlarge":"Standard_E4ds_v4",

        }
# 创建DLT
def create_dlt(url_target, access_token_target, dlt_data):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    response = requests.post(
        url_target,
        headers=headers,
        json=json.loads(str(dlt_data).replace('anker.com','anker-in.com'))
    )
    dlt_name=json.loads(dlt_data).get('name')
    if response.ok:
        print(dlt_name+"：迁移成功")
    else:
        print(dlt_name+"：迁移失败：", response.status_code, response.text)
    return 
#end 构造创建dlt函数
def call_migration_dlt(source_domain,source_token,domain_target,token_target):
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
    _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/pipelines/"
    init_flag = False
    #file_path = os.path.join(script_dir, 'log/dlt.log')
    file_path = '/home/lorealuser/Jimmy/cn3_01/try2_api/dlt.log'
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
            if response.status_code == 200 and data.get('statuses') != '' and data.get('statuses') != None:
                contents = data["statuses"]
                for content in contents:
                    df_tmp = requests.get(f"{databricks_domain}/api/{api_version}/pipelines/{content['pipeline_id']}", headers=headers).json()
                    #print(df_tmp)
                    with open('df_tmp.json', 'w') as f:
                        json.dump(df_tmp, f, indent=4)
                    name = df_tmp["name"]
                    libraries = df_tmp["spec"]["libraries"]
                    clusters = json.dumps(df_tmp["spec"]["clusters"])
                    for old, new in azure_aws_type_mapping.items():
                        if old in clusters:
                            clusters= clusters.replace(old,new)
                    trigger = df_tmp["spec"].get("trigger")
                    schema  = json.dumps(df_tmp["spec"].get("schema",""))
                    serverless = json.dumps(df_tmp["spec"].get("serverless",""))
                    storage = '"storage": {}'.format(json.dumps(df_tmp["spec"].get("storage","")))
                    if 'storage' in df_tmp["spec"]:
                        storage = f'{storage},'
                    target =  ',"target": {}'.format(json.dumps(df_tmp["spec"].get("target","")))
                    
                    catalog = '"catalog": {}'.format(json.dumps(df_tmp["spec"].get("catalog", "")))
                    if 'catalog' in df_tmp["spec"]:
                        catalog = f'{catalog},'
                    create_json = f"""
                    {{
                    "name": "{name}",
                    "libraries": {json.dumps(libraries)},
                    "clusters": {clusters.replace('SPOT','SPOT_AZURE').replace('ON_DEMAND','ON_DEMAND_AZURE').replace('SPOT_WITH_FALLBACK','SPOT_WITH_FALLBACK_AZURE').replace('aws_attributes','azure_attributes')},
                    {storage if df_tmp["spec"].get("storage") else ""}
                    "continuous": {json.dumps(df_tmp["spec"]["continuous"])}, 
                    "development": {json.dumps(df_tmp["spec"]["development"])}, 
                    "photon": {json.dumps(df_tmp["spec"]["photon"])}, 
                    "edition": "{df_tmp["spec"]["edition"]}", 
                    {'schema:'+schema +"," if schema =="" else ""}
                    "serverless":{serverless},
                    {catalog if df_tmp["spec"].get("catalog") else ""}
                    "channel": "{df_tmp["spec"]["channel"]}",
                    "data_sampling": {json.dumps("data_sampling")}
                    { json.dumps(f",trigger:"+trigger) if trigger is not None else ""}
                    {target if df_tmp["spec"].get("target") else ""} 
                    }}
                    """  

                    _create_json = create_json
                    txtfile.write(str(_create_json)+ '\n')
                    create_dlt(url_target=_url_target,access_token_target= _access_token_target, dlt_data=_create_json)
            else:
                print(f"Error: {response.status_code} - {response.text}")

            if data.get('next_page_token') == None or data.get('next_page_token') == '':
                break



# {
#   "continuous": false,
#   "name": "Wikipedia pipeline (SQL)",
#   "clusters": [
#     {
#       "label": "default",
#       "autoscale": {
#         "min_workers": 1,
#         "max_workers": 5,
#         "mode": "ENHANCED"
#       }
#     }
#   ],
#   "libraries": [
#     {
#       "notebook": {
#         "path": "/Users/username/DLT Notebooks/Delta Live Tables quickstart (SQL)"
#       }
#     }
#   ],
#   "storage": "/Users/username/data"
# }
