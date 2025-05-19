import requests
import json
import os

#from modify_log.modify_query_id import old_id

# 替换为你的Databricks域名、访问令牌和版本号
# databricks_domain = ""
# access_token = ""
# _databricks_domain_target = ""
# _access_token_target = ""
script_dir = os.path.dirname(os.path.abspath(__file__))
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


# 创建sql warehouse
def create_warehouse(url_target, access_token_target, warehouse_data):
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    response = requests.post(
        url_target,
        headers=headers,
        json=json.loads(warehouse_data)
    )
    # id = response.get(id)

    #id = data['id']
    # url_stop = f"{url_target}/{id}/stop"
    # response_stop = requests.post(
    #     url_stop,
    #     headers=headers 
    # )
    # if response_stop.ok:
    #     print("Stop 成功")
    # else:
    #     print("Stop 失败")
    if response.ok:
        print("请求成功")
    else:
        print("请求失败：", response.status_code, response.text)
    return id
# 发送Source GET请求
def call_migratoin_sqlwarehouse(source_domain,source_token,domain_target,token_target,args)->None:
    databricks_domain = source_domain
    access_token = source_token
    _databricks_domain_target = domain_target
    _access_token_target = token_target
    # 设置请求头，包括访问令牌
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    api_version = "2.0"
    # 构建 GET API请求URL
    url = f"{databricks_domain}/api/{api_version}/sql/warehouses"
    _api_version_target = "2.0"
    # 构建API请求URL
    _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/sql/warehouses/"
    response = requests.get(url, headers=headers)
    data = response.json()
    # 检查响应状态码
    if response.status_code == 200 and data.get('warehouses')!=None:
        # 解析JSON响应
        sql_clusters = data["warehouses"]
        #将clusters信息写入到json文件
        with open('sql_warehouses.json', 'w') as f:
            json.dump(sql_clusters, f, indent=4)
        #print(script_dir)    
        #file_path = os.path.join(script_dir, 'log/warehouses.log')
        export_dir = args.set_export_dir
        file_path = os.path.join(export_dir, f'{args.session}_api/warehouses.json')
        map_file_path = os.path.join(export_dir, f'{args.session}_api/warehouse_map.json')
        # file_path ='/home/lorealuser/Jimmy/cn3_01/try2_api/warehouse.log'
        # map_file_path = '/home/lorealuser/Jimmy/cn3_01/try2_api/warehouse_id_map.log'
        with open(file_path, "w", newline="") as txtfile, open(map_file_path, "w", newline="") as mapfile:
            for clusters in sql_clusters :
                txtfile.write(str(clusters)+ '\n')
                old_id = clusters['id']
                if clusters['name'] !='Serverless Starter Warehouse1':
                    name = clusters['name']
                    cluster_size = clusters['cluster_size']
                    min_num_clusters = clusters['min_num_clusters']
                    max_num_clusters = clusters['max_num_clusters']
                    auto_stop_mins=clusters['auto_stop_mins']
                    creator_name=clusters['creator_name']
                    tags=clusters['tags']
                    spot_instance_policy=clusters['spot_instance_policy']
                    warehouse_type=clusters['warehouse_type']
                    clusters['state']
                    enable_serverless_compute=clusters['enable_serverless_compute']
                    enable_photon =clusters['enable_photon']
                    
                    create_json = f"""
                    {{
                        "name": "{name}",
                        "cluster_size": "{cluster_size}",
                        "min_num_clusters": {min_num_clusters},
                        "max_num_clusters": {max_num_clusters},
                        "auto_stop_mins": "{auto_stop_mins}",
                        "creator_name": "{creator_name}",
                        "spot_instance_policy": "{spot_instance_policy}",
                        "enable_photon": {json.dumps(enable_photon)},
                        "enable_serverless_compute": {json.dumps(enable_serverless_compute)},
                        "warehouse_type": "{warehouse_type}",
                        "channel": {json.dumps(clusters.get('channel'))},
                        "tags": {json.dumps(clusters['tags'])}  
                        
                    }}
                    """     
                    _create_json = create_json
                    new_id = create_warehouse(url_target=_url_target,access_token_target= _access_token_target, warehouse_data=_create_json)
                mapfile.writelines(f'{{{old_id}:{new_id}}}')
                
    else:
        print(f"Error: {response.status_code} - {response.text}")
