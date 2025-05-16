import requests
import json
import os


# 创建sql warehouse
def create_vector(url_target, access_token_target, vector_data):
    headers_target = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    response_vector = requests.post(
        url_target,
        headers=headers_target,
        json=json.loads(vector_data)
    )
    
    if response_vector.ok:
        print("迁移请求成功")
    else:
        print("迁移请求失败：", response_vector.status_code, response_vector.text)
    return 
def call_migration_vector(source_domain,source_token,domain_target,token_target)->None:
    print('开始迁移vector search')
    databricks_domain = source_domain
    access_token = source_token
    _databricks_domain_target = domain_target
    _access_token_target = token_target
    api_version = "2.0"
    _api_version_target = "2.0"
    # 构建 GET API请求URL
    url = f"{databricks_domain}/api/{api_version}/vector-search/endpoints"
    _api_version_target = "2.0"
    # 构建API请求URL
    _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/vector-search/endpoints"

    # 设置请求头，包括访问令牌
    headers_source = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    # 发送Source GET请求
    script_dir = os.path.dirname(os.path.abspath(__file__))
    #file_path = os.path.join(script_dir, 'log/vector_search.log')
    file_path ='/home/ankeruser/aws/aws_workspace_config/try1_api/vector_search.log'
    response = requests.get(url, headers=headers_source)
    data = response.json()
    # 检查响应状态码
    if response.status_code == 200 and data.get('endpoints')!=None:
        # 解析JSON响应
        sql_endpoints = data["endpoints"]
        with open(file_path, "w", newline="") as txtfile:
            txtfile.write(str(sql_endpoints))
        #将clusters信息写入到json文件
        with open('sql_endpoints.json', 'w') as f:
            json.dump(sql_endpoints, f, indent=4)
            
        for endpoint in sql_endpoints :
            if endpoint['name'] !='Starter Warehouse':
                name = endpoint['name']
                endpoint_type = endpoint['endpoint_type']
                create_json = f"""
                {{
                    "name": "{name}",
                    "endpoint_type": "{endpoint_type}"
                    
                }}
                """     
                _create_json = create_json
                #print(_create_json)
                create_vector(url_target=_url_target,access_token_target= _access_token_target, vector_data=_create_json)
    
                
    else:
        print(f"Error: {response.status_code} - {response.text}")
    print('迁移vector search完成')    
