import requests
import json
import csv
import os
import datetime

# 获取当前日期和时间
now = datetime.datetime.now()
# 格式化日期和时间
formatted_date = now.strftime("%Y%m%d_%H%M%S")

# 替换为你的Databricks域名、访问令牌和版本号

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
#             if key == '_databricks_domain':
#                 _databricks_domain_target = value   
#             if key == '_access_token':
#                 _access_token_target = value
#                 break  

#Start构造创建dlt函数
# 替换为你的target Databricks域名、访问令牌和版本号
# 构建API请求URL

# 创建DLT
def disable_dataflow(df_data,domain_target,token_target):
    access_token_target = token_target
    _databricks_domain_target = domain_target
    headers = {
        "Authorization": f"Bearer {access_token_target}",
        "Content-Type": "application/json"
    }
    url_target = f"{_databricks_domain_target}/api/2.1/jobs/update"
    response = requests.post(
        url_target,
        headers=headers,
        json=json.loads(df_data)
    )
    
    if response.ok:
        print("请求成功")
    else:
        print("请求失败：", response.status_code, response.text)
    return 
#end 构造创建dlt函数
def call_disable_job(domain_target,token_target):
    _databricks_domain_target = domain_target
    _access_token_target = token_target
    _api_version_target = "2.1"
    # 设置请求头，包括访问令牌
    headers = {
        "Authorization": f"Bearer {_access_token_target}",
        "Content-Type": "application/json"
    }
    _url_target = f"{_databricks_domain_target}/api/{_api_version_target}/jobs/list"
    init_flag = False
    # 发送GET请求
    while True:
        if init_flag:
            response = requests.get(_url_target, headers=headers, params={'page_token':data.get('next_page_token')})
        else:
            response = requests.get(_url_target, headers=headers)
        data = response.json()
        
        init_flag=True
        # 检查响应状态码
        if response.status_code == 200 and data.get('jobs') != '' and data.get('jobs') != None:
            contents = data["jobs"]
            filename = f"disabled_job{formatted_date}.txt"
            with open(filename, "w", newline="") as onlytrigger:
                for content in contents:
                    job_id = content.get('job_id')
                    settings = content.get('settings')
                    if settings.get('trigger') !=None  and settings.get('schedule')==None:
                        if settings.get('trigger').get('pause_status')=="UNPAUSED" :
                                #print(job_id,settings.get('trigger'))
                            create_json = f"""
                                {{
                                "job_id": "{job_id}",
                                "new_settings": {json.dumps(settings).replace('UNPAUSED','PAUSED')}
                                }}
                                """  
                            old_json = f"""
                                {{
                                "job_id": "{job_id}",
                                "new_settings": {json.dumps(settings)}
                                }}
                                """     
                            onlytrigger.write(old_json)
                            onlytrigger.write('\n')
                            
                            disable_dataflow(df_data=create_json,domain_target = _databricks_domain_target,token_target= _access_token_target)
                            #print("onlytrigger")
                    if settings.get('schedule') !=None and settings.get('trigger')==None:
                        if settings.get('schedule').get('pause_status')=="UNPAUSED" :
                            create_json = f"""
                            {{
                            "job_id": "{job_id}",
                            "new_settings": {json.dumps(settings).replace('UNPAUSED','PAUSED')}
                            }}
                            """  
                            old_json = f"""
                                {{
                                "job_id": "{job_id}",
                                "new_settings": {json.dumps(settings)}
                                }}
                                """     
                            onlytrigger.write(old_json)
                            onlytrigger.write('\n')
                            
                            disable_dataflow(df_data=create_json)
                            #print("onlyschedule")
                    
                    if settings.get('schedule') !=None and settings.get('trigger') !=None:
                        if settings.get('schedule').get('pause_status')=="UNPAUSED" and settings.get('schedule').get('pause_status')=="UNPAUSED":
                            create_json = f"""
                            {{
                            "job_id": "{job_id}",
                            "new_settings": {json.dumps(settings).replace('UNPAUSED','PAUSED')}
                            }}
                            """  
                            old_json = f"""
                                {{
                                "job_id": "{job_id}",
                                "new_settings": {json.dumps(settings)}
                                }}
                                """     
                            onlytrigger.write(old_json)
                            onlytrigger.write('\n')
                            
                            disable_dataflow(df_data=create_json,domain_target = _databricks_domain_target,token_target= _access_token_target)
                            #print("UNPAUSED schedule but pAUSED trigger")
                        if settings.get('schedule').get('pause_status')!="UNPAUSED" and settings.get('schedule').get('pause_status')=="UNPAUSED":
                            create_json = f"""
                            {{
                            "job_id": "{job_id}",
                            "new_settings": {json.dumps(settings).replace('UNPAUSED','PAUSED')}
                            }}
                            """  
                            old_json = f"""
                                {{
                                "job_id": "{job_id}",
                                "new_settings": {json.dumps(settings)}
                                }}
                                """     
                            onlytrigger.write(old_json)
                            onlytrigger.write('\n')
                            
                            disable_dataflow(df_data=create_json,domain_target = _databricks_domain_target,token_target= _access_token_target)
                            #print("PAUSED schedule but unpAUSED trigger")
                        if settings.get('schedule').get('pause_status')=="UNPAUSED" and settings.get('schedule').get('pause_status')!="UNPAUSED":
                            create_json = f"""
                            {{
                            "job_id": "{job_id}",
                            "new_settings": {json.dumps(settings).replace('UNPAUSED','PAUSED')}
                            }}
                            """ 
                            old_json = f"""
                                {{
                                "job_id": "{job_id}",
                                "new_settings": {json.dumps(settings)}
                                }}
                                """     
                            onlytrigger.write(old_json)
                            onlytrigger.write('\n')
                            
                            disable_dataflow(df_data=create_json,domain_target = _databricks_domain_target,token_target= _access_token_target)
                            #print("UNPAUSED schedule but unpAUSED trigger")    
                        
                    # _create_json = create_json
                    
                    print("--------")
                #create_dlt(url_target=_url_target,access_token_target= _access_token_target, dlt_data=_create_json)
        else:
            print(f"Error: {response.status_code} - {response.text}")

        if data.get('next_page_token') == None or data.get('next_page_token') == '':
            break
