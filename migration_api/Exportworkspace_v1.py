import requests
import os
import base64
import urllib.parse
import json
import concurrent.futures
from datetime import datetime
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
#             if key == '_databricks_domain':
#                 _databricks_domain_target = value   
#             if key == '_access_token':
#                 _access_token_target = value
                # break  
# databricks_host = "https://adb-4145924773612620.0.azuredatabricks.net"
# token = "dapic6d4c1d9850a23087423bafdb3b3f4db"
# databricks_host_t = "https://adb-4145924773612620.0.azuredatabricks.net"
# token_t = "dapic6d4c1d9850a23087423bafdb3b3f4db"
def my_map(F, items):
    to_return = []
    for elem in items:
        to_return.append(F(elem))
    return to_return
def export_workspace(databricks_host,token,export_path):
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        url_2 = f"{databricks_host}/api/2.0/workspace/list"
        params = {"path": export_path}
        response = requests.get(url_2, headers=headers,params= params)
        if response.status_code == 200:
            return response.json().get('objects', [])  # 返回导出的数据
        else:
            print(f"Failed to export workspace: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
         
def download_file(databricks_host, token,export_path):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    print(export_path)
    url_1 = f"{databricks_host}/api/2.0/workspace/export"
    params = {
        "path": export_path,
        "format":"AUTO"
    }
    response = requests.get(url_1, headers=headers,params= params)
    if response.status_code == 200:
        return response# 返回导出的数据
    else:
        print(f"Failed to export workspace: {response.status_code} - {response.text}")
        return response.text
def upload_file(databricks_host_, token_,in_args):
    headers = {
        "Authorization": f"Bearer {token_}",
        "Content-Type": "application/json"
    }
    url_1 = f"{databricks_host_}/api/2.0/workspace/import"
    params =  json.loads(json.dumps(in_args).replace('anker.com','anker-in.com'))
    
    response = requests.post(url_1, headers=headers,json= params)
    if response.status_code == 200:
        return response
    else:
        print(f"Failed to import file: {response.status_code} - {response.text}")
        return None
def upload_mkdir(databricks_host_, token_,in_args):
    headers = {
        "Authorization": f"Bearer {token_}",
        "Content-Type": "application/json"
    }
    url_1 = f"{databricks_host_}/api/2.0/workspace/mkdirs"
    params =  {
         "path": in_args
    }
    response = requests.post(url_1, headers=headers,params= params)
    if response.status_code == 200:
        return response
    else:
        print(f"Failed to import file: {response.status_code} - {response.text}")
        return None    
    
def filter_workspace_items(item_list, item_type):
    """
    Helper function to filter on different workspace types.
    :param item_list: iterable of workspace items
    :param item_type: DIRECTORY, NOTEBOOK, LIBRARY       
    :return: list of items filtered by type
    """
    # print(item_list,item_type)
    # supported_types = {'DIRECTORY', 'NOTEBOOK', 'LIBRARY',"FILE"}
    # if item_type not in supported_types:
    #     raise ValueError('Unsupported type provided: {0}.\n. Supported types: {1}'.format(item_type,
    #                                                                                           str(supported_types)))
    filtered_list = list(my_map(lambda y: {'path': y.get('path', None),
                                                    'object_id': y.get('object_id', None),'language': y.get('language', None)},
                                         filter(lambda x: x.get('object_type', None) == item_type, item_list)))
    return filtered_list  

def trans_download_upload(soure_host,source_token,target_host,target_token,filtered_list,filetype):
    databricks_host = soure_host
    token =source_token
    databricks_host_t =target_host
    token_t =target_token
    format = "AUTO"
    for file in filtered_list:
        path = file.get('path').rstrip('\n')
        #save_file_path = script_dir+"/log/"+os.path.dirname(path)
        save_file_path = '/home/ankeruser/aws/aws_workspace_config/try1_api'+os.path.dirname(path)
        if not os.path.exists(save_file_path):
            os.makedirs(save_file_path)
            print(f"Directory created: {save_file_path}")
        resp=download_file(databricks_host, token,path)
        if "10485760" not in resp:
            filename=urllib.parse.quote(str(os.path.basename(path)))
            save_filename = save_file_path+"/" +filename
            #save_filename = urllib.parse.quote(str(os.path.basename(path)))
            #写文件
            if 'ads_search_term_11' not in save_filename:
                if 'bi_ap_kk_invoices__view_rename_1' not in save_filename:
                    with open(save_filename.replace('@anker.com','@anker-in.com'), "wb") as f:
                        f.write(base64.b64decode(resp.json().get('content')))
                    #读文件    
                    fp = open(save_filename.replace('@anker.com','@anker-in.com'), "rb")      
                    in_args = {
                                "content": base64.encodebytes(fp.read()).decode('utf-8'),
                                "path": path  
                            } 
                    if filetype=="NOTEBOOK":
                        format= "SOURCE"
                        language =file.get('language',None)
                        if language is not None:
                            in_args['language'] = language
                    else:
                        format= "AUTO"
                    in_args['format'] = format
                    in_args['overwrite'] = 'true'
                    #in_args['object_type'] = 'FILE'
                    resp_upload = upload_file(databricks_host_t, token_t,in_args)  
                   # return (resp_upload.json())
# Databricks REST API的URL和您的访问令牌
# 导出工作区
def call_exportworkspace(source_host,source_token,target_host,target_token,filetye=None):
    if filetye is None:
        filetye="FILE"
    databricks_host= source_host
    token = source_token
    databricks_host_t =target_host
    token_t = target_token
    export_path = "/" 
    exported_data = export_workspace(databricks_host, token,export_path)
    export_paths=[]
    if exported_data:
            root_items = exported_data
            folders= filter_workspace_items(root_items, 'DIRECTORY')
            ws_path = ""
            path =""
            for tld_obj in folders:
                # obj has 3 keys, object_type, path, object_id
                ws_path = tld_obj.get('path')
                export_paths.append(ws_path)   
                #print('ws_path',ws_path) 
                        
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(
                    lambda path: export_workspace(databricks_host, token, path),
            export_paths
                ))
            #print(results)
            for result in results:
                        #
                        if result is not None and len(result)>=1:
                            data =filter_workspace_items(result,'DIRECTORY')
                            filterfile =filter_workspace_items(result,filetye)
                            DIRECTORY = [item['path'] for item in data if 'path' in item]
                            if len(filterfile)>0:
                                trans_download_upload(soure_host=databricks_host,source_token=token,target_host=databricks_host_t,target_token=token_t ,filtered_list=filterfile,filetype=filetye)
                            if len(DIRECTORY) >=1:
                                with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
                                    results_ = list(executor.map(
                                                lambda path: export_workspace(databricks_host, token, path),DIRECTORY
                                            ))     
                            results_=[sublist for sublist in results_ if sublist] 
                            if len(results_) >=1:
                                for i in  results_:    
                                    u_item=filter_workspace_items(i,filetye)
                                    trans_download_upload(soure_host=databricks_host,source_token=token,target_host=databricks_host_t,target_token=token_t ,filtered_list=u_item,filetype=filetye)
                                 
#call_exportworkspace(source_host="https://dbc-1c15bce2-47da.cloud.databricks.com",source_token="dapi62958dd57a824a0fe6c80f027bbd3b3c",target_host="https://adb-4145924773612620.0.azuredatabricks.net",target_token="dapia3063c27ba4269ac2bd7aefcd1338b3b",filetye="DASHBOARD")     
            
          
