import requests
import os
import base64
import urllib.parse
import json
import concurrent.futures
from datetime import datetime
script_dir = os.path.dirname(os.path.abspath(__file__))
def get_ws_acls(obj_id,obj_type,_domain,_token):
    """
    Export all cluster permissions for a specific cluster id
    :return:
    """
    headers = {'Authorization': f'Bearer {_token}'}
    url = f'{_domain}/api/2.0/permissions/{obj_type}/{obj_id}/'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        acl_list = response.json()
        return acl_list
    else:
        return None
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
        params = {
            "path": f"/{export_path}"
        }
        
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

def trans_download_upload(soure_host,source_token,target_host,target_token,filtered_list,filetype,args):
    databricks_host = soure_host
    token =source_token
    databricks_host_t =target_host
    token_t =target_token
    format = "AUTO"
    file = filtered_list
    # for file in filtered_list:
    path = file.get('path').rstrip('\n')

    export_dir = args.set_export_dir
    save_file_path = os.path.join(export_dir, f'{args.session}_api/artifacts/')+os.path.dirname(path)
    if not os.path.exists(save_file_path):
        os.makedirs(save_file_path)
        print(f"Directory created: {save_file_path}")
    resp=download_file(databricks_host, token,path)
    if "10485760" not in resp:
        filename = os.path.basename(path)
        print(filename)
        save_filename = save_file_path.replace('anker.com','anker-in.com')+"/" +filename
        #写文件
        if 'ads_search_term_11' not in save_filename:
            if 'bi_ap_kk_invoices__view_rename_1' not in save_filename:
                with open(save_filename, "wb") as f:
                    f.write(base64.b64decode(resp.json().get('content')))
                #读文件    
                fp = open(save_filename, "rb")      
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
def call_exportworkspace(source_host,source_token,target_host,target_token,args,filetye=None):
    if filetye is None:
        filetye="FILE"
    databricks_host= source_host
    token = source_token
    databricks_host_t =target_host
    token_t = target_token
    export_dir = args.set_export_dir

    if filetye.lower()=="notebook":
        acl_path = os.path.join(export_dir, f'{args.session}/acl_notebooks.log')

        workspace_path =os.path.join(export_dir, f'{args.session}/user_workspace.log')
    else:
        acl_path =os.path.join(export_dir, f'{args.session}/acl_{filetye.lower()}.log')
        workspace_path =os.path.join(export_dir, f'{args.session}/{filetye.lower()}.log')
    export_path = "/"


    def recursive_export_objects(current_path:str,acl_file,wsfile):
        objects_in_current_path = export_workspace(databricks_host, token,current_path)
        sub_folder_list = filter_workspace_items(objects_in_current_path, 'DIRECTORY')
        objects_list = filter_workspace_items(objects_in_current_path, filetye)
        for data in objects_list:
            if filetye.lower()=="notebook":
                obj_id =data.get('object_id')
                alc_list=get_ws_acls(obj_id,'notebooks',source_host,source_token)
                alc_list['name'] = data.get('path')
                aclfile.write(str(json.dumps(alc_list))+"\n")
            wsfile.write(str(json.dumps(data))+"\n")
            trans_download_upload(soure_host=databricks_host,source_token=token,target_host=databricks_host_t,target_token=token_t ,filtered_list=data,filetype=filetye,args=args)

        # objects_total_list.extend(objects_list)
        for sub_folder in sub_folder_list:
            recursive_export_objects(sub_folder.get('path'),acl_file,wsfile)





    
    with open(acl_path, 'w') as aclfile, open(workspace_path, 'w') as wsfile:
        recursive_export_objects(export_path,aclfile,wsfile)
