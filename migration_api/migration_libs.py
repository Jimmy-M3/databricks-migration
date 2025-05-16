import os
import requests
script_dir = os.path.dirname(os.path.abspath(__file__))
#file_path = os.path.join(script_dir, 'log/libs.log')
def process_target_cluster(instance_target, token_target):
    init_flag = False
    cluster_list = {}
    _url = f'{instance_target}/api/2.0/clusters/list'
    _headers = {
            'Authorization': f'Bearer {token_target}'
        }
    while True:
        if init_flag:
            clusterlist_response = requests.get(_url, headers=_headers, params={'page_token':data.get('next_page_token')})
        else:
            clusterlist_response = requests.get(_url, headers=_headers)

        data=clusterlist_response.json()
        cls=data.get('clusters')
        init_flag = True
        if clusterlist_response.status_code == 200 :
            cluster_list.update({cluster['cluster_name']:cluster['cluster_id'] for cluster in cls})
        else:
            cluster_list['error'] = f"Error: {clusterlist_response.status_code} - {clusterlist_response.text}"
            break  # 遇到错误时退出循环
        if data.get('next_page_token') is None:
            break  # 没有更多的分页时退出循环

    return cluster_list

def get_cluster_name(cluster_id,token_source,instance_source)->str:
    cluster_url = f'{instance_source}/api/2.1/clusters/get'
    cluster_headers = {
            'Authorization': f'Bearer {token_source}'
        }
    cluster_response = requests.get(cluster_url, headers=cluster_headers,params={"cluster_id":cluster_id})  
    cluster_name ="" 
    if cluster_response.status_code ==200:
        cluster= cluster_response.json()
        cluster_name =cluster.get('cluster_name')
        
    return cluster_name
def  get_cluster_list(token_target,instance_target):
    _url = f'{instance_target}/api/2.0/clusters/list'
    _headers = {
            'Authorization': f'Bearer {token_target}'
        }
    clusterlist_response = requests.get(_url, headers=_headers)  
    clusterlist=clusterlist_response.json()
    cls=clusterlist.get('clusters')
    
    # clusters =list(filter(lambda x: x % 2 == 0, clusterlist['clusters']))
    return {cluster['cluster_name']:cluster['cluster_id'] for cluster in cls}
    
def trans_libs_to_newcluster(token_target,instance_target,json_data):
    _headers = {
        'Authorization': f'Bearer {token_target}'
    }
    retun_code =""
    add_url = f'{instance_target}/api/2.0/libraries/install'
    libadd_response = requests.post(add_url, headers=_headers,json= json_data)
    if(libadd_response.status_code==200):
        retun_code =f"lib迁移成功"+ str(json_data)
    else:
        retun_code = libadd_response.text
    return retun_code    
                            

def call_migration_cluster_libs(source_domain,source_token,domain_target,token_target,args):
    
   # cls = get_cluster_list(token_target=ACCESS_TOKEN,instance_target=DATABRICKS_INSTANCE)
    cluster_list = process_target_cluster(token_target=token_target,instance_target=domain_target)
    libs_url = f'{source_domain}/api/2.0/libraries/all-cluster-statuses'
    headers = {
        'Authorization': f'Bearer {source_token}'
    }
    # u = f"https://{DATABRICKS_INSTANCE}/api/2.0/libraries/list"
    # res = requests.get(u, headers=headers)
    # print(res)
    # 发送 GET 请求
    libs_response = requests.get(libs_url, headers=headers)
    # 检查请求是否成功

    export_dir = args.set_export_dir
    file_path = os.path.join(export_dir, f'{args.session}_api/libs.log')
    with open(file_path, "w", newline="") as txtfile:
        if libs_response.status_code == 200:
            libs = libs_response.json()['statuses']
            for lib in libs:
                source_cluster_id = lib.get('cluster_id')
                library_statuses = lib.get('library_statuses')
                if library_statuses is not None:
                    _library_statuses = []
                    for lib in library_statuses:
                        #print(lib)
                        temp_lib = {}
                        lib =lib.get('library')
                        pypi = lib.get('pypi')
                        if pypi is not None:
                            temp_lib['pypi'] = pypi
                        jar = lib.get('jar')
                        if jar is not None:
                            temp_lib['jar'] = jar
                        whl = lib.get('whl')
                        if whl is not None:
                            temp_lib['whl'] = whl
                        maven = lib.get('maven')
                        if maven is not None:
                            temp_lib['maven'] = maven
                        cran = lib.get('cran')
                        if cran is not None:
                            temp_lib['cran'] = cran
                        requirements = lib.get('requirements')
                        if requirements is not None:
                            temp_lib['requirements'] = requirements
                        if temp_lib:
                            _library_statuses.append(temp_lib)

                    cluster_name = get_cluster_name(token_source=source_token,instance_source=source_domain ,cluster_id=source_cluster_id)
                    target_cluster_id=cluster_list.get(cluster_name)
                    #print(target_cluster_id,source_cluster_id)
                    final_json = {
                            "cluster_id": target_cluster_id,
                            "libraries": _library_statuses
                        }
                    txtfile.write(str(final_json))
                    retun_result =trans_libs_to_newcluster(token_target=token_target,instance_target=domain_target ,json_data =final_json)
                    print(retun_result)  
            
        else:
            print(f'Error: {libs_response.status_code}')
        
        

#migration_cluster_libs()        
  


