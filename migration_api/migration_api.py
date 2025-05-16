import configparser

import migration_aws_notebook_permissions as notebook_permissions
import migration_query_v5 as query
import migration_aws_sqlwarehouse as sqlwarehouse
import migration_aws_sqlwarehouse_permissions as sqlwarehouse_permissions
import migration_aws_pool_permissions as pool_permissions
import migration_aws_dashboard_permissions as dash_permissions
import migration_aws_query_permissions_v1 as query_permissions
import migration_aws_vectorsearch as vector
import migration_account_user_roles_to_group as group
import migration_aws_dlt_permissions as dlt_permissions
import migration_aws_dlt as dlt
import migration_catalog_permissions as catalog_permissions
import disabled_dataflow as disable_dataflow
import migration_libs as m_libs
import Exportworkspace_v2 as extwk
import migration_aws_group_sp_permissions as group_sp_permissions
import argparse
import sys
import os
from datetime import datetime

print("local script")
def get_login_credentials(creds_path='~/.databrickscfg', profile='DEFAULT'):
    config = configparser.ConfigParser()
    abs_creds_path = os.path.expanduser(creds_path)
    config.read(abs_creds_path)
    try:
        current_profile = dict(config[profile])
        if not current_profile:
            raise ValueError(f"Unable to find a defined profile to run this tool. Profile \'{profile}\' not found.")
        return current_profile
    except KeyError:
        raise ValueError(
            'Unable to find credentials to load for profile. Profile only supports tokens.')
# from modify_clustersize_attributes import *
parser = argparse.ArgumentParser(description='Run different functoin based on arguments.')

parser.add_argument('--profile_of_oldWS',required=True, action='store', default='DEFAULT',
                    help='Profile to parse the credentials')
parser.add_argument('--profile_of_newWS',required=True, action='store', default='DEFAULT',
                    help='Profile to parse the credentials')
parser.add_argument('--set-export-dir',required=True, action='store', default='DEFAULT',
                    help='set a export dir')

parser.add_argument('--session',required=True, action='store', default='DEFAULT',
                    help='Session')
# 添加 --dlt 参数
parser.add_argument('--dlt', action='store_true',
                    help='Run migation dlt and permissions.')
# 添加 --sqlwarehouse 参数
parser.add_argument('--sqlwarehouse', action='store_true',
                    help='Run migation sqlwarehouse and permissions.')
# 添加 --vector-search 参数
parser.add_argument('--vectorsearch', action='store_true',
                    help='Run migation vector-search.')
# 添加 --query 参数
parser.add_argument('--query', action='store_true',
                    help='Run migation query.')
# 添加 --acl_group 参数
parser.add_argument('--acl_group', action='store_true',
                    help='Run migation group  permissions.')
# 添加 --acl_group 参数
parser.add_argument('--acl_sp', action='store_true',
                    help='Run migation  ap permissions.')
# 添加 --catalog 参数
parser.add_argument('--catalog', action='store_true',
                    help='Run migation catalog/schema/table and permissions')
# 添加 --disablejob 参数
# parser.add_argument('--disablejob', action='store_true',
#                     help='Run disable job... ')
# 添加 --disablejob 参数
parser.add_argument('--libs', action='store_true',
                    help='Run migration libs.')
# 添加 --files 参数
parser.add_argument('--files', action='store_true',
                    help='Run migration files.')
# 添加 --dashboard 参数
parser.add_argument('--dashboards', action='store_true',
                    help='Run migration dashboards.')
# 添加 --dashboardacl 参数
parser.add_argument('--acl_dashboards', action='store_true',
                    help='Run migration acl_dashboards.')
# 添加 --Notebooks 参数
parser.add_argument('--notebooks', action='store_true',
                    help='Run migration notebooks.')
# 添加 --acl_pool 参数
parser.add_argument('--acl_pool', action='store_true',
                    help='Run migration pools''s permissions... ')

#解析参数
# databricks_domain = ""
# access_token = ""
# _databricks_domain_target = ""
# _access_token_target = ""
# account_id_source = ""
# account_token_source = ""
# account_id_target  = ""
# account_token_target = ""
# script_dir = os.path.dirname(os.path.abspath(__file__))
# file_path = os.path.join(script_dir, 'config.txt')
# with open(file_path, 'r') as file:
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
#             if key == 'target_databricks_domain':
#                 _databricks_domain_target = value
#             if key == 'target_access_token':
#                 _access_token_target = value
#             if key == 'account_id_source':
#                 account_id_source = value
#             if key == 'account_token_source':
#                 account_token_source = value
#             if key == 'account_id_target':
#                 account_id_target = value
#             if key == 'account_token_target':
#                 account_token_target = value
#                 break
args = parser.parse_args()
old_ws_login_credentials = get_login_credentials(profile=args.profile_of_oldWS)
new_ws_login_credentials = get_login_credentials(profile=args.profile_of_newWS)
databricks_domain = old_ws_login_credentials['host']
access_token = old_ws_login_credentials['token']
_databricks_domain_target = new_ws_login_credentials['host']
_access_token_target = new_ws_login_credentials['token']
account_login_credentials = get_login_credentials(profile='ACCOUNT')
account_id_source = account_login_credentials['account_id']
account_token_source = account_login_credentials['token']
account_id_target  = account_id_source
account_token_target = account_token_source

print("Load Credentials Successful")
# 根据参数执行相应的功能
if args.dlt:
    print("start migation dlt and permissions..."+"at:"+str(datetime.now()))
    dlt.call_migration_dlt(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target)
    dlt_permissions.call_migratoin_dlt_permissions(databricks_domain,access_token,_databricks_domain_target,_access_token_target)
    print("completed migation dlt and permissions..."+"at:"+str(datetime.now()))
elif args.sqlwarehouse:
    print("start migation sqlwarehouse and permissions..."+"at:"+str(datetime.now()))
    sqlwarehouse.call_migratoin_sqlwarehouse(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target,args=args)
    sqlwarehouse_permissions.call_migratoin_sql_permission(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target,args=args)
    print("completed sqlwarehouse and permissions..."+"at:"+str(datetime.now()))
elif args.vectorsearch:
    print("start migation vector search..."+"at:"+str(datetime.now()))
    vector.call_migration_vector(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target)
    print("completed vector search..."+"at:"+str(datetime.now()))
elif args.query:
    print("start migation query..."+"at:"+str(datetime.now()))
    query.call_migration_query(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target)
    print("completed query..."+"at:"+str(datetime.now()))
    print("start migration acl_query..."+"at:"+str(datetime.now()))
    query_permissions.call_migration_acl_query(source_host=databricks_domain,source_token=access_token,target_host=_databricks_domain_target,target_token=_access_token_target)
    print("completed query..."+"at:"+str(datetime.now()))
elif args.acl_group:
    print("start migation acl group ..."+"at:"+str(datetime.now()))
    group_sp_permissions.call_migratoin_group_sp_permissions(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target,account_id_source=account_id_source,account_id_target=account_id_target,type="groups")
    print("completed migation group ..."+"at:"+str(datetime.now()))
elif args.acl_sp:
    print("start migation acl servicePrincipals..."+"at:"+str(datetime.now()))
    group_sp_permissions.call_migratoin_group_sp_permissions(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target,account_id_source=account_id_source,account_id_target=account_id_target,type="servicePrincipals")
    print("completed migation servicePrincipals..."+"at:"+str(datetime.now()))    
elif args.catalog:
    print("start migation catalog/schema/table and permissions..."+"at:"+str(datetime.now()))
    catalog_permissions.call_migration_catalog_permissions(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target)
    print("completed catalog/schema/table and permissions..."+"at:"+str(datetime.now()))
elif args.libs:
    print("start migration libs..."+"at:"+str(datetime.now()))
    m_libs.call_migration_cluster_libs(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target)
    print("completed migration libs..."+"at:"+str(datetime.now())) 
elif args.files:
    print("start migration files..."+"at:"+str(datetime.now()))
    extwk.call_exportworkspace(source_host=databricks_domain,source_token=access_token,target_host=_databricks_domain_target,target_token=_access_token_target,filetye="FILE",args=args)
    print("completed migration files..."+"at:"+str(datetime.now()) )   
elif args.dashboards:
    print("start migration dashboards..."+"at:"+str(datetime.now()))
    extwk.call_exportworkspace(source_host=databricks_domain,source_token=access_token,target_host=_databricks_domain_target,target_token=_access_token_target,filetye="DASHBOARD")
    print("completed migration dashboards..."+"at:"+str(datetime.now()) )
elif args.acl_dashboards:
    print("start migration acl_dashboards..."+"at:"+str(datetime.now()))
    dash_permissions.call_migration_acl_dashboard(source_host=databricks_domain,source_token=access_token,target_host=_databricks_domain_target,target_token=_access_token_target)
    print("completed migration acl_dashboards..."+"at:"+str(datetime.now()))    
elif args.notebooks:
    print(f"start migration notebooks..."+"at:"+str(datetime.now()))
    extwk.call_exportworkspace(source_host=databricks_domain,source_token=access_token,target_host=_databricks_domain_target,target_token=_access_token_target,filetye="NOTEBOOK")
    print(f"completed migration notebooks..."+"at:"+str(datetime.now()))       
    print(f"start migration notebooks permissions..."+"at:"+str(datetime.now()))
    notebook_permissions.call_migration_acl_notebook(target_host=_databricks_domain_target,target_token=_access_token_target)
    print(f"completed migration notebooks permissions..."+"at:"+str(datetime.now()))
elif args.acl_pool:
    print("start migation pools 's permissions..."+"at:"+str(datetime.now()))
    pool_permissions.call_migratoin_pool_permissions(source_domain=databricks_domain,source_token=access_token,domain_target=_databricks_domain_target,token_target=_access_token_target)
    print("completed pools 's permissions..."+"at:"+str(datetime.now()))            
else:
    # 如果没有参数，提示用户输入参数
    print("Please provide an argument. Use -h or --help for more information.")
    sys.exit(1)
