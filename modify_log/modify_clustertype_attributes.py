import re
import fileinput
import os
import json
import shutil
#define 日志路径
# 假设变量 drive 和 path 已经定义
log_directory = "/home/ankeruser/aws/aws_workspace_config/try1/"
log_mail_address = "/home/ankeruser/aws/modify_log/mail.log"
# change cluster_type
def translate_cluster_types_toazure(log_dir):
    # log files to adapt the Azure cluster types
    logs_to_update = [
        "clusters.log",
        "cluster_policies.log",
        "instance_pools.log",
        "jobs.log",
    ]

    # for the sake of simplicity, we map all Azure cluster types to 'i3.xlarge'
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
    '''
    azure_aws_type_mapping = {
        "m4.xlarge":target_cluster_type,
        "m4.large":target_cluster_type,
        "r5dn.large":target_cluster_type,
        "r5dn.12xlarge":target_cluster_type,
        "c5.2xlarge":target_cluster_type,
        "m5d.2xlarge":target_cluster_type,
        "i3.xlarge":target_cluster_type,
        "m5d.2xlarge":target_cluster_type,
        "c5.xlarge":target_cluster_type,
        "i3.2xlarge":target_cluster_type,
        "i3.4xlarge":target_cluster_type,
        "i3en.xlarge":target_cluster_type,
        "g4dn.xlarge":target_cluster_type,
        "r5a.large":target_cluster_type,
        "i3.xlarge":target_cluster_type,
        "r5d.4xlarge":target_cluster_type,
        "i3.xlarge":target_cluster_type,
        "m5d.xlarge":target_cluster_type,
        "i3.xlarge":target_cluster_type,
        "m5.large":target_cluster_type,
        "m5d.large":target_cluster_type,
        "r6id.xlarge":target_cluster_type,
    }
    '''
    cnt =0 
    for logfile in logs_to_update:
        with fileinput.FileInput(log_dir + logfile, inplace=True) as fp:
            for line in fp:
                for old, new in azure_aws_type_mapping.items():
                    line = line.replace(old, new)
                    line = re.sub('"availability": "SPOT_WITH_FALLBACK"', '"availability": "SPOT_WITH_FALLBACK_AZURE"', line)
                    line = re.sub('"availability": "SPOT"', '"availability": "SPOT_AZURE"', line)
                    line = re.sub('"availability": "ON_DEMAND"', '"availability": "ON_DEMAND_AZURE"', line)
                # update log file with new values
                print(line, end="")
        
# change attribute from aws to azure                
def translate_instance_pool_attributes_toazure(log_dir):
    # log files to adapt instance pools attributes
    logs_to_update = ["instance_pools.log", "jobs.log","clusters.log","cluster_policies.log"]
      # log files to adapt instance pools attributes
    for logfile in logs_to_update:
        with fileinput.FileInput(log_dir + logfile, inplace=True) as fp:
            for line in fp:
                # adapt attribute prefix
                line = line.replace("aws_attributes", "azure_attributes")
                # remove Azure suffix
                line = line.replace("_AWS", "")
                # adapt spot bid price configuration to AWS syntax with 
                # default value of 100%
                line = re.sub(
                    '"spot_bid_max_price": -?\d*\.?\d*',
                    '"spot_bid_price_percent": 100',
                    line,
                )
                line = re.sub('"availability": "SPOT"', '"availability": "SPOT_AZURE"', line)
                # update log file with new values
                print(line, end="")      
#指定更新个人mail信息
def _update_email_address(log_dir, old_email_address, new_email_address):
    # NOTE: the original implementation of `update_email_addresses` only
    # updates a subset of the files below, but we expect that the email
    # addresses need to be adapted in all files where they can be found.
    logs_to_update = [
        "users.log",
        "acl_jobs.log",
        "acl_clusters.log",
        "acl_cluster_policies.log",
        "acl_notebooks.log",
        "acl_directories.log",
        "acl_repos.log",
        "clusters.log",
        "jobs.log",
        "repos.log",
        "secret_scopes_acls.log",
        "user_dirs.log",
        "user_name_to_user_id.log",
        "user_workspace.log",
    ]

    for logfile in logs_to_update:
        # copying the file
        source_path = log_dir + logfile
        if os.path.exists(source_path):
            with fileinput.FileInput(source_path, inplace=True) as fp:
                for line in fp:
                    # the inline replacement of FileInput writes back the changes on
                    # the line with calling "print"
                    print(line.replace(old_email_address, new_email_address), end="")

    # update the path for user notebooks in bulk export mode
    bulk_export_dir = log_dir + "artifacts/Users/"
    for dirpath in os.listdir(bulk_export_dir):
        if old_email_address in dirpath:
                new_folder_name = dirpath.replace(old_email_address, new_email_address)
                old_folder_path = os.path.join(bulk_export_dir, dirpath)
                new_folder_path = os.path.join(bulk_export_dir, new_folder_name)
                try:
                    if os.path.exists(new_folder_path):
                        shutil.move(old_folder_path, new_folder_path)
                        print(f"Merged file: {old_folder_path} -> {new_folder_path}")
                    else:    
                        os.rename(old_folder_path, new_folder_path)
                        print(f"Renamed '{dirpath}' to '{new_folder_name}'")
                except OSError as e:
                    print(f"Error renaming folder '{dirpath}': {e}")
    old_bulk_export_dir = bulk_export_dir + old_email_address
    new_bulk_export_dir = bulk_export_dir + new_email_address
    if os.path.exists(old_bulk_export_dir):
        os.rename(old_bulk_export_dir, new_bulk_export_dir)

    # update the path for user notebooks in single user export mode
    single_user_dir = log_dir + "user_exports/"
    old_single_user_dir = single_user_dir + old_email_address
    new_single_user_dir = single_user_dir + new_email_address
    if os.path.exists(old_single_user_dir):
        os.rename(old_single_user_dir, new_single_user_dir)
    old_single_user_nbs_dir = (
        new_single_user_dir + "/user_artifacts/Users/" + old_email_address
    )
    new_single_user_nbs_dir = (
        new_single_user_dir + "/user_artifacts/Users/" + new_email_address
    )
    if os.path.exists(old_single_user_nbs_dir):
        os.rename(old_single_user_nbs_dir, new_single_user_nbs_dir)

    # update email in groups
    groups_path = log_dir + "groups"
    group_files = [
        f
        for f in os.listdir(groups_path)
        if os.path.isfile(os.path.join(groups_path, f))
    ]
    for f in group_files:
        group_file_path = os.path.join(groups_path, f)
        _update_email_address_in_file(
            group_file_path, old_email_address, new_email_address
        )

    print(f"Update email address {old_email_address} to {new_email_address}")


def _update_email_address_in_file(source_path, old_email_address, new_email_address):
    with fileinput.FileInput(source_path, inplace=True) as fp:
        for line in fp:
            # the inline replacement of FileInput writes back the changes on
            # the line with calling "print"
            print(line.replace(old_email_address, new_email_address), end="")

# 调用函数，传入日志文件目录
#print("start change attributes")
#translate_instance_pool_attributes_toazure(log_directory) 
#print("end change attributes")  
print("start change cluster_type")
translate_cluster_types_toazure(log_directory)  
print("end change cluster_type")  

print("start change attributes")
translate_instance_pool_attributes_toazure(log_directory)
print("end change attributes")

print("start change mail")
with open(log_mail_address, 'r') as file:
     for line in file:
         old_mail, new_mail = line.strip().split(',')  # 假设替换对是用逗号分隔的
         print(old_mail,new_mail)
         _update_email_address(log_directory,old_mail,new_mail)  
#print("end change mail")                  
