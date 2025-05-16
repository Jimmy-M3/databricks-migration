import requests
import json
import os
#迁移group到azure
def trans_group_to_traget(account_id_target,token_target,group_name):
    _account_id_target = account_id_target
    _token_target = token_target
    _group_name = group_name
    headers_target = {
    'Authorization': f'Bearer {_token_target}',
    'Content-Type': 'application/scim+json'
}
    url = f'https://accounts.azuredatabricks.net/api/2.0/accounts/{_account_id_target}/scim/v2/Groups'
    payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "displayName": _group_name,  # 您希望创建的组的名称
        
    }
   #print(payload)
    # 发送POST请求
    response = requests.post(url, headers=headers_target, data=json.dumps(payload))
    # 检查响应
    if response.status_code == 201:
        print("Group created successfully:", response.json())
    else:
        print("Failed to create group:", response.status_code, response.text)
    return
def call_migraton_group(account_id_source,account_token_source,account_id_target,account_token_target):
    # 替换为你的Databricks域名、访问令牌和版本号
    # account_id = "e90446ad-6591-4466-a2d2-f70c4d3200be"
    # token_source = "dsapi0eb7669e7153ce1bccf746ce0e5796ac"
    # account_id_target = "48573fef-e064-4a42-9437-41eee5fdaa96"
    # token_target = "dsapi5d83fbace8134d966ae8f31639f59124"
    account_id = account_id_source
    token_source = account_token_source
    account_id_target = account_id_target
    token_target = account_token_target
    # Databricks SCIM API 端点
    users_url_target = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id_target}/scim/v2/Users"
    groups_url_target = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id_target}/scim/v2/Groups"
    groups_url_source = f"https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}/scim/v2/Groups"
    # 设置请求头
    headers_target = {
        "Authorization": f"Bearer {token_target}",
        "Content-Type": "application/json"
    }
    headers_source = {
        "Authorization": f"Bearer {token_source}",
        "Content-Type": "application/json"
    }
    response_source = requests.get(groups_url_source, headers=headers_source)
    groups_source = response_source.json()
    #groups = groups_source.get("Resources", [])

    group_source_names = {group['displayName']: group['members']  for group in groups_source.get("Resources", [])}
    #trans group name to target
    for _group_name,member in group_source_names.items():
        trans_group_to_traget(account_id_target=account_id_target,token_target=token_target,group_name=_group_name)
        
    # 获取用户ID
    response_users= requests.get(users_url_target, headers=headers_target)
    users_target = response_users.json()
    users_names = {user.get('displayName'): user['id']  for user in users_target.get("Resources", [])}
    response_target= requests.get(groups_url_target, headers=headers_target)
    groups_target = response_target.json()
    group_target_ids = {group['displayName']: group['id']  for group in groups_target.get("Resources", [])}
    for g_name,member in group_source_names.items(): 
        for g_name_target,group_id in group_target_ids.items():
            if g_name == g_name_target  :
                for n in member:
                    user_display_name= n['display']
                    user_id=users_names.get(user_display_name)
                    print(user_display_name,user_id)
            #         add_members_args = {
            #     "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            #     "Operations": [{
            #         "op": "add",
            #         "value": {"members": [{'value':user_id}]},
            #         "type": "user"
            #         }
            #     ]
            # }
                    add_members_args = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [
            {
                "op": "add",
                "path": "members",
                "value": [
                    {
                        "display": user_display_name,
                        "value": "[{'value': '6180174849971126'}, {'value': '766363704992282'}]",
                       
                    }
                ]
            }
        ]
    }
                    patch_url = f"{groups_url_target}/{group_id}"
                    patch_response = requests.patch(patch_url, headers=headers_target, json=add_members_args)
                    if patch_response.status_code == 200:
                        #print(patch_response.json())
                        print(f"User {n['display']} added to group {g_name} successfully.")
                    else:
                        print(f"Failed to add user {n['display']} to group {g_name}:", patch_response.json())
# users = response.json()

# # 创建一个字典来存储用户ID
# user_ids = {user['displayName']: user['id'] for user in users.get("Resources", [])}

# # 遍历用户和组的信息
# for display_name, groups in users_to_add.items():
#     user_id = user_ids.get(display_name)
#     if user_id:
#         for group_id in groups:
#             # 构建添加用户的请求体
#             new_member = {
#                 "members": [{
#                     "display": display_name,
#                     "value": user_id,
#                     "$ref": f"Users/{user_id}",
#                     "userName": f"{display_name.lower().replace(' ', '.')}@example.com",
#                     "type": "user"
#                 }]
#             }

#             # 发送 PATCH 请求以添加用户到组
#             patch_url = f"{groups_url}/{group_id}"
#             patch_response = requests.patch(patch_url, headers=headers, data=json.dumps(new_member))
#             if patch_response.status_code == 200:
#                 print(f"User {display_name} added to group {group_id} successfully.")
#             else:
#                 print(f"Failed to add user {display_name} to group {group_id}:", patch_response.json())
#     else:
#         print(f"User {display_name} not found")
