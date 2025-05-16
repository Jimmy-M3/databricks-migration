# #################################################################创建Group - account level

import requests
import json


# 配置
DATABRICKS_INSTANCE = 'https://accounts.databricks.azure.cn'
ACCOUNT_ID = '20c26866-115e-4e40-a202-a0f035943ad5'
WORKSPACE_ID = '1553473792208990'
PRINCIPAL_ID = '83711749289633'
# 83711749289633
TOKEN = 'dsapiceddfe587e6b281e10d9288a23af9ac3'

# PUT /api/2.0/accounts/{account_id}/workspaces/{workspace_id}/permissionassignments/principals/{principal_id}

# https://adb-1553473792208990.2.databricks.azure.cn/settings/workspace/identity-and-access/groups?o=1553473792208990

# API 端点
API_ENDPOINT = f'{DATABRICKS_INSTANCE}/api/2.1/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}/permissionassignments/principals/{PRINCIPAL_ID}'
# https://accounts.databricks.azure.cn/api/2.1/accounts/20c26866-115e-4e40-a202-a0f035943ad5/scim/v2
# Headers
headers = {
'Authorization': f'Bearer {TOKEN}',
'Content-Type': 'application/json'
}

# 权限数据
permission_data = {
"permissions": [
"USER"
]
}

# 发起 POST 请求
response = requests.put(API_ENDPOINT, headers=headers, data=json.dumps(permission_data))
print(response.content())

# 检查响应
if response.status_code == 200:
    print('Permissions successfully assigned.')
else:
    print(f'Error: {response.status_code}')
    print(response.json())

