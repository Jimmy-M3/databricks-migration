
# source_token,source_domain,target_token,target_domain
import datetime
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from databricks import connect
def connectToDatabricks(host,token,cluster_id): 
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.remote(
        host=host,
        token=token,
        cluster_id=cluster_id
    ).getOrCreate()
    return  spark
"""
连接databricks
"""   
def get_privilege_list(_domain,_token,cloudtype): 
    if cloudtype =="aws":
        spark_sql = connectToDatabricks(host=_domain,token=_token,cluster_id="1011-023250-wuu5jk38" )
  
    #限制条件
    excluded_catalogs = {"test_migration","bdec"}
    excluded_schemas = {"default","information_schema"} 
    excluded_catalogs_str = ', '.join(f"'{catalog}'" for catalog in excluded_catalogs) 
    #执行catalog语句 
    catalog_sql_text=f"""select * from 
    system.information_schema.catalog_privileges where catalog_name in({excluded_catalogs_str});""" 
                                
    catalog=spark_sql.sql(catalog_sql_text).collect()
    #执行schema语句 
    schema_sql_text=f"""select * from 
    system.information_schema.schema_privileges where catalog_name in({excluded_catalogs_str})  ;"""                                 
    schema=spark_sql.sql(schema_sql_text).collect()
    #执行table语句
    table_sql_text=f"""select * from 
    system.information_schema.table_privileges   where table_catalog in({excluded_catalogs_str}) ;"""                                 
    table=spark_sql.sql(table_sql_text).collect()
    catalog_sql=[]
    schema_sql=[]
    table_sql=[]
    all_sql=[]
    #生成SQL语句
    for i in catalog:
        privilege_type = i["privilege_type"]
        catalog_name = i["catalog_name"]
        grantee = i["grantee"].replace('@anker.com','@anker-in.com')
        if not all([privilege_type, catalog_name, grantee]):
            raise ValueError("Missing or empty values in the catalog.")
        catalog_sql.append(f"GRANT {privilege_type} ON CATALOG {catalog_name} TO `{grantee}` ;")
    schema=[i for i in schema if i["schema_name"] not in excluded_schemas]    
    for i in schema:
        privilege_type = i["privilege_type"]
        catalog_name = i["catalog_name"]
        schema_name = i["schema_name"]
        grantee = i["grantee"].replace('@anker.com','@anker-in.com')
        if not all([privilege_type, catalog_name, schema_name,grantee]):
            raise ValueError("Missing or empty values in the schema.")
        # 构建SQL语句
        schema_sql.append(f"GRANT {privilege_type} ON SCHEMA {catalog_name}.{schema_name} TO `{grantee}` ;")    
    table=[i for i in table if i["table_schema"] not in excluded_schemas]         
    for i in table:
        privilege_type = i["privilege_type"]
        table_catalog = i["table_catalog"]
        table_schema = i["table_schema"]
        table_name = i["table_name"]
        grantee = i["grantee"].replace('@anker.com','@anker-in.com')
        if not all([privilege_type, table_catalog, table_schema,table_name,grantee]):
            raise ValueError("Missing or empty values in the table.")
        # 构建SQL语句    
        table_sql.append(f"GRANT {privilege_type} ON table {table_catalog}.{table_schema}.{table_name} TO `{grantee}` ;")
    #授权
    all_sql.extend(catalog_sql)
    all_sql.extend(schema_sql)
    all_sql.extend(table_sql)
    # if cloudtype == "aws":
    #     spark_aws = SparkSession.builder.appName("Optimized Queries").getOrCreate()
    #     results = [spark_aws.sql(query) for query in all_sql]
    #     # 使用线程池执行器并行执行查询
    #     with ThreadPoolExecutor(max_workers=50) as executor:
    #         futures = [executor.submit(spark.sql, query) for query in all_sql]
    #         # 获取查询结果
    #         for future in futures:
    #             future.result()
    return all_sql
source_token="dapif68be4994c246f0cb09c54e1faa337dc"
print(datetime.datetime.now())
source_domain="https://dbc-1c15bce2-47da.cloud.databricks.com"
target_token="dapic6d4c1d9850a23087423bafdb3b3f4db"
target_domain="https://adb-4145924773612620.0.azuredatabricks.net"
spark_azue =  connectToDatabricks(host=target_domain,token=target_token,cluster_id='1023-084744-v79tf9hl')
all_sql=get_privilege_list(source_domain,source_token,"aws")
print(all_sql)
#a=spark_azue.sql("select 1;")
#a.show()
# print(all_sql)
 
results = [spark_azue.sql(query) for query in all_sql]
print(datetime.datetime.now())
# 创建 DatabricksSession
 
# all_sql=get_privilege_list(source_domain,source_token,"aws")
# spark_azure=  connectToDatabricks(host=_domain,token=_token,cluster_id="1011-023250-wuu5jk38" )
# spark_azure = SparkSession.builder.appName("Optimized Queries").getOrCreate()
# results = [spark_azure.sql(query) for query in all_sql]



        
 

