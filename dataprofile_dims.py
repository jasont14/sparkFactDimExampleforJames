
# COMMAND ----------

import time
from datetime import datetime
from dateutil import tz
from pyspark.sql import functions as F
from pyspark.sql.functions import max, min, avg, count, mean, mode, lit, countDistinct, median, when, isnull, DataFrame, concat_ws, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pandas as pd

# COMMAND ----------


def GenerateTableDDL(table:str,write_out_location:str,field_sql:str) -> str:
    sql = """     
    CREATE TABLE IF NOT EXISTS {}(    
       {}
    )
    USING DELTA
    LOCATION '{}'
    """.format(table,field_sql,write_out_location)
    return sql

def CreateDimTable(env:str,catalog:str,database:str,table:str,location:str,sql) -> str:
    ddl_sql = GenerateTableDDL(table,location,sql)
    spark.sql("USE CATALOG {}".format(catalog))
    spark.sql("USE DATABASE {}".format(database))    
    spark.sql(ddl_sql)

# COMMAND ----------

#create lists of dim info. Could be held in file / table. 

#Nice To Have: Move concat out to function. Create a class/object. 

columns = ['env', 'catalog', 'database', 'table', 'write_out_location', 'sql', 'read_in_location', 'spark_sql_select', 'dim_table_name']

dim_catalog_prod = ['prod'\
               ,'utility'\
               ,'data_profile'\
               , 'dim_catalog'\
               , abfss_dim_cat
               ,"""dim_catalog_key STRING NOT NULL, catalog_name    STRING NOT NULL,catalog_comment STRING
               ,catalog_owner   STRING
               ,created         TIMESTAMP
               ,created_by      STRING
               ,last_altered    TIMESTAMP
               ,last_altered_by STRING """\
               ,'fivetran.information_schema.catalogs'\
               ,[col('catalog_name').cast('string').alias('dim_catalog_key'),'catalog_name',col('comment').alias('catalog_comment'),'catalog_owner','created','created_by','last_altered','last_altered_by']\
               ,'dim_catalog']

dim_columns_prod = ['prod','utility','data_profile', 'dim_column', abfss_dim_col
,"""dim_column_key       STRING NOT NULL
,table_catalog   STRING NOT NULL
,table_schema   STRING NOT NULL
,column_name  STRING NOT NULL
,ordinal_position INT
,data_type STRING
,character_maximum_length INT
,datetime_precision INT
,numeric_precision INT
,numeric_precision_radix INT
,numeric_scale INT
,partition_index INT"""\
,'fivetran.information_schema.columns'\
,[(concat_ws('|',col('table_catalog'),col('table_schema'),col('table_name'),col('column_name'))).cast('string').alias('dim_column_key'),col('table_catalog'),col('table_schema'),col('column_name'),col('ordinal_position').cast('integer'),col('data_type'),col('character_maximum_length').cast('integer'),col('datetime_precision').cast('integer'),col('numeric_precision').cast('integer'),col('numeric_precision_radix').cast('integer'),col('numeric_scale').cast('integer'),col('partition_index').cast('integer')]\
,'dim_column']

dim_table_prod = ['prod','utility','data_profile', 'dim_table',\
             abfss_dim_table
             ,"""dim_table_key  STRING NOT NULL
,table_name STRING NOT NULL
,table_catalog  STRING NOT NULL
,table_schema STRING NOT NULL
,table_type STRING
,table_owner STRING
,table_comment STRING
,data_source_format STRING
,storage_sub_directory STRING"""
,'fivetran.information_schema.tables'\
,[(concat_ws('|',col('table_catalog'),col('table_schema'),col('table_name'))).cast('string').alias('dim_table_key'),col('table_name'),col('table_catalog'),col('table_schema'),col('table_type'),col('table_owner'),col('comment').alias("table_comment"),col('data_source_format'),col('storage_sub_directory')]\
 ,'dim_table'] 

dim_database_prod = ['prod','utility','data_profile', 'dim_database', abfss_dim_database
,"""dim_database_key STRING NOT NULL
,catalog_name STRING NOT NULL
,database_name_aka_schema STRING NOT NULL
,database_owner STRING
,database_comment STRING
,created TIMESTAMP
,created_by STRING
,last_altered TIMESTAMP
,last_altered_by STRING"""\
,'fivetran.information_schema.schemata'\
,[(concat_ws('|',col('catalog_name'),col('schema_name'))).cast('string').alias('dim_database_key'),col('catalog_name'),col('schema_name').alias("database_name_aka_schema"),col('schema_owner').alias("database_owner"),col('comment').alias("database_comment"),col('created'),col('created_by'),col('last_altered'),col('last_altered_by')]\
,'dim_database']



# COMMAND ----------



dim_catalog = ['dev'\
               ,'sb_jthatcher'\
               ,'thatcher_dev_silver'\
               , 'dim_catalog'\
               , abfss_dim_catalog
               ,"""dim_catalog_key STRING NOT NULL, catalog_name    STRING NOT NULL,catalog_comment STRING
               ,catalog_owner   STRING
               ,created         TIMESTAMP
               ,created_by      STRING
               ,last_altered    TIMESTAMP
               ,last_altered_by STRING """\
               ,'fivetran.information_schema.catalogs'\
               ,[col('catalog_name').cast('string').alias('dim_catalog_key'),'catalog_name',col('comment').alias('catalog_comment'),'catalog_owner','created','created_by','last_altered','last_altered_by']\
               ,'dim_catalog']

dim_columns = ['dev','sb_jthatcher','thatcher_dev_silver', 'dim_column', abfss_dim_column
,"""dim_column_key       STRING NOT NULL
,table_catalog   STRING NOT NULL
,table_schema   STRING NOT NULL
,column_name  STRING NOT NULL
,ordinal_position INT
,data_type STRING
,character_maximum_length INT
,datetime_precision INT
,numeric_precision INT
,numeric_precision_radix INT
,numeric_scale INT
,partition_index INT"""\
,'fivetran.information_schema.columns'\
,[(concat_ws('|',col('table_catalog'),col('table_schema'),col('table_name'),col('column_name'))).cast('string').alias('dim_column_key'),col('table_catalog'),col('table_schema'),col('column_name'),col('ordinal_position').cast('integer'),col('data_type'),col('character_maximum_length').cast('integer'),col('datetime_precision').cast('integer'),col('numeric_precision').cast('integer'),col('numeric_precision_radix').cast('integer'),col('numeric_scale').cast('integer'),col('partition_index').cast('integer')]\
,'dim_column']

dim_table = ['dev','sb_jthatcher','thatcher_dev_silver', 'dim_table',\
             'abfss_dim_table'\
             ,"""dim_table_key  STRING NOT NULL
,table_name STRING NOT NULL
,table_catalog  STRING NOT NULL
,table_schema STRING NOT NULL
,table_type STRING
,table_owner STRING
,table_comment STRING
,data_source_format STRING
,storage_sub_directory STRING"""
,'fivetran.information_schema.tables'\
,[(concat_ws('|',col('table_catalog'),col('table_schema'),col('table_name'))).cast('string').alias('dim_table_key'),col('table_name'),col('table_catalog'),col('table_schema'),col('table_type'),col('table_owner'),col('comment').alias("table_comment"),col('data_source_format'),col('storage_sub_directory')]\
 ,'dim_table'] 

dim_database = ['dev','sb_jthatcher','thatcher_dev_silver', 'dim_database', abfss_dim_database
,"""dim_database_key STRING NOT NULL
,catalog_name STRING NOT NULL
,database_name_aka_schema STRING NOT NULL
,database_owner STRING
,database_comment STRING
,created TIMESTAMP
,created_by STRING
,last_altered TIMESTAMP
,last_altered_by STRING"""\
,'fivetran.information_schema.schemata'\
,[(concat_ws('|',col('catalog_name'),col('schema_name'))).cast('string').alias('dim_database_key'),col('catalog_name'),col('schema_name').alias("database_name_aka_schema"),col('schema_owner').alias("database_owner"),col('comment').alias("database_comment"),col('created'),col('created_by'),col('last_altered'),col('last_altered_by')]\
,'dim_database']



dim_list = [dim_catalog_prod,dim_database_prod,dim_table_prod,dim_columns_prod]

#create dataframe to hold lists 

df_dims = pd.DataFrame(dim_list,columns=columns)    

# COMMAND ----------

df_dims.head()

# COMMAND ----------

#CREATE TABLES
for i, row in df_dims.iterrows():
    CreateDimTable('dev',row['catalog'],row['database'],row['table'],row['write_out_location'],row['sql'])

# COMMAND ----------

#WRITE DATA TO TABLES
for i, row in df_dims.iterrows():
    df = spark.read.format('delta').table(row['read_in_location'])
    df2 = df.select(row['spark_sql_select'])
    df2.write.format('delta').mode('overwrite').saveAsTable(row['dim_table_name'])
