# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC create flag parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
# print(type(incremental_flag))

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating dimension model

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fetch relative column

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carsalessa.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(Dealer_ID) as Dealer_ID, DealerName from parquet.`abfss://silver@carsalessa.dfs.core.windows.net/carsales`
# MAGIC

# COMMAND ----------

df_src = spark.sql('''
                   select distinct(Dealer_ID) as Dealer_ID, DealerName  from parquet.`abfss://silver@carsalessa.dfs.core.windows.net/carsales`
                   ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # create surrogate key 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 1 as dim_dealer_key, Dealer_ID, DealerName 
# MAGIC from parquet.`abfss://silver@carsalessa.dfs.core.windows.net/carsales`
# MAGIC where 1=0
# MAGIC -- here 1=0 gives only schema (only column names no data)
# MAGIC -- and 1 as dim_model_key to prevent errro saying column does not exists, it creates it

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_model sink -  initial and incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog_gold.dim_dealer'):
    # incremental run
    df_sink = spark.sql('''
    select dim_dealer_key, Dealer_ID, DealerName 
    from cars_catalog.gold.dim_dealer
    ''')
else:
    # initial run
    df_sink = spark.sql('''
    select 1 as dim_dealer_key, Dealer_ID, DealerName 
    from parquet.`abfss://silver@carsalessa.dfs.core.windows.net/carsales`
    where 1=0 ''')


# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### applyiing join betwween source and destination because we are filtering old and new data for insertion and updation

# COMMAND ----------

df_filer = df_src.join(df_sink,df_src['Dealer_ID'] == df_sink['Dealer_ID'],'left').select(df_src['Dealer_ID'],df_src['DealerName'],df_sink['dim_dealer_key'])

# COMMAND ----------

df_filer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **df_filer_old**
# MAGIC

# COMMAND ----------

df_filter_old = df_filer.filter(col('dim_dealer_key').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new = df_filer.filter(col('dim_dealer_key').isNull()).select('Dealer_ID','DealerName')

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch the max surrogate key from existing table**

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    # max_value = df_filter_old.agg(max(col('dim_model_key')))
    max_value_df = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC **Create surrogate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key',max_value+monotonically_increasing_id())
df_filter_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Combining old and new data
# MAGIC Create Final DF - df_filer_old + [df_filter_new](url)**

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)


# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **SCD Type 1 (upsert)** - update + insert

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    # apply upsert - incremental run
    delta_tbl = DeltaTable.forPath(spark,'abfss://gold@carsalessa.dfs.core.windows.net/dim_dealer')
    delta_tbl.alias('tgt').merge(
        df_final.alias('src'),
        'tgt.dim_dealer_key = src.dim_dealer_key'
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # if table not exists - initial run
    df_final.write.format('delta').mode('overwrite').option('path','abfss://gold@carsalessa.dfs.core.windows.net/dim_dealer').saveAsTable('cars_catalog.gold.dim_dealer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer