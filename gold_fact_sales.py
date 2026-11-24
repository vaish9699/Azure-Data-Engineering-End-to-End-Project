# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading silver data

# COMMAND ----------

df_silver =  spark.sql("select * from parquet.`abfss://silver@carsalessa.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading all the dims 

# COMMAND ----------

df_dealer = spark.sql("select * from cars_catalog.gold.dim_dealer")

df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")

df_date = spark.sql("select * from cars_catalog.gold.dim_date")

df_model = spark.sql("select * from cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bringing keys to the Fact table

# COMMAND ----------

df_fact = df_silver.join(df_branch,df_silver.Branch_ID == df_branch.Branch_ID, 'left')\
    .join(df_dealer,df_silver.Dealer_ID == df_dealer.Dealer_ID, 'left')\
        .join(df_date,df_silver.Date_ID == df_date.Date_ID, 'left')\
        .join(df_model,df_silver.Model_ID == df_model.Model_ID, 'left')\
            .select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver['revenue_per_unit'],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'],df_date['dim_date_key'],df_model['dim_model_key'])


# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Only on rare scenarios we apply merge on fact table otherwise we usually insert

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('factsales'):
    deltatbl = DeltaTable.forName(spark,'cars_catalog.gold.factsales')
    deltatbl.alias('t').merge(df_fact.alias('s'),'t.dim_date_key = s.dim_date_key and t.dim_branch_key = s.dim_branch_key and t.dim_dealer_key = s.dim_dealer_key and t.dim_model_key = s.dim_model_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
                .execute()

else:
    df_fact.write.format('delta')\
        .mode('overwrite')\
            .option('path','abfss://gold@carsalessa.dfs.core.windows.net/factsales')\
                .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales