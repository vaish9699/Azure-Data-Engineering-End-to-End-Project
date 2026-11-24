# Databricks notebook source
# MAGIC %md
# MAGIC Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user(), current_metastore();
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create catalog
# MAGIC %sql
# MAGIC create catalog cars_catalog managed location 'abfss://silver@carsalessa.dfs.core.windows.net/';

# COMMAND ----------

# MAGIC %md
# MAGIC create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.bronze;

# COMMAND ----------

