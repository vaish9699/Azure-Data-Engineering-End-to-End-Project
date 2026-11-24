# Databricks notebook source
df = spark.read.format('parquet')\
    .option('inferSchema',True)\
        .load('abfss://bronze@carsalessa.dfs.core.windows.net/rawdata')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # transformations

# COMMAND ----------

from pyspark.sql.functions import split,col,sum
from pyspark.sql.types import StringType
from pyspark.sql import functions as F



# COMMAND ----------

df = df.withColumn('model_category',split(col('Model_ID'),'-')[0])
df.show(5)

# COMMAND ----------

df = df.withColumn('Units_Sold', col('Units_Sold').cast(StringType()))

# COMMAND ----------

df = df.withColumn('revenue_per_unit',col("Revenue")/col("Units_Sold"))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

performance_per_branch_df = df.filter(col('Year') == 2017).groupBy('Year','BranchName').agg(sum(col('Units_Sold')).alias('total_units_sold')).orderBy("BranchName","total_units_sold",ascending=[1,1]).select('total_units_sold','Year','BranchName')

# COMMAND ----------

performance_per_branch_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data writing 

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
        .option('path','abfss://silver@carsalessa.dfs.core.windows.net/carsales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carsalessa.dfs.core.windows.net/carsales`

# COMMAND ----------

