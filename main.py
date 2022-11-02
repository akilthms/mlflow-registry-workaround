# Databricks notebook source
# MAGIC %pip install prophet

# COMMAND ----------

dbutils.fs.ls("/tmp/solacc/demand_forecast/train/train.csv")



# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS akthom.mlflow_registry_workaround;
# MAGIC CREATE TABLE IF NOT EXISTS akthom.mlflow_registry_workaround.sales; 
# MAGIC 
# MAGIC COPY INTO akthom.mlflow_registry_workaround.sales
# MAGIC   FROM 'dbfs:/tmp/solacc/demand_forecast/train/train.csv'
# MAGIC   FILEFORMAT = CSV
# MAGIC   FORMAT_OPTIONS ('mergeSchema' = 'true',
# MAGIC                   'header' = 'true')
# MAGIC   COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC 
# MAGIC GRANT SELECT ON TABLE akthom.mlflow_registry_workaround.sales TO `peyman@databricks.com`;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM akthom.mlflow_registry_workaround.sales;

# COMMAND ----------

sql_statement = '''
  SELECT
    store,
    item,
    CAST(date as date) as ds,
    SUM(sales) as y
  FROM akthom.mlflow_registry_workaround.sales
  GROUP BY store, item, ds
  ORDER BY store, item, ds
  '''
 
store_item_history = (
  spark
    .sql( sql_statement )
    .repartition(sc.defaultParallelism, ['store', 'item'])
  ).cache()

store_item_history.display()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import *
 
result_schema =StructType([
  StructField('ds',DateType()),
  StructField('store',IntegerType()),
  StructField('item',IntegerType()),
  StructField('y',FloatType()),
  StructField('yhat',FloatType()),
  StructField('yhat_upper',FloatType()),
  StructField('yhat_lower',FloatType())
  ])

# COMMAND ----------

def forecast_store_item( history_pd: pd.DataFrame ) -> pd.DataFrame:
  
  # TRAIN MODEL AS BEFORE
  # --------------------------------------
  # remove missing values (more likely at day-store-item level)
  history_pd = history_pd.dropna()
  
  # configure the model
  # consider using a statsmodel (PossionModel 🤷🏾‍♂️)
  model = Prophet(
    interval_width=0.95,
    growth='linear',
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=True,
    seasonality_mode='multiplicative'
    )
  
  # train the model
  model.fit( history_pd )
  # --------------------------------------
  # STORE MODEL IN ARBITRATY OBJECT STORE LOCATION AND RECORD URI
  
  return results_pd

# COMMAND ----------

from pyspark.sql.functions import current_date
 
results = (
  store_item_history
    .groupBy('store', 'item')
      .applyInPandas(forecast_store_item, schema=result_schema)
    .withColumn('training_date', current_date() )
    )
 
results.createOrReplaceTempView('new_forecasts')
 
display(results)

