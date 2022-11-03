# Databricks notebook source
# MAGIC %pip install prophet azure-storage-file-datalake

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

# COMMAND ----------

sql_statement = '''
  SELECT
    store,
    item,
    CAST(date as date) as ds,
    SUM(sales) as y
  FROM akthom.mlflow_registry_workaround.sales -- REPLACE WITH YOUR OWN TABLE
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

from pyspark.sql.types import *
 
result_schema =StructType([
  StructField('path',StringType()),
  StructField('store_id',StringType()),
  StructField('sku',StringType()),
  StructField('model_params', StringType() ), 
  StructField('inference_params', StringType() )
  ])

# COMMAND ----------

# DBTITLE 1,Connect to your storage container
def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

def initialize_storage_account_ad(storage_account_name, client_id, client_secret, tenant_id):
    
    try:  
        global service_client

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=credential)
    
    except Exception as e:
        print(e)
        
def upload_file_to_directory(path, artifact):
    try:

        file_system_client = service_client.get_file_system_client(file_system="my-file-system")

        directory_client = file_system_client.get_directory_client("my-directory")
        
        file_client = directory_client.create_file("uploaded-file.txt")
        local_file = open("C:\\file-to-upload.txt",'r')

        file_contents = local_file.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

    except Exception as e:
      print(e)

# COMMAND ----------

from prophet.serialize import model_to_json, model_from_json
import joblibs
# ❌ Dont use pickle
from azure.storage.filedatalake import DataLakeServiceClient

USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
DESTINATION = f"dbfs:/home/{USER}/customers/abi/mlflow-registry-workaround"

def write_model(store_id, sku, dest:str="", model=None, **kwargs) -> None:
  pass
  filename = f"storeId-{store_id}-sku-{sku}.json"
  artifact = {"model_params": kwargs["model_params"], 
              "inference_params": kwargs["inference_params"],
              "model": model_to_json(model)}
  path = f'{dest}/{filename}'
  upload_file_to_directory(path, artifact)
  return path

# COMMAND ----------

import pandas as pd
from prophet import Prophet
def forecast_store_item( history_pd: pd.DataFrame ) -> pd.DataFrame:
  # TRAIN MODEL AS BEFORE
  # --------------------------------------
  # remove missing values (more likely at day-store-item level)
  history_pd = history_pd.dropna()
  store_id = history_pd['store'].iloc[0]
  sku = history_pd['item'].iloc[0]
  
  mparams = {"interval_width": 0.95, 
                  "growth" : 'linear', 
                  "daily_seasonality" : False, 
                  "weekly_seasonality" : True,
                  "yearly_seasonality" : True, 
                  "seasonality_mode" : 'multiplicative'}
  
  iparams = {"periods":90, 
             "freq":'d', 
             "include_history":True}
  # configure the model
  # consider using a statsmodel (PossionModel 🤷🏾‍♂️)
  model = Prophet(**mparams)
  
  # train the model
  trained_model = model.fit( history_pd )
  # --------------------------------------
  # STORE MODEL IN ARBITRATY OBJECT STORE LOCATION AND RECORD URI
  result_cols = ["path","store_id", "model_params", "inference_params", "model"]
  path = write_model(store_id, 
                     sku, 
                     dest=DESTINATION, 
                     model = trained_model,
                     model_params=mparams, 
                     inference_params=iparams )
  mparams = str(mparams)
  iparams = str(iparams)
  results = { "path":[path],
              "store_id": [store_id],
              "sku": [sku],
              "model_params": [mparams], 
              "inference_params": [iparams]
            }
 
  results_pd = pd.DataFrame(data=results)
  return results_pd

# COMMAND ----------

from pyspark.sql.functions import current_date
 
results = (
  store_item_history
    .groupBy('store', 'item')
      .applyInPandas(forecast_store_item, schema=result_schema)
    .withColumn('training_date', current_date() )
    )
 
display(results)


# COMMAND ----------

# Create index File
results.toPandas().to_json(orient = 'records').replace('\\', '')
with open(str(indexFileName), "w") as outfile:
    outfile.write(index_df_new.toPandas().to_json(orient = 'records').replace('\\', ''))

# COMMAND ----------

import mlflow

index_file_path = indexFileName = f"/dbfs/home/{user}/ABI/index.json"

with mlflow.start_run():
  mlflow.log_artifact("/dbfs/home/akil@databricks.com/ABI/index.json)

# COMMAND ----------

from prophet.serialize import model_from_json

def run_inference(store_id: str, sku: str, uri: str) -> FileObject:
  uri = find_uri_by_keys(store_id, sku)
  model = mlflow.download_artifact(uri)
  prophet_model = model_from_json(model)
  predictions = prophet_model.predict(new_data_df)
  return predictions
