[Repo URL](https://github.com/akilthms/mlflow-registry-workaround)

### Demo Steps


1. Train the model in a pandas udf
2. Write the model output to object store. Ensuring that any Python objects
   ()
3. Return a uri / location of the model output from the pandas udf
4. Collect all uris in the driver node as an index file.
5. Log the experiment run(s) post parallel training 
   1. As a single run? (if it is a single run I am assuming we log the index file with it?)
   2. Or multiple runs?

### From Ben Wilson 
2 & 3 -> Make sure you have a reference to the model 'name' or identifier to the uri but ensure that you're converting any Python objects (dicts, tuples, etc) to a string serialized representation since Arrow can't handle Objects.
4 -> you don't have to collect them. If there are millions of them, you could write from the executors as parquet :slightly_smiling_face:
5a. single run. Don't spam the create_run() API. And yep, log the index file as an artifact to the run. You can also log metrics / params for each model as a .csv if you're so inclined (that's what Diviner does)

### Separation of Duties
#### Peyman
* Create a function that writes a model output 
to object store (dbfs, adls, s3, etc.) and returns
the location of where the model is stored.
* Create a function that collects a list of file uris 
in a single file called index_file
* 
#### Akil
* Set up git repo
* Set up demo scenario from kaggle 
* Get training data
* Setup test of hitting api-limits
  * This will be useful to showcase how this solution avoids 
  the bottleneck of databricks infrastructure limits when training scales massively
### Open Questions
* Do we save the mlflow experiment as a single run or multiple runs?
* What Databricks demo environment should we use?