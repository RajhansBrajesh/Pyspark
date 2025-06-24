# Databricks Notebook for future references
# To read parameters from other Notebook/ADF use dbutils.widget.text() then read the data using dbutils. 
# Create widgets (this will be used when the notebook is triggered from ADF or another notebook)
dbutils.widgets.text("input_path", "")
dbutils.widgets.text("run_date", "")

# Read widget values
input_path = dbutils.widgets.get("input_path")
run_date = dbutils.widgets.get("run_date")

# To create a Dataframe
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)
df.show()


# To run any SQL from python shell use saprk.sql
df.createOrReplaceTempView("vw_people")
spark.sql("select * from vw_people")

# Perform Join Using SQL
result = spark.sql("""
SELECT e.emp_id, e.name, d.dept_name
FROM employees e
JOIN departments d
ON e.dept_id = d.dept_id
""")
result.show()

#You can read directly from Azure Data Lake Storage (ADLS) Gen2 without mounting it by using spark.conf.set(...) to configure the credentials and storage access in Databricks.
# This is how it is done in ph
authenticationkey = dbutils.secrets.get(scope="<scope-name>", key="<key-name>")
<storage-account-name> = 'az21q1datalake'
spark.conf.set("fs.azure.account.auth.type.<storage-account-name>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account-name>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account-name>.dfs.core.windows.net", "<client-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account-name>.dfs.core.windows.net", authenticationkey )
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")

#Now, you can read files directly using abfss:// path:
<container-name> = bronze
<storage-account-name> = 'az21q1datalake'
file_path = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/folder/*.csv"
df = spark.read.option("header", "true").csv(file_path)
df.show()

# To read parquet file
file_path = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/folder/*.parquet"
df = spark.read.format("parquet").load(file_path)
df.show()

#To enable schema auto-merge in Databricks (especially for Delta Lake tables), you can use. Below bot conditions are needed for merge schema to work:
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

df.write.format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .save("/mnt/delta/events")

# The mergeSchema = True condition is not needed in case of merge query although spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") needed:
# Enable schema auto merge globally
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# No .option("mergeSchema", "true") needed here
spark.sql("""
MERGE INTO delta.`/mnt/delta/merge_target` AS target
USING source_data AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")





























