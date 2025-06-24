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
file_path = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/folder/data.csv"
df = spark.read.option("header", "true").csv(file_path)
df.show()



























