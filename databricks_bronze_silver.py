df_raw = spark.read.option("header", "true").csv(file_path)
df = df_raw.withColumn("ADLS_LOADED_DATE", F.lit(triggerdate))

#To replace special chartecter in column names
df = df.select([F.col(col).alias(col.replace('-','')) for col in df.columns])

# To check if a table is being loaded first time
def is_delta_table_available(silver_table_df):
  return len(silver_table_df.schema.fields)>0
  
# generally we use df_raw to write in Bronze and Silver both as written below
df.write.option('mergeSchema', 'true').saveAsTable(BronzeTableName, mode = 'append')
stagingTable = "stg_" +objectName
df.createOrReplaceTempView(stagingTable)
Source = stagingTable
Target = prod_l1.wpc.tableName
if extratiobType.lower() == 'full':
  df.write.option('mergeSchema', 'true').saveAsTable(SilverTableName, mode = 'overwrite')
else:
   # Below code will automatically merge the schema as spark config is True
  spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
  if is_delta_table_available(Target):
     spark.sql("""
            MERGE INTO {Target} AS target
            USING {Source} AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)

    # There is another way of doing the same:
      spark.sql("""
              MERGE INTO {Target} AS target
              USING {Source} AS source
              ON target.id = source.id
              WHEN MATCHED THEN DELETE
              """)
     df.write.option('mergeSchema', 'true').saveAsTable(SilverTableName, mode = 'append')
else:
    df.write.option('mergeSchema', 'true').saveAsTable(Target, mode = 'overwrite')
############################## Delta from bronze to silver ###########################################
# There is another way to load Silver from bronze table i.e. use the data loaded in bronze
df.write.option('mergeSchema', 'true').saveAsTable(BronzeTableName, mode = 'append')
last_loaded_date = select max(watermarkCol) from silverTable
df = spark.sql(f"select * from BronzeTable where watermarkCol >= last_loaded_date")
# It can have duplicate value so do the deduplication
if orderByCol == '':
  orderByCol = pk
rowNumber = Window.partitionBy(PK).orderBY(desc(orderByCol))
df = df.withColumn('rk', dense_rank().over(rownumber))
df = df[df['rk']==1]
df = df.drop('rk')
# Then use below merge logic to load silver table
  spark.sql("""
            MERGE INTO {Target} AS target
            USING {Source} AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)

####################CDC from Bronze to silver used by L2 ############################
# make CDC enabled on bronze one time activity
ALTER TABLE bronze_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# Utilize the table_changes function to retrieve incremental changes from the bronze table
SELECT * FROM table_changes('bronze_table', starting_version);
# Replace starting_version with the appropriate version number from which you want to capture changes.
# For each run, update a metadata table or checkpoint with the last processed version. below is metadata update for max_version of Bronze
CREATE TABLE bronze_table_version_metadata (
  table_name STRING PRIMARY KEY,
  last_processed_version BIGINT,
  updated_at TIMESTAMP
);

# now actual code from WCMS by Nagapriya
# Check if it is Full load. Check the brnze_table version logged in bronze_table_version_metadata control table
if full_load_flag:
  last_processed_version = None
  initial load = True
else:
  try:
    last_processed_version = spark.read.table(bronze_table_version_metadata).filter(col("table_name") == bronze_table_name).collect()[0]['last_processed_version']
    initial_load = False
  except:
    last_processed_version = None
    initial_load = True

#Get current version from unity table
broze_table = DeltaTable.forName(spark,bronze_table_name)
current_verion = bronze_table.history(1).select("vesrion").collect()[0][0]

#Load from bronze
if initial_load:
  bronze_data = spark.read.table(bronze_table_name)
else:
  if last_processed_version<current_version:
    bronze_data = spark.read.option("readChangeData", "true").option("startingVersion", last_processed_version +1)\
                  .table(bronze_table_name).filter("_change_type IN ('insert', 'update_postimage', 'delete')")
  else:
    bronze_data = None
if bronze_data:
  silver_table = DeltaTable.forName(spark,silver_table_name)
  merge_condition = "target.id = source.id"

  if initial_load:
    bronze_data.drop("_change_type", "_commit_timestamp", "_commit_version", "_metadata")..write.option('mergeSchema', 'true').saveAsTable(silver_table_name, mode = 'overwrite')
    silver_table = DeltaTable.forName(spark,silver_table_name)
  else:
    window_spec = Window.partitionNy(primary_key_col_list).orderBy(col("_commit_version").desc())
    deduplicate_bronze_data = bronze_data.withColumn("rank", row_number().over(window_spec)\
                                   .filter("rank ==1")\
                                   .drop("rank")
   silver_table.alias("target").merge(deduplicate_bronze_data.alias("source"), merge_condition)
                               .whenMatchedDelete( condition = "source._change_type = 'delete'")\
                               .whenMatchedUpdate(set={col:f"source.{col} for col in common_columns})
                               .whenNotMatchedInsert(condition = "source._change_type in ('insert', 'update_postimage')", values= {col:f"source.{col} for col in common_columns})\
                               .execute()


spark.sql(f"delete from {bronze_table_version_metadata} where table_name = '{bronze_table_name}'")
spark.sql(f"insert inot {bronze_table_version_metadata} values ('{bronze_table_name}', {current_version})")

############################ SAP CDC implementation in use ########################

def replace_none_from_primarykey_col(df, col_list =[]):
  """ Remove null with NA values in pk col"""
  df = df.na.fill('NA', col_list)
  return df

def is_delta_table_available(target_delta_table) -> bool:
  return len(target_delta_table.toDF().schema.fields)>0

def get_merge_condition(pk_col_list):
  merge_condition = ''
  if len(pk_col_list)>0:
    left_col = []
    right_col = []
    for pk in pk_col_list:
      left_col.append(f"s1.{pk}")
      right_col.append(f"t1.{pk}")

merge_condition = [i+ ' = ' + j for i,j in zip(left_col,right_col)]
join_columns = ''
for cols in merge_condition:
  join_columns = join_columns _cols + ' And '

merge_condition = join_columns[:-4]
return merge_condition

# Function to load from bronze to silver
def updert_to_delta(batch_df, batch_id):
  ''' Here try and except with raise Exception and terminates the notebook'''
  
  try:
    tgt_delta_table = DeltaTable.forName(spark, silver_table_name)
    window_spec = Window.partitionNy(primary_key_col_list).orderBy(col("_processed_time").desc())
    batch_df = batch_df.withColumn("latest_record", row_number().over(window_spec)\
                                   .filter("latest_record ==1")\
                                   .drop("latest_record")

   if extraction_type == 'full':
      batch_df.write.option('mergeSchema', 'true').saveAsTable(SilverTableName, mode = 'overwrite')
  else:
    if is_delta_table_available(tgt_delta_table):
      merge_condition = get_merge_condition()
      tgt_deltatable.alias("t1").merge(batch_df.alias("s1"), merge_condition) \
                                .whenMatchedDelete("s1._operation_type = 'D'") \
                                .whenMatchUpdateAll("s1._operation_type != 'D'") \
                                .whenNotMatchInsertAll("s1._operation_type != 'D'") 
                                .execute()
  else:
   batch_df.write.option('mergeSchema', 'true').saveAsTable(SilverTableName, mode = 'overwrite')

  except Exception as error:
    raise Exception(f'Error occurred while merging, error msg: {error}')


## Read bronze Table
read_bronze_df = spark.readstream.option("ignoreDeletes", "true").table(bronze_table_name)

if len(primary_key_col_list)>0:
  read_bronze_df = replace_none_from_primarykey_col(read_bronze_df, priamary_key_col_list)

##Write to silver table

read_bronze_df.writeStream\
              .format("delta")\
              .outputMode("update")\
              .option("mergeSchema", "true")\
               .option("chekpointlocation", checkpoint_location)\
               .option("failOnDataLoss" False)\
               .trigger(availableNow = true)
               .foreachBatch(upsert_to_delta)\
               .start()



#################### To read data from Eventhub like hybris and write to bronze and silver ########################

kafka_opts = {
    "kafka.bootstrap.servers": EH_BOOTSTRAP,
    "subscribe": EH_NAME,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": EH_JAAS,
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}



def pushstreamingdata(batch,id):
  try:
     batch.write.mode('append').format('delta').Save(bronzepath)
    if is_delta_table_available(tgt_delta_table):
      merge_condition = get_merge_condition()
      tgt_deltatable.alias("t1").merge(batch_df.alias("s1"), merge_condition) \
                                .whenMatchedDelete("s1._operation_type = 'D'") \
                                .whenMatchUpdateAll("s1._operation_type != 'D'") \
                                .whenNotMatchInsertAll("s1._operation_type != 'D'") 
                                .execute()
  else:
   batch_df.write.option('mergeSchema', 'true').saveAsTable(SilverTableName, mode = 'overwrite')

  except Exception as error:
    raise Exception(f'Error occurred while merging, error msg: {error}')

def deltaload():
  raw_stream_df = spark
                .readStream
                .format("kafka")
                .options(**kafka_opts)
                .option("subscribe", kafkatopic)
                .load()
raw_stream_df.writeStream\
             .format("delta")
             .outputMode("update")\
             .option("mergeschema", "true")\
             .option("checkpointlocation", checkpoint_location)\
             .trigger(once = True)\
             .foreachBatch(pushStreamingdata)\
             .start()


##Run stream
deltaload()































