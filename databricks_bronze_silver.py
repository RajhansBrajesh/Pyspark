df_raw = spark.read.option("header", "true").csv(file_path)
df = df_raw.withColumn("ADLS_LOADED_DATE", F.lit(triggerdate))

#To replace special chartecter in column names
df = df.select([F.col(col).alias(col.replace('-','')) for col in df.columns])

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
####################################################################################################
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









