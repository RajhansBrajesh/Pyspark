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









