# Databricks notebook source
# MAGIC %md
# MAGIC #Azure Databricks Project Circuits

# COMMAND ----------

# MAGIC %md
# MAGIC ####Connecting to Azure Data Lake

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datamainperu.dfs.core.windows.net",
    "Nr6TDbK5oF6QngMTj2Xh6yUjce1/9MZGZnquM7euG3kHmTo2NYawK3n0lTo3/OGFkMscdddI/1WK+AStGEg//Q=="
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Mounting Container (Bronze, Silver, Gold)

# COMMAND ----------

def mount_container(storage_account,container):
    mount_point = f"/mnt/{storage_account}/{container}"
    mounts = dbutils.fs.mounts()
    if any(mount.mountPoint == mount_point for mount in mounts):
        dbutils.fs.unmount(mount_point)

    dbutils.fs.mount(
        source = f"wasbs://{container}@{storage_account}.blob.core.windows.net",
        mount_point = f"/mnt/{storage_account}/{container}",
        extra_configs = {f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net": "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-07-31T12:36:48Z&st=2023-07-31T04:36:48Z&spr=https&sig=fE2h1fn6DsmIe3uiCNZeUGmO7LriuzOwblXKvJ9feao%3D"}
    )
    print(f"{mount_point} has been mounted.")
    display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls('/mnt/datamainperu/bronze')

# COMMAND ----------

mount_container("datamainperu","bronze")
mount_container("datamainperu","silver")
mount_container("datamainperu","gold")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Direct path

# COMMAND ----------

fs_path_raw="abfss://bronze@datamainperu.dfs.core.windows.net"
fs_path_processed="abfss://silver@datamainperu.dfs.core.windows.net"
fs_path_presentation="abfss://gold@datamainperu.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rutas montadas

# COMMAND ----------

fs_path_raw="/mnt/datamainperu/bronze"
fs_path_processed="/mnt/datamainperu/silver"
fs_path_presentation="/mnt/datamainperu/gold"

# COMMAND ----------

#display(dbutils.fs.ls(fs_path_raw))
#display(dbutils.fs.ls(fs_path_processed))
#display(dbutils.fs.ls(fs_path_presentation))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw >>> Processed

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read\
              .option("header",True)\
              .schema(circuits_schema)\
              .csv(f"{fs_path_raw}/circuits.csv")
display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(
                        col("circuitId"),
                        col("circuitRef"),
                        col("name"),
                        col("location"),
                        col("country").alias("race_country"),
                        col("lat"),
                        col("lng"),
                        col("alt")
                      )

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS circuits_processed_silver

# COMMAND ----------

dbutils.fs.rm("/mnt/datamainperu/silver/circuits_processed_silver_py", recurse=True)
dbutils.fs.mkdirs("/mnt/datamainperu/silver/circuits_processed_silver_py")

# COMMAND ----------

#circuits_selected_df.write.mode("overwrite").format("parquet").saveAsTable("circuits_processed_silver")
circuits_selected_df.write.format("parquet").option("path",f"/mnt/datamainperu/silver/circuits_processed_silver_py").saveAsTable("circuits_processed_silver")

# COMMAND ----------

df = spark.sql("SELECT * FROM circuits_processed_silver")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processed >>> Presentation

# COMMAND ----------

circuits_df_processed = spark.read.parquet(f"{fs_path_processed}/circuits_processed_silver_py") \
.withColumnRenamed("location","circuit_location")
display(circuits_df_processed)

# COMMAND ----------

races_df = spark.read\
              .option("header",True)\
              .schema(circuits_schema)\
              .csv(f"{fs_path_raw}/races.csv")
display(races_df)

# COMMAND ----------

df_presentation = circuits_df_processed.join(races_df,'circuitId','left').select( circuits_df_processed.circuitId,
                                                                circuits_df_processed.name,
                                                                races_df.circuitRef,
                                                                races_df.country)
df_presentation.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS circuits_presentation_gold

# COMMAND ----------

dbutils.fs.rm("/mnt/datamainperu/silver/circuits_presentation_gold_py", recurse=True)
dbutils.fs.mkdirs("/mnt/datamainperu/silver/circuits_presentation_gold_py")

# COMMAND ----------

df_presentation.write.format("parquet").option("path",f"/mnt/datamainperu/silver/circuits_presentation_gold_py").saveAsTable("circuits_presentation_gold")

# COMMAND ----------

df = spark.sql("SELECT * FROM circuits_presentation_gold")
df.show()
