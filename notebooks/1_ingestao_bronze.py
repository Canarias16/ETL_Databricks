# Ingestão BRONZE

from pyspark.sql.functions import input_file_name
df = spark.read.option("header", "true").csv("/mnt/datalake/raw/clientes.csv")
df = df.withColumn("source_file", input_file_name())
df.write.format("delta").mode("overwrite").save("/mnt/datalake/bronze/clientes")
