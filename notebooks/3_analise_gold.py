# Análise GOLD

from pyspark.sql.functions import year, count
df = spark.read.format("delta").load("/mnt/datalake/silver/clientes")

# Agregação: clientes por ano de nascimento
df_resultado = df.withColumn("ano_nasc", year("data_nascimento")) \
                 .groupBy("ano_nasc").agg(count("*").alias("total_clientes"))

df_resultado.write.format("delta").mode("overwrite").save("/mnt/datalake/gold/clientes_por_ano")
df_resultado.show()
