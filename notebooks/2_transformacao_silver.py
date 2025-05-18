# Transformação SILVER

from pyspark.sql.functions import upper, to_date, col
from utils.validacao import validar_schema, validar_valores_nulos

df = spark.read.format("delta").load("/mnt/datalake/bronze/clientes")

# Validações
schema_esperado = ['id', 'nome', 'email', 'data_nascimento', 'estado']
validar_schema(df, schema_esperado)
validar_valores_nulos(df, ['id', 'nome', 'email'])

# Transformações
df_silver = df.withColumn("nome", upper(col("nome"))) \
              .withColumn("data_nascimento", to_date(col("data_nascimento"), "dd/MM/yyyy")) \
              .filter(col("estado").isin("activo", "pendente"))

df_silver.write.format("delta").mode("overwrite").save("/mnt/datalake/silver/clientes")
