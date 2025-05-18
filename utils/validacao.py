from pyspark.sql.functions import col

def validar_schema(df, schema_esperado):
    campos_faltando = [c for c in schema_esperado if c not in df.columns]
    if campos_faltando:
        raise Exception(f"Campos ausentes: {campos_faltando}")

def validar_valores_nulos(df, colunas_obrigatorias):
    for col_nome in colunas_obrigatorias:
        nulos = df.filter(col(col_nome).isNull()).count()
        if nulos > 0:
            raise Exception(f"Coluna '{col_nome}' contém {nulos} valores nulos.")
