# ETL de Clientes com Databricks

Este projecto implementa uma pipeline ETL robusta com PySpark e Delta Lake, usando dados sintéticos de clientes.

## Arquitectura Medallion

- **Bronze**: ingestão directa do ficheiro CSV
- **Silver**: validação, limpeza e transformação dos dados
- **Gold**: análise e agregação dos dados transformados

## Etapas

1. `1_ingestao_bronze.py`: lê dados crus e grava em Delta na camada bronze
2. `2_transformacao_silver.py`: valida e transforma os dados para a camada silver
3. `3_analise_gold.py`: produz indicadores (ex: clientes por ano) e grava na camada gold

## Validações Incluídas

- Validação de schema
- Verificação de nulos em colunas obrigatórias

## Como Usar

1. Substituir `/mnt/datalake/...` pelos caminhos do teu ambiente
2. Carregar `data/clientes.csv` no Data Lake
3. Executar os notebooks na ordem `1 → 2 → 3`
