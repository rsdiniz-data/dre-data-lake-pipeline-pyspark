# =========================================================
# 02_RESULTADO.py
# Projeto: DRE Embraer | Data Lake + PySpark
#
# Objetivo:
# - Transformar dados financeiros da DRE
# - Converter estrutura colunar em formato analítico (fato)
# - Preparar base para camada Gold (ftResultado)
#
# 🔗 Rastreabilidade:
# - Documento técnico: ../docs/03_desenvolvimento.md
# - Artigo técnico ../docs/06_artigo_tecnico.md
#   3.4 Desenvolvimento dos notebooks em PySpark
#   3.4.2 Notebook 02 – Transformações de Resultado
#
# Pipeline:
# Bronze → Silver (Resultado)
# =========================================================

import nekt
from pyspark.sql import functions as F

# =========================================================
# 1. Parâmetros do processo
# =========================================================
# Referência: docs/03_desenvolvimento.md → Parametrização

bronze_layer_name = "Bronze"
source_table_name = "sharepoint_dfp_basepdfs"

silver_base_path = "/mnt/silver"
output_name = "Resultado"
output_path = f"{silver_base_path}/{output_name}"

# =========================================================
# 2. Inicialização da sessão Spark
# =========================================================

spark = nekt.get_spark_session()

# =========================================================
# 3. Leitura da camada Bronze
# =========================================================
# Referência:
# - docs/02_arquitetura.md → Camada Bronze
# - Separação de responsabilidades (ingestão vs transformação)

df_origem = nekt.load_table(
    layer_name=bronze_layer_name,
    table_name=source_table_name
)

# =========================================================
# 4. Limpeza prévia
# =========================================================
# Referência:
# - 3.4.2.1 → Limpeza prévia dos registros
#
# Regras:
# - Remover linhas sem código contábil
# - Remover registros sem valores em todos os períodos
#
# Objetivo de negócio:
# - Garantir qualidade dos dados desde a origem

df_limpo = (
    df_origem
    .withColumn("codigo_da_conta", F.trim(F.col("codigo_da_conta")))
    .withColumn("descricao_da_conta", F.trim(F.col("descricao_da_conta")))
)

df_limpo = df_limpo.filter(
    F.col("codigo_da_conta").isNotNull() & (F.col("codigo_da_conta") != "")
)

df_limpo = df_limpo.filter(
    F.coalesce(
        F.col("penultimo_exercicio_01_01_2021_a_31_12_2021"),
        F.col("ultimo_exercicio_01_01_2022_a_31_12_2022"),
        F.col("penultimo_exercicio_01_01_2023_a_31_12_2023"),
        F.col("ultimo_exercicio_01_01_2024_a_31_12_2024")
    ).isNotNull()
)

df_limpo = df_limpo.dropDuplicates()

# =========================================================
# 5. Unpivot (transformação crítica)
# =========================================================
# Referência:
# - 3.4.2.2 → Unpivot das colunas de exercício
#
# Objetivo técnico:
# - Converter colunas de anos em linhas
#
# Objetivo de negócio:
# - Viabilizar análise temporal (YoY, tendência, variação)

expr_unpivot = """
stack(
    4,
    '31/12/2021', `penultimo_exercicio_01_01_2021_a_31_12_2021`,
    '31/12/2022', `ultimo_exercicio_01_01_2022_a_31_12_2022`,
    '31/12/2023', `penultimo_exercicio_01_01_2023_a_31_12_2023`,
    '31/12/2024', `ultimo_exercicio_01_01_2024_a_31_12_2024`
) as (data, valor_bruto)
"""

df_unpivot = df_limpo.selectExpr(
    "codigo_da_conta",
    "descricao_da_conta",
    expr_unpivot
)

# =========================================================
# 6. Tratamento do valor
# =========================================================
# Referência:
# - 3.4.2.3 → Tratamento do campo de valor
#
# Regras:
# - Remoção de separador de milhar
# - Ajuste de vírgula decimal
# - Conversão de negativos (parênteses)
#
# Objetivo de negócio:
# - Garantir precisão nos indicadores financeiros

df_tratado = (
    df_unpivot
    .withColumn("valor_bruto", F.trim(F.col("valor_bruto").cast("string")))
    .withColumn(
        "valor_bruto",
        F.when(F.col("valor_bruto").isin("", "-", "null", "None"), None)
         .otherwise(F.col("valor_bruto"))
    )
    .withColumn(
        "valor_bruto",
        F.regexp_replace(F.col("valor_bruto"), r"\.", "")
    )
    .withColumn(
        "valor_bruto",
        F.regexp_replace(F.col("valor_bruto"), ",", ".")
    )
    .withColumn(
        "valor_bruto",
        F.regexp_replace(F.col("valor_bruto"), r"^\((.*)\)$", r"-\1")
    )
)

# =========================================================
# 7. Tipagem + Data + Ano
# =========================================================
# Referência:
# - 3.4.2.4 → Criação da coluna data
# - 3.4.2.5 → Criação da coluna ano
#
# Objetivo:
# - Padronizar estrutura para modelo analítico
# - Preparar particionamento

df_resultado = (
    df_tratado
    .withColumn("codigo_da_conta", F.col("codigo_da_conta").cast("string"))
    .withColumn("descricao_da_conta", F.col("descricao_da_conta").cast("string"))
    .withColumn("data", F.to_date(F.col("data"), "dd/MM/yyyy"))
    .withColumn("valor", F.col("valor_bruto").cast("double"))
    .withColumn("ano", F.year(F.col("data")))
    .drop("valor_bruto")
)

# =========================================================
# 8. Limpeza final
# =========================================================
# Referência:
# - 3.4.2.6 → Remoção de nulos e duplicidades
#
# Objetivo:
# - Garantir consistência da Silver

df_resultado = df_resultado.filter(F.col("valor").isNotNull())
df_resultado = df_resultado.dropDuplicates()

# =========================================================
# 9. Persistência particionada (Parquet)
# =========================================================
# Referência:
# - 3.4.2.7 → Gravação particionada em parquet
# - docs/05_entrega_valor.md → Performance (formato columnar + particionamento)
#
# Objetivo técnico:
# - Melhorar performance de leitura
# - Reduzir custo de processamento
#
# Objetivo de negócio:
# - Escalar consultas analíticas por período

df_resultado.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("ano") \
    .save(output_path)
