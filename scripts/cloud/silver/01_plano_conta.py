# =========================================================
# 01_PLANO_CONTA.py
# Projeto: DRE Embraer | Data Lake + PySpark
#
# Objetivo:
# - Ler dados da camada Bronze
# - Aplicar transformações técnicas (Silver)
# - Preparar dados para modelagem analítica
#
# 🔗 Rastreabilidade:
# - Documento técnico: ../docs/03_desenvolvimento.md
# - Artigo técnico ../docs/06_artigo_tecnico.md
#   3.4 Desenvolvimento dos notebooks em PySpark
#   3.4.1 Notebook 01 – Transformações de PlanoConta
#
# Pipeline:
# Bronze → Silver (PlanoConta)
# =========================================================

import nekt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================================================
# 1. Parâmetros do processo
# =========================================================
# Referência: docs/03_desenvolvimento.md → Parametrização

bronze_layer_name = "Bronze"
source_table_name = "sharepoint_planocontas_planocontas"

silver_base_path = "/mnt/silver"
output_name = "PlanoConta"
output_path = f"{silver_base_path}/{output_name}"

# =========================================================
# 2. Inicialização da sessão Spark
# =========================================================

spark = nekt.get_spark_session()

# =========================================================
# 3. Leitura da Bronze
# =========================================================
# Referência:
# - 3.4.1.1 → Leitura da Bronze
# - docs/02_arquitetura.md → Camada Bronze

df_origem = nekt.load_table(
    layer_name=bronze_layer_name,
    table_name=source_table_name
)

# =========================================================
# 4. Ordenação dos registros
# =========================================================
# Referência:
# - 3.4.1.2 → Ordenação por id_conta
# Regra:
# - Necessária para garantir consistência do FillDown

df_ordenado = df_origem.orderBy(F.col("id_conta"))

janela_rownum = Window.orderBy(F.col("id_conta"))

df_ordenado = df_ordenado.withColumn(
    "row_id",
    F.row_number().over(janela_rownum)
)

janela_filldown = Window.orderBy("row_id").rowsBetween(
    Window.unboundedPreceding, 0
)

# =========================================================
# 5. Identificação da hierarquia
# =========================================================
# Referência:
# - 3.4.1.3 → Cálculo do comprimento
# - 3.4.1.4 → Criação dos níveis n1, n2, n3

df_hierarquia = df_ordenado.withColumn(
    "comprimento",
    F.length(F.col("id_conta"))
)

df_hierarquia = df_hierarquia.withColumn(
    "n1",
    F.when(F.col("comprimento") == 4, F.col("descricao"))
)

df_hierarquia = df_hierarquia.withColumn(
    "n2",
    F.when(F.col("comprimento") == 7, F.col("descricao"))
     .when(F.col("comprimento") == 4, F.lit("XXX"))
)

df_hierarquia = df_hierarquia.withColumn(
    "n3",
    F.when(F.col("comprimento") == 10, F.col("descricao"))
)

# =========================================================
# 6. FillDown da hierarquia
# =========================================================
# Referência:
# - 3.4.1.5 → FillDown dos níveis
# Objetivo:
# - Propagar hierarquia para níveis inferiores

df_hierarquia = df_hierarquia.withColumn(
    "n1",
    F.last("n1", ignorenulls=True).over(janela_filldown)
)

df_hierarquia = df_hierarquia.withColumn(
    "n2",
    F.last("n2", ignorenulls=True).over(janela_filldown)
)

df_hierarquia = df_hierarquia.withColumn(
    "n2",
    F.when(F.col("n2") == "XXX", F.lit(None)).otherwise(F.col("n2"))
)

# =========================================================
# 7. Criação do cod_dre
# =========================================================
# Referência:
# - 3.4.1.6 → Criação de cod_dre

df_hierarquia = df_hierarquia.withColumn(
    "cod_dre",
    F.when(F.col("comprimento") == 4, F.col("id_conta"))
)

df_hierarquia = df_hierarquia.withColumn(
    "cod_dre",
    F.last("cod_dre", ignorenulls=True).over(janela_filldown)
)

# =========================================================
# 8. Conversão de tipos
# =========================================================
# Referência: Padronização técnica (Silver)

df_tipado = (
    df_hierarquia
    .withColumn("cod_dre", F.col("cod_dre").cast("string"))
    .withColumn("id_conta", F.col("id_conta").cast("string"))
    .withColumn("lancamento", F.col("lancamento").cast("long"))
    .withColumn("descricao", F.col("descricao").cast("string"))
    .withColumn("n1", F.col("n1").cast("string"))
    .withColumn("n2", F.col("n2").cast("string"))
    .withColumn("n3", F.col("n3").cast("string"))
    .withColumn("calculado", F.col("calculado").cast("long"))
)

# =========================================================
# 9. Classificação financeira
# =========================================================
# Referência:
# - 3.4.1.7 → Criação de tipo_indicador

df_tipado = df_tipado.withColumn(
    "tipo_indicador",
    F.when(F.col("cod_dre").isin("3.02", "3.04"), F.lit(-1))
     .otherwise(F.lit(1))
     .cast("long")
)

# =========================================================
# 10. Limpeza
# =========================================================

df_final = df_tipado.drop("comprimento", "row_id")

# =========================================================
# 11. Persistência na Silver
# =========================================================
# Referência:
# - 3.4.1.8 → Gravação em parquet
# - docs/02_arquitetura.md → Camada Silver
# - docs/05_entrega_valor.md → Performance (formato colunar)

df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(output_path)
