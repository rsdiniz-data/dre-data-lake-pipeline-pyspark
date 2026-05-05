# =========================================================
# 04_FTRESULTADO.py
# Projeto: DRE Embraer | Data Lake + PySpark
#
# Objetivo:
# - Construir e publicar a tabela fato ftResultado
# - Integrar dados financeiros com dimensão contábil
# - Disponibilizar base final para consumo analítico
#
# 🔗 Rastreabilidade:
# - Documento técnico: ../docs/03_desenvolvimento.md
# - Documentação do projeto/artigo: ../docs/anexos/documentacao_dre_cloud.docx
#   3.4 Desenvolvimento dos notebooks em PySpark
#   3.4.4 Notebook 04 – Publicação de ftResultado
#
# Pipeline:
# Silver → Gold (ftResultado)
# =========================================================

import nekt
from pyspark.sql import functions as F

# =========================================================
# 1. Parâmetros do processo
# =========================================================
# Referência: docs/03_desenvolvimento.md → Parametrização

silver_base_path = "/mnt/silver"
gold_base_path = "/mnt/gold"

plano_conta_path = f"{silver_base_path}/PlanoConta"
resultado_path = f"{silver_base_path}/Resultado"

output_parquet_path = f"{gold_base_path}/ftResultado"

gold_layer_name = "Gold"
gold_table_name = "ftResultado"

# =========================================================
# 2. Inicialização da sessão Spark
# =========================================================

spark = nekt.get_spark_session()

# =========================================================
# 3. Leitura da camada Silver
# =========================================================
# Referência:
# - 3.4.4.1 → Leitura de Resultado e PlanoConta
# - docs/02_arquitetura.md → Camada Silver
#
# Objetivo:
# - Reutilizar dados tratados
# - Garantir consistência entre entidades

df_plano_conta = spark.read.parquet(plano_conta_path)
df_resultado = spark.read.parquet(resultado_path)

# =========================================================
# 4. Join com dimensão (PlanoConta)
# =========================================================
# Referência:
# - 3.4.4.2 → Join entre resultado e plano de contas
#
# Objetivo técnico:
# - Enriquecer fato com atributos da dimensão
#
# Objetivo de negócio:
# - Identificar contas analíticas vs sintéticas

df_merge = (
    df_resultado.alias("r")
    .join(
        df_plano_conta.alias("p"),
        F.col("r.codigo_da_conta") == F.col("p.id_conta"),
        "left"
    )
)

# =========================================================
# 5. Seleção das colunas relevantes
# =========================================================
# Referência: preparação para modelo dimensional

df_expandido = df_merge.select(
    F.col("r.codigo_da_conta"),
    F.col("r.data"),
    F.col("r.valor"),
    F.col("r.ano"),
    F.col("p.lancamento")
)

# =========================================================
# 6. Filtro de contas analíticas + escopo temporal
# =========================================================
# Referência:
# - 3.4.4.3 → Filtro de contas analíticas
# - 3.4.4.4 → Exclusão do período 31/12/2021
#
# Regras:
# - Considerar apenas Lancamento = 1
# - Excluir período fora do escopo
#
# Objetivo de negócio:
# - Evitar duplicidade
# - Garantir consistência nos indicadores

df_filtrado = df_expandido.filter(
    (F.col("lancamento") == 1) &
    (F.col("data") != F.to_date(F.lit("2021-12-31")))
)

# =========================================================
# 7. Estrutura técnica (parquet)
# =========================================================
# Objetivo:
# - Manter estrutura otimizada para reuso técnico

df_final = df_filtrado.drop("lancamento")

# =========================================================
# 8. Camada semântica (tabela Gold)
# =========================================================
# Objetivo:
# - Adaptar nomes para consumo analítico
#
# Observação:
# - Remoção da coluna "ano" (uso técnico de particionamento)

df_gold = (
    df_final
    .select(
        F.col("codigo_da_conta").alias("ID Conta"),
        F.col("data").alias("Data"),
        F.col("valor").alias("Valor")
    )
)

# =========================================================
# 9. Publicação como tabela Gold
# =========================================================
# Referência:
# - docs/02_arquitetura.md → Camada Gold
#
# Objetivo técnico:
# - Disponibilizar entidade para consumo direto
#
# Objetivo de negócio:
# - Acelerar construção de dashboards e análises

nekt.save_table(
    df=df_gold,
    layer_name=gold_layer_name,
    table_name=gold_table_name,
    mode="overwrite"
)

# =========================================================
# 10. Persistência em parquet particionado
# =========================================================
# Referência:
# - docs/05_entrega_valor.md → Performance e escalabilidade
#
# Objetivo técnico:
# - Otimizar leitura por período (partition pruning)
#
# Objetivo de negócio:
# - Reduzir custo e tempo de consulta

df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("ano") \
    .save(output_parquet_path)

# Observação:
# A tabela ftResultado representa a granularidade mínima (conta + data),
# permitindo agregações seguras no modelo analítico.
