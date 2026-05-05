# =========================================================
# 03_DPLANO_CONTA.py
# Projeto: DRE Embraer | Data Lake + PySpark
#
# Objetivo:
# - Publicar dimensão dPlanoConta na camada Gold
# - Criar camada semântica para consumo analítico
# - Disponibilizar dados para ferramentas de visualização
#
# 🔗 Rastreabilidade:
# - Documento técnico: ../docs/03_desenvolvimento.md
# - Documentação do projeto/artigo: ../docs/anexos/documentacao_dre_cloud.docx
#   3.4 Desenvolvimento dos notebooks em PySpark
#   3.4.3 Notebook 03 – Publicação de dPlanoConta
#
# Pipeline:
# Silver → Gold (dPlanoConta)
# =========================================================

import nekt
from pyspark.sql import functions as F

# =========================================================
# 1. Parâmetros do processo
# =========================================================
# Referência: docs/03_desenvolvimento.md → Parametrização

silver_base_path = "/mnt/silver"
gold_base_path = "/mnt/gold"

input_name = "PlanoConta"
output_name = "dPlanoConta"

input_path = f"{silver_base_path}/{input_name}"
output_path = f"{gold_base_path}/{output_name}"

gold_layer_name = "Gold"
gold_table_name = "dPlanoConta"

# =========================================================
# 2. Inicialização da sessão Spark
# =========================================================

spark = nekt.get_spark_session()

# =========================================================
# 3. Leitura da camada Silver (parquet)
# =========================================================
# Referência:
# - 3.4.3.1 → Leitura do parquet PlanoConta
# - docs/02_arquitetura.md → Camada Silver
#
# Objetivo:
# - Reutilizar dados já tratados
# - Garantir desacoplamento entre transformação e consumo

df = spark.read.parquet(input_path)

# =========================================================
# 4. Criação da camada semântica (nomes amigáveis)
# =========================================================
# Referência:
# - 3.4.3.2 → Criação da versão para tabela
#
# Objetivo técnico:
# - Adaptar nomes para consumo analítico
#
# Objetivo de negócio:
# - Facilitar uso por usuários não técnicos
# - Melhorar usabilidade em ferramentas de BI

df_gold = (
    df
    .withColumnRenamed("id_conta", "ID Conta")
    .withColumnRenamed("descricao", "Descrição")
    .withColumnRenamed("lancamento", "Lançamento")
    .withColumnRenamed("calculado", "Calculado")
    .withColumnRenamed("n1", "N1")
    .withColumnRenamed("n2", "N2")
    .withColumnRenamed("n3", "N3")
    .withColumnRenamed("cod_dre", "CodDRE")
    .withColumnRenamed("tipo_indicador", "TipoIndicador")
)

# =========================================================
# 5. Publicação como tabela na Gold
# =========================================================
# Referência:
# - 3.4.3.3 → Gravação como tabela Gold
# - docs/02_arquitetura.md → Camada Gold
#
# Objetivo técnico:
# - Disponibilizar entidade no catálogo
#
# Objetivo de negócio:
# - Facilitar consumo por dashboards e análises

nekt.save_table(
    df=df_gold,
    layer_name=gold_layer_name,
    table_name=gold_table_name,
    mode="overwrite"
)

# =========================================================
# 6. Persistência em parquet (reuso técnico)
# =========================================================
# Referência:
# - 3.4.3.4 → Gravação como parquet Gold
# - docs/05_entrega_valor.md → Escalabilidade e reuso
#
# Objetivo técnico:
# - Manter versão otimizada (formato columnar)
# - Permitir integração com novos pipelines
#
# Objetivo de negócio:
# - Garantir flexibilidade e escalabilidade da solução

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(output_path)

# Observação:
# A camada Gold mantém duas representações:
# - Tabela: otimizada para consumo analítico
# - Parquet: otimizado para reuso e pipelines futuros
