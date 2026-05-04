# 1. Justificativa do Projeto

## 🎯 Problema

Mesmo após a evolução para SQL Server, a solução ainda apresentava limitações para cenários de crescimento e arquitetura moderna de dados:

- Limitação de escalabilidade em ambiente local  
- Dependência de infraestrutura gerenciada manualmente  
- Dificuldade de expansão para novos volumes e fontes  
- Baixa flexibilidade para processamento distribuído  

Embora estruturada e performática, a solução ainda não estava preparada para suportar demandas de maior escala e complexidade.

---

## 💡 Solução

Evoluir para uma arquitetura baseada em Data Lake, utilizando o padrão Medallion (Bronze, Silver e Gold):

- Centralização da ingestão em uma fonte governada (SharePoint)  
- Separação clara entre dados brutos, tratados e analíticos  
- Processamento distribuído com PySpark  
- Pipeline automatizado e orquestrado  
- Preparação para escalabilidade e reuso dos dados  

---

## 🔗 Rastreabilidade

- 📥 Ingestão (Bronze → Silver): [Ver Script](../scripts/python/01_ingestao_plano_conta.py)  
- 📊 Transformação Resultado (Bronze → Silver): [Ver Script](../scripts/python/02_ingestao_resultado.py)  
- 🧱 Dimensão dPlanoConta (Silver → Gold): [Ver Script](../scripts/python/03_gold_dplano_conta.py)  
- 🔄 Fato ftResultado (Silver → Gold): [Ver Script](../scripts/python/04_gold_ftresultado.py)  
