# 📊 DRE Embraer | Data Lake + PySpark + Power BI

## 🧠 Sobre o Projeto

Desenvolvi um projeto para análise da DRE (Demonstração do Resultado do Exercício) da Embraer utilizando uma arquitetura em nuvem baseada em Data Lake.

O desafio: transformar dados públicos (Excel no SharePoint) em informação confiável para análise financeira — agora com foco em escalabilidade, governança e automação.

---

## 🚧 Contexto

Os dados estavam disponíveis em formatos semi-estruturados (Excel), o que dificultava:

* Padronização das análises  
* Reutilização dos dados  
* Escalabilidade da solução  
* Governança e rastreabilidade  

Inicialmente, desenvolvi uma versão em Power BI consumindo diretamente esses arquivos.

Essa abordagem permitiu uma implementação rápida, porém apresentava limitações importantes:

* Dependência de arquivos locais  
* Risco de quebra do relatório  
* Processamento repetitivo no Power BI  
* Baixa governança de dados  

Como evolução, construí uma solução baseada em SQL Server, trazendo maior centralização e controle.

Ainda assim, alguns desafios permaneceram:

* Limitação de escalabilidade em ambiente local  
* Dependência de infraestrutura gerenciada manualmente  
* Dificuldade de expansão para novos volumes e fontes  
* Baixa flexibilidade para processamento distribuído  

Mesmo com ganhos significativos em organização e performance, a solução ainda não estava preparada para cenários de crescimento e para uma arquitetura moderna de dados.

---

## 🔗 Projetos anteriores

Power BI 👉 [*DRE Automatizada – Análise Financeira*](https://github.com/rsdiniz-data/dre-analise-financeira-powerbi) 

SQL Server👉 [*DRE Embraer – SQL Server + Power BI*](https://github.com/rsdiniz-data/dre-data-pipeline-sql-server)

---

## 🚀 Evolução da Solução

Evoluí o projeto para uma arquitetura baseada em Data Lake, utilizando o padrão Medallion (Bronze, Silver e Gold):

* Ingestão automatizada via SharePoint
* Armazenamento em Data Lake
* Transformações com PySpark (processamento distribuído)
* Pipeline orquestrado e automatizado
* Publicação de dados prontos para consumo analítico

Com isso:

* Separação clara entre dado bruto, tratado e analítico
* Processamento distribuído e escalável
* Redução de acoplamento entre ingestão e consumo
* Reuso de dados em múltiplos cenários
* Maior governança e rastreabilidade

Mais do que uma evolução técnica, foi a transição de um ambiente local para uma arquitetura orientada a dados em escala.

---

## 📌 Navegação

* 📄 [Justificativa](./docs/01_justificativa.md)
* 🏗️ [Arquitetura](./docs/02_arquitetura.md)
* ⚙️ [Desenvolvimento](./docs/03_desenvolvimento.md)
* 📊 [Dicionário de Dados](./docs/04_dicionario_dados.md)
* 💡 [Entrega de Valor](./docs/05_entrega_valor.md)

---

## 📊 Arquitetura

* Data Lake no padrão Medallion
* Camada Bronze (dados brutos)
* Camada Silver (dados tratados)
* Camada Gold (dados analíticos)
* PySpark para processamento distribuído
* Power BI como camada de consumo

📷 ![Arquitetura](./images/arquitetura.png)

---

## 🔄 Pipeline

SharePoint → Bronze → Silver → Gold → Power BI

* Ingestão automática via conector
* Execução encadeada de notebooks
* Dependência entre camadas
* Pipeline orientado a eventos

---

## 💻 Scripts

* 🪵 [Camada Bronze (Ingestão)](./docs/03_desenvolvimento.md)
* 📥 [Plano de Contas (Silver)](./scripts/cloud/silver/01_plano_conta.py)  
* 📊 [Resultado (Silver)](./scripts/cloud/silver/02_resultado.py)  
* 🧱 [Dimensão dPlanoConta (Gold)](./scripts/cloud/gold/03_d_plano_conta.py)  
* 🔄 [Fato ftResultado (Gold)](./scripts/cloud/gold/04_ft_resultado.py)  

---

## 💡 Valor para o Negócio

* Centralização da ingestão de dados
* Redução de dependência de arquivos e ambientes locais
* Maior confiabilidade e consistência dos dados
* Escalabilidade para novos volumes e fontes
* Base preparada para múltiplos consumidores (BI e dados)
* Uso eficiente de recursos em nuvem, com ingestão seletiva e otimização de armazenamento

---

## 📢 Links

📊 [Acessar dashboard interativo](link)
📢 [Ler artigo completo](link)

---

## ✅ Conclusão

Este projeto demonstra a evolução de uma solução estruturada em SQL Server para uma arquitetura moderna baseada em Data Lake, com foco em escalabilidade, governança e processamento distribuído.

Mais do que uma melhoria técnica, representa a transição para um modelo alinhado às práticas de Engenharia de Dados, preparado para crescimento e novos cenários analíticos.
