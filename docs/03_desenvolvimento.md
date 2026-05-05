# 3. Desenvolvimento do Projeto

## 🔄 Pipeline

1. Ingestão (SharePoint → Bronze)
2. Tratamento (Silver - PySpark)
3. Modelagem (Gold - PySpark)
4. Consumo (Power BI)

---

## 📥 Ingestão (Bronze)

Responsável por:

* Leitura de arquivos no SharePoint (Excel)
* Persistência dos dados brutos no Data Lake
* Manutenção da rastreabilidade da origem

🔗 Documentação:
👉 [03_desenvolvimento.md](./03_desenvolvimento.md)

---

## 🥈 Transformações (Silver - PySpark)

### 📊 PlanoConta

🔗 Script:
👉 [01_plano_conta.py](../scripts/cloud/silver/01_plano_conta.py)

Responsável por:

* Padronização dos dados
* Limpeza e tipagem
* Estruturação da base de contas
* Preparação para modelagem dimensional

---

### 📈 Resultado

🔗 Script:
👉 [02_resultado.py](../scripts/cloud/silver/02_resultado.py)

Regras aplicadas:

* Unpivot de colunas de anos
* Conversão de valores financeiros
* Padronização de estrutura
* Preparação para integração com dimensão

---

## 🥇 Modelagem (Gold - PySpark)

### 📊 dPlanoConta

🔗 Script:
👉 [03_d_plano_conta.py](../scripts/cloud/gold/03_d_plano_conta.py)

Regras:

* Construção de hierarquia por nível (N1, N2, N3)
* Classificação de contas
* Identificação de contas analíticas
* Estruturação da dimensão

---

### 📈 ftResultado

🔗 Script:
👉 [04_ft_resultado.py](../scripts/cloud/gold/04_ft_resultado.py)

Regras:

* Join com dimensão dPlanoConta
* Filtro de contas analíticas
* Estruturação da tabela fato
* Preparação para consumo analítico

---

## ⚙️ Orquestração

Pipeline executado de forma encadeada:

1. Ingestão Bronze
2. Processamento Silver
3. Publicação Gold

Execução pode ser realizada via notebooks ou jobs agendados.

---

## 📊 Consumo

Power BI conectado à camada Gold (dados já tratados e modelados).
