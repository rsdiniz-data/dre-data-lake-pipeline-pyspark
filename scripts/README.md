# 💻 Scripts do Projeto

Esta pasta contém os scripts responsáveis pelo pipeline de dados no Data Lake.

---

## 🔄 Pipeline

1. Ingestão (SharePoint → Bronze)
2. Tratamento (Silver - PySpark)
3. Modelagem (Gold - PySpark)

---

## 📂 Estrutura

### 🪵 Bronze (Raw Data)

Responsável por:

* Ingestão de dados via SharePoint
* Persistência dos dados brutos no Data Lake
* Manutenção da rastreabilidade da origem

📄 Implementação descrita em:
👉 [../docs/03_desenvolvimento.md](../docs/03_desenvolvimento.md)

---

### 🥈 Silver (Trusted Data)

* [01_plano_conta.py](./cloud/silver/01_plano_conta.py)
* [02_resultado.py](./cloud/silver/02_resultado.py)

Responsável por:

* Limpeza e padronização dos dados
* Tipagem e transformação estrutural
* Preparação para modelagem analítica

---

### 🥇 Gold (Business Data)

* [03_d_plano_conta.py](./cloud/gold/03_d_plano_conta.py)
* [04_ft_resultado.py](./cloud/gold/04_ft_resultado.py)

Responsável por:

* Modelagem dimensional (Star Schema)
* Aplicação de regras de negócio
* Preparação para consumo analítico

---

## 🔗 Integração com Docs

Ver detalhes completos do pipeline em:
👉 [../docs/03_desenvolvimento.md](../docs/03_desenvolvimento.md)
