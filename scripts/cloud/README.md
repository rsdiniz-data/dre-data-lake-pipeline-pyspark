# ☁️ Cloud Scripts (Data Lake)

Esta pasta organiza os scripts do projeto seguindo o padrão Medallion (Bronze, Silver e Gold).

O objetivo é separar responsabilidades ao longo do pipeline de dados, garantindo escalabilidade, governança e rastreabilidade.

---

## 🧱 Camadas

### 🪵 Bronze (Raw Data)

* Dados brutos ingeridos da fonte
* Sem transformação relevante
* Preservação da origem

👉 [Ver camada Bronze](./bronze/README.md)

---

### 🥈 Silver (Trusted Data)

* Dados tratados e padronizados
* Aplicação de regras técnicas
* Preparação para modelagem

👉 [Ver camada Silver](./silver/README.md)

---

### 🥇 Gold (Business Data)

* Dados prontos para consumo analítico
* Modelagem dimensional
* Aplicação de regras de negócio

👉 [Ver camada Gold](./gold/README.md)

---

## 🔗 Integração com Documentação

Para visão completa do pipeline:

👉 [Desenvolvimento](../../docs/03_desenvolvimento.md)
