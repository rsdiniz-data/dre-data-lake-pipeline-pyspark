# 🪵 Bronze (Raw Data)

Camada responsável pela ingestão e armazenamento dos dados brutos no Data Lake.

---

## 🎯 Objetivo

* Preservar os dados na forma original
* Garantir rastreabilidade da origem
* Servir como base para processamento nas camadas superiores

---

## 📥 Ingestão

Os dados são ingeridos a partir do SharePoint:

* Arquivos Excel (Plano de Contas e DRE)
* Sem transformações estruturais relevantes

---

## ⚙️ Processamento

Nesta camada:

* Não há aplicação de regras de negócio
* Transformações são mínimas (ex: leitura e persistência)

---

## 🔗 Integração

A ingestão é descrita em:

👉 [Desenvolvimento](../../../docs/03_desenvolvimento.md)

---

## 📌 Observação

A camada Bronze pode não conter scripts explícitos, pois a ingestão pode ser realizada via conectores ou ferramentas gerenciadas.
