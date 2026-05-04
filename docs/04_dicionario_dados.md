# 4. Dicionário de Dados

> Este dicionário descreve as tabelas da camada Gold, preparadas para consumo analítico por ferramentas de visualização e outros consumidores de dados.

## 📊 dPlanoConta

| Coluna           | Tipo     | Descrição                              | Relacionamentos                     |
|------------------|----------|----------------------------------------|------------------------------------|
| `ID_Conta`       | STRING   | Código da conta                        | 1:N → `ftResultado[ID_Conta]`       |
| `Descricao`      | STRING   | Nome da conta                          | -                                  |
| `N1`             | STRING   | Nível 1 da hierarquia                  | -                                  |
| `N2`             | STRING   | Nível 2 da hierarquia                  | -                                  |
| `N3`             | STRING   | Nível 3 da hierarquia                  | -                                  |
| `CodDRE`         | STRING   | Código principal da DRE                | -                                  |
| `TipoIndicador`  | INT      | Receita (+1) / Despesa (-1)            | -                                  |

🔗 Script:  
👉 [03_d_plano_conta.py](../scripts/cloud/gold/03_d_plano_conta.py)

---

## 📈 ftResultado

| Coluna        | Tipo     | Descrição                     | Relacionamentos               |
|--------------|----------|-------------------------------|------------------------------|
| `ID_Conta`   | STRING   | Conta contábil (FK)           | N:1 → `dPlanoConta[ID_Conta]`|
| `Data`       | DATE     | Data (ano de referência)      | -                            |
| `Valor`      | DOUBLE   | Valor financeiro              | -                            |

🔗 Script:  
👉 [04_ft_resultado.py](../scripts/cloud/gold/04_ft_resultado.py)
