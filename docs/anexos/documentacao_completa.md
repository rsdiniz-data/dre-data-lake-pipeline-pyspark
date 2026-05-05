# SOLUÇÃO DE ENGENHARIA DE DADOS  
## PROJETO: DRE Embraer | Data Lake (Medallion) + PySpark + Nekt Cloud  
**Autor:** Ricardo Diniz  
**Plataforma:** Nekt Cloud  
**Arquitetura:** Data Lake (Medallion – Bronze, Silver, Gold)  
**Orquestração:** Notebooks (execução encadeada)  
**Tecnologias Utilizadas:**  
•	PySpark  
•	SharePoint Connector  
•	Data Lake (Parquet – formato columnar)  
•	Google Colab (desenvolvimento dos notebooks)  
•	Google BigQuery (integração nativa da plataforma)  
•	Power BI (Ferramenta de visualização) 
**Repositório com scripts e documentação técnica:** GitHub - Projeto DRE Nekt Cloud 


________________________________________
Sumário
## 1. Objetivo do Projeto
1.1 Justificativa
1.2 Plataforma Utilizada
## 2. Arquitetura da Solução
2.1 Visão Geral da Arquitetura
2.2 Camadas do Data Lake
2.2.1 Camada Bronze (Raw Data)
2.2.2 Camada Silver (Trusted Data)
2.2.3 Camada Gold (Business Data)
2.3 Tecnologias Utilizadas
## 3. Desenvolvimento do Projeto
3.1 Criação do Workspace e do Data Lake
3.2 Ingestão dos dados da DRE com o conector SharePoint
3.3 Estrutura do catálogo e publicação das camadas
3.4 Desenvolvimento dos notebooks em PySpark
3.4.1 Notebook 01 – Transformações de PlanoConta na Silver
3.4.2 Notebook 02 – Transformações de Resultado na Silver
3.4.3 Notebook 03 – Publicação de dPlanoConta na Gold
3.4.4 Notebook 04 – Publicação de ftResultado na Gold
3.5 Integração com o Power BI
3.5.1 Configuração da Conexão
3.5.2 Benefícios da Integração
3.6 Orquestração do Pipeline
3.6.1 Estrutura do Pipeline
3.6.2 Lógica de Orquestração
3.7. Evolução da Solução
3.7.1 Comparativo entre Projetos
3.7.2 Ganhos Obtidos
3.8 Resultado para o Negócio
3.9 Conclusão da etapa de desenvolvimento
## 4. Dicionário de Dados
4.1 Tabela dPlanoConta
4.1.1 Descrição da Tabela
4.1.2 Estrutura da Tabela
4.2 Tabela ftResultado
4.2.1 Descrição da Tabela
4.2.2 Estrutura da Tabela
## 5. Entrega de Valor
5.1 Perspectiva de Negócio
5.2 Perspectiva Técnica
5.3 Consolidação da Entrega
________________________________________
## 1. Objetivo do Projeto
## 1.1 Justificativa
Este projeto tem como objetivo estruturar uma solução de dados para análise da DRE da Embraer, a partir de dados públicos armazenados em SharePoint, utilizando uma arquitetura moderna em nuvem.
Antes da adoção da arquitetura em nuvem, o projeto evoluiu por duas etapas:
**• Power BI (arquivos locais)** – permitiu uma implementação rápida, porém com baixa governança, forte dependência de arquivos e limitações de performance
*Ver solução:* Acessar projeto no GitHub
• SQL Server (ambiente local) – trouxe centralização, maior controle e ganhos de desempenho, mas ainda com restrições de escalabilidade
Ver solução: Acessar projeto no GitHub
Versão atual:
• Data Lake em Cloud (Nekt + PySpark) – arquitetura orientada à escalabilidade, governança e automação
Nessa etapa, a solução foi estruturada no padrão Medallion (Bronze, Silver e Gold), organizando o fluxo de dados entre ingestão, tratamento e consumo.
Principais ganhos:
• Governança e rastreabilidade dos dados
• Automação do pipeline
• Escalabilidade para crescimento de volume e novas fontes
• Redução de dependência de infraestrutura local
• Base preparada para consumo analítico e reuso
• Uso eficiente de recursos em nuvem, com ingestão seletiva e otimização de armazenamento
1.2 Plataforma Utilizada
Para este novo estágio do projeto, eu escolhi utilizar a Nekt, que é uma plataforma de dados moderna voltada para a unificação e governança de informações.
A Nekt funciona como um ambiente único que conecta diversas fontes, organiza os dados em camadas e os deixa prontos tanto para nós, humanos, quanto para agentes de inteligência artificial.
Ela resolve problemas críticos como silos de dados e falta de histórico, criando uma camada de dados governada onde tudo é versionado e auditável.
Componentes principais:
• Ingestão via SharePoint
• Armazenamento em camadas (Bronze, Silver e Gold)
• Transformações com PySpark
• Orquestração de pipeline
• Integração com Power BI via BigQuery
________________________________________
2. Arquitetura da Solução
2.1 Visão Geral da Arquitetura
O projeto foi desenvolvido utilizando a arquitetura Medallion, estruturando o fluxo de dados em três camadas principais: Bronze, Silver e Gold.
Essa abordagem permite separar claramente responsabilidades entre ingestão, tratamento e consumo, garantindo escalabilidade, governança e reprocessamento controlado.
Fluxo da arquitetura:
SharePoint → Bronze → Silver → Gold → Power BI
________________________________________
2.2 Camadas do Data Lake
2.2.1 Camada Bronze (Raw Data)
Responsável por armazenar os dados brutos sem transformação significativa.
Fontes:
•	PlanoContas.xlsx 
•	DFP.xlsx 
Tabelas:
•	sharepoint_planocontas_planocontas 
•	sharepoint_dfp_basepdf 
Objetivo de negócio:
Centralizar a origem dos dados, eliminando dependência de arquivos locais e garantindo rastreabilidade.
________________________________________
2.2.2 Camada Silver (Trusted Data)
Responsável por aplicar transformações, limpeza e padronização dos dados.
Artefatos:
•	PlanoConta (parquet) 
•	Resultado (parquet particionado por ano) 
Transformações principais:
•	Limpeza de dados 
•	Estruturação hierárquica 
•	Unpivot 
•	Padronização de valores 
Objetivo de negócio:
Garantir qualidade, consistência e estrutura analítica para suportar decisões confiáveis.
________________________________________
2.2.3 Camada Gold (Business Data)
Responsável por disponibilizar os dados prontos para consumo analítico.
Tabelas:
•	dPlanoConta 
•	ftResultado 
Formatos:
•	Tabela (para BI) 
•	Parquet (para reuso técnico) 
Objetivo de negócio:
Disponibilizar dados prontos para análise, com alta performance e fácil entendimento pelo usuário final.
________________________________________
2.3 Tecnologias Utilizadas
•	Plataforma Nekt (Data Lake) 
•	PySpark (processamento distribuído) 
•	Google Colab (desenvolvimento) 
•	SharePoint (fonte de dados) 
•	Google BigQuery (integração) 
•	Power BI (visualização)










________________________________________
3. Desenvolvimento do Projeto
3.1 Criação do Workspace e do Data Lake
Objetivo: Estruturar o ambiente de dados em uma arquitetura escalável e organizada, definindo camadas com responsabilidades bem definidas para ingestão, transformação e consumo analítico.
Na plataforma da Nekt, foi configurado um Workspace e adicionado um Data Lake com arquitetura Medallion. Essa definição organiza o fluxo em camadas com responsabilidades distintas: Bronze para dados brutos, Silver para dados tratados e Gold para dados prontos para consumo analítico.
Camada	Objetivo	Artefatos do projeto
Bronze	Receber dados brutos sem transformação relevante.	sharepoint_dfp_basepdf; sharepoint_planocontas_planocontas
Silver	Aplicar limpeza, padronização técnica e estruturação analítica.	PlanoConta (parquet); Resultado (parquet particionado por ano)
Gold	Publicar entidades prontas para consumo em BI e reuso analítico.	dPlanoConta (tabela e parquet); ftResultado (tabela e parquet)










________________________________________
3.2 Ingestão dos dados da DRE com o conector SharePoint
Objetivo: Centralizar e automatizar a entrada dos dados no Data Lake, eliminando dependências de arquivos locais e garantindo confiabilidade na origem dos dados.
Ver implementação técnica: Acessar documento no GitHub
A ingestão dos dados brutos foi realizada utilizando o conector SharePoint da própria plataforma Nekt. A pasta monitorada continha os arquivos PlanoContas.xlsx e DFP.xlsx, que passaram a ser a fonte oficial do processo de carga.
A utilização do conector elimina a dependência de caminhos locais e cria uma entrada controlada para o Data Lake. Com isso, o processo passa a depender de uma origem centralizada, mais adequada para cenários de automação, auditoria e reprocessamento.
 
Figura 1 – Configuração do conector SharePoint na plataforma Nekt.
As tabelas geradas a partir desses arquivos foram chamadas de sharepoint_dfp_basepdf e sharepoint_planocontas_planocontas e adicionadas diretamente na camada Bronze do Data Lake.
________________________________________
3.3 Estrutura do catálogo e publicação das camadas
Objetivo: Organizar e disponibilizar os dados nas diferentes camadas do Data Lake, garantindo separação entre dados brutos, dados tratados e dados prontos para consumo analítico.
Após a ingestão, a camada Bronze passou a armazenar as tabelas brutas provenientes do SharePoint. Na sequência, os notebooks publicaram os resultados tratados na Silver em formato parquet e, posteriormente, as entidades analíticas na Gold como tabelas e também como arquivos parquet.
 
Figura 2 – Catálogo da solução com objetos nas camadas Bronze e Gold.
Essa organização em catálogo evidencia a separação entre a origem dos dados e os objetos de consumo analítico, reforçando o papel de cada camada dentro da arquitetura do projeto.
________________________________________
3.4 Desenvolvimento dos notebooks em PySpark
Objetivo: Implementar as transformações de dados de forma escalável e padronizada, utilizando processamento distribuído para construção das camadas Silver e Gold.
Foram criados quatro notebooks desenvolvidos no Google Colab, utilizando o token gerado na Nekt para autenticação e execução. O desenvolvimento foi realizado em PySpark, tecnologia adequada para processamento distribuído e aderente ao contexto de Data Lake.
Notebook	Origem	Destino
01_INGESTAO_DADOS_PLANO_CONTA	Bronze.sharepoint_planocontas_planocontas	Silver.PlanoConta (.parquet)
02_INGESTAO_DADOS_RESULTADO	Bronze.sharepoint_dfp_basepdf	Silver.Resultado (.parquet particionado por ano)
03_INGESTAO_DADOS_dPLANO_CONTA	Silver.PlanoConta	Gold.dPlanoConta (tabela e .parquet)
04_INGESTAO_DADOS_ftRESULTADO	Silver.Resultado + Silver.PlanoConta	Gold.ftResultado (tabela e .parquet)

________________________________________
3.4.1 Notebook 01 – Transformações de PlanoConta na Silver
Objetivo:
Transformar a tabela sharepoint_planocontas_planocontas em uma estrutura técnica pronta para reutilização, preservando a lógica hierárquica da DRE.
Ver implementação técnica: Acessar script no GitHub
________________________________________
3.4.1.1 Leitura da Bronze.
O notebook lê a tabela bruta do plano de contas diretamente da camada Bronze, isolando a etapa de ingestão da etapa de tratamento.
Negócio:
Garante separação de responsabilidades, permitindo reprocessamento dos dados sem depender da fonte original.
________________________________________
3.4.1.2 Ordenação por id_conta
A ordenação garante o comportamento correto do preenchimento hierárquico no ambiente Spark, reproduzindo a lógica de FillDown usada anteriormente no Power Query.
Negócio:
Assegura consistência na estrutura contábil, evitando distorções na hierarquia da DRE.



________________________________________
3.4.1.3 Cálculo do comprimento do código
O tamanho de id_conta é utilizado para identificar o nível hierárquico da conta e distinguir grupo principal, subgrupo e conta analítica.
Negócio:
Permite identificar automaticamente a estrutura da DRE sem depender de regras manuais.
________________________________________
3.4.1.4 Criação das colunas n1, n2 e n3
Essas colunas estruturam a hierarquia contábil e permitem análises futuras por agrupamento e navegação na DRE.
Negócio:
Viabiliza análises gerenciais por nível (ex.: Receita → Subgrupo → Conta), fundamentais para tomada de decisão.
________________________________________
3.4.1.5 FillDown dos níveis
O preenchimento dos níveis superiores garante que cada conta herde corretamente o contexto hierárquico ao qual pertence.
Negócio:
Evita perda de contexto nas análises e garante consistência na agregação dos valores.
________________________________________
3.4.1.6 Criação de cod_dre
O código do grupo principal passa a identificar a grande linha da DRE à qual a conta pertence, apoiando consolidação e indicadores.
Negócio:
Facilita a construção de KPIs financeiros como Receita, Custos, EBITDA e Lucro Líquido.
________________________________________
3.4.1.7 Criação de tipo_indicador
A classificação em 1 e -1 separa linhas com impacto positivo e redutor no resultado, suportando cálculos e leituras corretas dos indicadores financeiros.
Negócio:
Garante que cálculos como margens e resultados sejam realizados corretamente sem ajustes manuais.
________________________________________
3.4.1.8 Gravação em parquet na Silver
O resultado é salvo como PlanoConta em formato parquet, criando uma camada tratada e reutilizável para etapas posteriores.
Negócio:
Cria uma base padronizada e performática para consumo por múltiplos processos e ferramentas analíticas.


________________________________________
3.4.2 Notebook 02 – Transformações de Resultado na Silver
Objetivo:
Transformar a tabela sharepoint_dfp_basepdf em uma estrutura analítica do tipo conta–data–valor, adequada para análise temporal e futura composição da tabela fato.
Ver implementação técnica: Acessar script no GitHub
________________________________________
3.4.2.1 Limpeza prévia dos registros
O notebook remove linhas sem código contábil e elimina registros em que todos os períodos estão vazios, reduzindo ruído da extração original.
Negócio:
Garante qualidade dos dados desde a origem, evitando que registros inválidos impactem análises financeiras e indicadores.
________________________________________
3.4.2.2 Unpivot das colunas de exercício
A conversão do formato colunar para o formato analítico transforma os anos em linhas, permitindo comparações temporais, variações e inteligência de tempo.
Negócio:
Viabiliza análises temporais como crescimento, comparação entre períodos e cálculo de variações (YoY).
________________________________________
3.4.2.3 Tratamento do campo de valor
São tratados separadores de milhar, vírgula decimal, valores nulos e valores negativos entre parênteses, garantindo integridade numérica.
Negócio:
Evita erros de cálculo e garante precisão nas análises financeiras, especialmente em indicadores como margem e resultado.
________________________________________
3.4.2.4 Criação da coluna data
Os exercícios são padronizados como datas de fechamento, o que viabiliza modelagem temporal e integração futura com calendário.
Negócio:
Permite análises por período, construção de séries históricas e integração com dimensões de tempo.
________________________________________
3.4.2.5 Criação da coluna ano
A coluna ano é utilizada exclusivamente para particionamento técnico do parquet, melhorando organização e potencialmente a performance de leitura.
Negócio:
Otimiza consultas e reduz custo computacional ao acessar apenas partições relevantes dos dados.
________________________________________
3.4.2.6 Remoção de nulos e duplicidades
O conjunto final preserva apenas registros válidos, garantindo maior consistência para a camada Silver.
Negócio:
Evita duplicidade de valores e inconsistências que poderiam comprometer análises e relatórios.
________________________________________
3.4.2.7 Gravação particionada em parquet
O notebook salva o resultado como Resultado, particionado por ano, preparando a base para a publicação da tabela fato na Gold.
Negócio:
Cria uma base performática e escalável, pronta para consumo e reuso em múltiplos cenários analíticos.





















________________________________________
3.4.3 Notebook 03 – Publicação de dPlanoConta na Gold
Objetivo:
Publicar a dimensão dPlanoConta para consumo analítico.
Ver implementação técnica: Acessar script no GitHub
________________________________________
3.4.3.1 Leitura do parquet PlanoConta
A dimensão é lida a partir da Silver, reforçando o desacoplamento entre tratamento técnico e consumo analítico.
Negócio:
Permite reaproveitamento de dados tratados e garante consistência entre diferentes consumidores de dados.
________________________________________
3.4.3.2 Criação da versão para tabela
As colunas são renomeadas apenas na tabela da Gold para nomes amigáveis ao usuário de negócio e ao Power BI.
Negócio:
Facilita o entendimento dos dados por usuários não técnicos e reduz dependência da área de dados.
________________________________________
3.4.3.3 Gravação como tabela Gold
A publicação como tabela facilita descoberta no catálogo e integração com ferramentas analíticas.
Negócio:
Acelera o acesso aos dados e reduz esforço de integração para dashboards e análises.
________________________________________
3.4.3.4 Gravação como parquet Gold
A mesma entidade também é preservada em parquet, garantindo reuso técnico e flexibilidade para novos fluxos.
Negócio:
Permite reutilização dos dados em pipelines futuros, garantindo flexibilidade e escalabilidade da solução.






________________________________________
3.4.4 Notebook 04 – Publicação de ftResultado na Gold
Objetivo:
Publicar a tabela fato final, pronta para consumo no modelo analítico.
Ver implementação técnica: Acessar script no GitHub
________________________________________
3.4.4.1 Leitura de Resultado e PlanoConta
O notebook consome os dois conjuntos tratados da Silver para compor a fato final.
Negócio:
Permite integração entre dados financeiros e estrutura contábil, essencial para análises completas da DRE.
________________________________________
3.4.4.2 Join entre resultado e plano de contas
A integração pelo ID da conta permite recuperar o atributo Lançamento necessário para distinguir contas analíticas de contas sintéticas.
Negócio:
Garante que apenas valores válidos sejam considerados nas análises, evitando duplicidade e distorções.
________________________________________
3.4.4.3 Filtro de contas analíticas
Mantêm-se apenas linhas com Lançamento = 1, evitando duplicidade de somas e distorções na análise financeira.
Negócio:
Assegura que os indicadores financeiros reflitam corretamente o desempenho real da empresa.
________________________________________
3.4.4.4 Exclusão do período de 31/12/2021
O filtro preserva o escopo analítico utilizado no projeto, concentrando a análise nos exercícios posteriores.
Negócio:
Garante consistência temporal e evita comparações fora do escopo definido para análise.







________________________________________
3.5 Integração com o Power BI
3.5.1 Configuração da Conexão
A integração foi realizada utilizando a opção Nekt Express, via conector Google BigQuery (native) no Power BI.
 
Figura 3 – Configuração da fonte de dados no Power BI Desktop.
Essa integração consolida a camada Gold como ponto de consumo do projeto e reforça o papel do Data Lake como base corporativa para análise financeira.
________________________________________
3.5.2 Benefícios da Integração
•	Conexão direta com dados governados 
•	Redução de dependência de arquivos 
•	Atualização centralizada 
•	Escalabilidade para múltiplos relatórios 
Negócio:
Permite que diferentes áreas utilizem a mesma base confiável, evitando retrabalho e divergência de números.










________________________________________
3.6 Orquestração do Pipeline
3.6.1 Estrutura do Pipeline
A execução do pipeline foi automatizada utilizando gatilhos baseados na ingestão do SharePoint.
Fluxo de execução:
•	SharePoint finaliza ingestão 
•	Dispara notebooks 01 e 02 
•	Notebook 03 depende do 01 
•	Notebook 04 depende do 02 


 
Figura 4 – Pipeline de dados com gatilho da fonte SharePoint e execução encadeada dos notebooks.
________________________________________
3.6.2 Lógica de Orquestração
•	Execução paralela na ingestão 
•	Execução sequencial nas transformações 
•	Dependência entre camadas 
Negócio:
Garante consistência do pipeline, evita processamento de dados incompletos, reduz a intervenção manual, padroniza a atualização e fortalece a confiabilidade do processo de ponta a ponta.




________________________________________
3.7. Evolução da Solução
3.7.1 Comparativo entre Projetos
Projeto	Arquitetura	Limitação	Evolução
Projeto 1	Power BI + arquivos locais	Fragilidade e dependência de arquivos	Início da análise
Projeto 2	SQL Server	Centralização, mas limitado a banco local	Estruturação
Projeto 3	Data Lake + PySpark	Escalável e distribuído	Arquitetura moderna
________________________________________
3.7.2 Ganhos Obtidos
Escalabilidade
Processamento distribuído com PySpark permite lidar com grandes volumes de dados.
Governança
Separação por camadas garante controle e rastreabilidade.
Performance
Uso de parquet e particionamento melhora leitura e consultas.
Reusabilidade e flexibilidade
Dados podem ser consumidos por múltiplas ferramentas analíticas, sem dependência de uma tecnologia específica.
Automação
Pipeline elimina processos manuais.
________________________________________
3.8 Resultado para o Negócio
Com a nova arquitetura em nuvem, o projeto passou a oferecer:
•	Centralização da ingestão e do tratamento em um Data Lake;
•	Separação entre dado bruto, dado tratado e dado pronto para consumo;
•	Reprocessamento controlado a partir da Bronze e da Silver;
•	Publicação padronizada das entidades analíticas dPlanoConta e ftResultado;
•	Integração mais estruturada com o Power BI;
•	Base técnica mais aderente a expansão futura do projeto.



________________________________________
3.9 Conclusão da etapa de desenvolvimento
A evolução para Nekt Cloud e PySpark consolidou a transição do projeto para uma arquitetura moderna de dados. Se no primeiro projeto o foco estava na modelagem analítica dentro do Power BI e, no segundo, na centralização em SQL Server, neste terceiro estágio o destaque passa a ser a construção de um pipeline cloud com camadas bem definidas, automação de execução e preparação dos dados para consumo em escala.
Essa evolução demonstra a saída de uma solução local e dependente de arquivos para uma arquitetura baseada em Data Lake e processamento distribuído, mais adequada para cenários de crescimento e maior volume de dados.
Com isso, a solução passa a ser:
•	Escalável 
•	Reutilizável 
•	Governada 
•	Preparada para crescimento 
Além disso, evidencia a capacidade de construção de pipelines completos, desde a ingestão até o consumo analítico, alinhando tecnologia às necessidades de negócio e garantindo uma base sólida para futuras evoluções.


________________________________________
4. Dicionário de Dados
4.1 Tabela dPlanoConta
Ver estrutura dos dados: Acessar dicionário de dados no GitHub
4.1.1 Descrição da Tabela
A tabela dPlanoConta é a dimensão responsável por estruturar a hierarquia do plano de contas da DRE dentro do modelo analítico implementado na arquitetura em Data Lake.
Ela é construída a partir da camada Silver (arquivo parquet PlanoConta) e publicada na camada Gold, estando disponível tanto como tabela quanto como arquivo parquet para consumo analítico e reuso técnico.
A tabela organiza as contas contábeis em níveis hierárquicos (N1, N2 e N3), permitindo análises estruturadas por grupo, subgrupo e contas analíticas. Essa estrutura é essencial para navegação e agregação dos dados financeiros na DRE.
Além disso, contém atributos auxiliares que suportam regras de negócio diretamente na camada de dados, reduzindo dependência de transformações no Power BI e garantindo maior consistência entre diferentes consumidores.
________________________________________
4.1.2 Estrutura da Tabela
Nome da Coluna	Tipo de Dado	Descrição da Coluna	Relacionamentos Ativos
ID Conta	STRING	Código identificador único da conta contábil no plano de contas.	Relacionamento 1:* com ftResultado[ID Conta] (ativo)
Descrição	STRING	Nome ou descrição da conta contábil.	Não possui relacionamento direto
Lançamento	INT	Indicador que define se a conta permite lançamentos diretos. 
1 = Conta analítica 
0 = Conta sintética	Não possui relacionamento direto
Calculado	INT	Indicador auxiliar utilizado na estrutura do plano de contas para controle de cálculos e agregações.	Não possui relacionamento direto
N1	STRING	Nível 1 da hierarquia — Grupo principal da DRE (ex.: Receita, Custos, Despesas).	Não possui relacionamento direto
N2	STRING	Nível 2 da hierarquia — Subgrupo contábil dentro do grupo principal.	Não possui relacionamento direto
N3	STRING	Nível 3 da hierarquia — Conta analítica utilizada para análise detalhada.	Não possui relacionamento direto
CodDRE	STRING	Código do grupo da DRE utilizado para consolidação e cálculo de indicadores financeiros.	Não possui relacionamento direto
TipoIndicador	INT	Classificação da conta utilizada para identificar o tipo de indicador financeiro (ex.: Receita, Custo, Despesa).	Não possui relacionamento direto












________________________________________
4.2 Tabela ftResultado
Ver estrutura dos dados: Acessar dicionário de dados no GitHub
________________________________________
4.2.1 Descrição da Tabela
A tabela ftResultado é a tabela fato do modelo dimensional implementado na arquitetura em Data Lake.
Ela é construída a partir da integração dos dados tratados da camada Silver (Resultado e PlanoConta) e publicada na camada Gold, estando disponível como tabela e também como arquivo parquet.
A tabela armazena os valores financeiros da DRE por conta contábil e período, sendo a base para cálculos, agregações e análises temporais no Power BI.
Durante o processo de transformação, são aplicadas regras de negócio como:
•	seleção apenas de contas analíticas (Lançamento = 1) 
•	filtragem de períodos fora do escopo 
•	padronização dos valores financeiros 
Isso garante consistência, qualidade e confiabilidade dos dados antes do consumo analítico.
________________________________________
4.2.2 Estrutura da Tabela
Nome da Coluna	Tipo de Dado	Descrição da Coluna	Relacionamentos Ativos
ID Conta	STRING	Código da conta contábil associado ao valor financeiro.	Relacionamento *:1 com dPlanoConta[ID Conta] (ativo)
Data	DATE	Data de referência do valor (fechamento do exercício).	Relacionamento *:1 com dCalendario[Data] (ativo)
Valor	DOUBLE	Valor monetário registrado para a conta na data informada.	Não possui relacionamento direto









________________________________________
5. Entrega de Valor
5.1 Perspectiva de Negócio
5.1.1 Governança e confiabilidade dos dados
A solução deixa de depender de arquivos locais e passa a operar com origem controlada em SharePoint, armazenamento em Data Lake e objetos publicados no catálogo.
Isso melhora rastreabilidade, reduz o risco de quebra por movimentação de arquivos e organiza o acesso às entidades finais.
5.1.2 Automação e previsibilidade operacional
A existência de um pipeline com gatilho na fonte e dependências entre notebooks transforma o processo em uma rotina estruturada, reduzindo intervenção manual e aumentando a confiabilidade das execuções.
5.1.3 Flexibilidade de consumo (agnosticismo de ferramenta)
Embora a integração tenha sido realizada com Power BI, a arquitetura construída é independente de ferramenta, permitindo que os dados sejam consumidos por outras soluções como Tableau, Qlik ou qualquer ferramenta compatível.
Isso evita dependência tecnológica e permite que diferentes áreas utilizem a mesma base analítica, reduzindo retrabalho e divergência de informações.
5.1.4 Custo e eficiência operacional
A ingestão de dados foi realizada de forma seletiva, trazendo apenas as tabelas e campos necessários para a análise da DRE.
Além disso, o uso de parquet e o particionamento por ano reduzem o volume de leitura nas consultas.
Isso demonstra controle sobre custos em ambiente cloud, evitando processamento desnecessário e otimizando o uso de recursos.










________________________________________
5.2 Perspectiva Técnica
5.2.1 Escalabilidade e performance
O uso de parquet nas camadas Silver e Gold reduz acoplamento e melhora a eficiência de leitura.
O particionamento por ano na tabela de Resultado organiza o armazenamento e melhora a performance em cenários de crescimento de volume.
5.2.2 Reuso e desacoplamento de dados
A publicação das entidades dPlanoConta e ftResultado como tabela e parquet permite atender tanto o consumo em BI quanto o reuso técnico para novas integrações e análises.
Essa abordagem desacopla o processamento da camada de consumo, aumentando a flexibilidade da arquitetura.
5.2.3 Arquitetura em camadas (Medallion)
A separação entre Bronze, Silver e Gold organiza o fluxo de dados e define responsabilidades claras entre ingestão, tratamento e consumo.
Isso facilita manutenção, reprocessamento e evolução da solução.
________________________________________
5.3 Consolidação da Entrega
O projeto DRE na Nekt consolida a evolução de uma solução baseada em arquivos para uma arquitetura moderna em nuvem, com Data Lake, PySpark e pipeline automatizado.
Com isso, a solução passa a ser:
• Escalável
• Governada
• Reutilizável
• Preparada para crescimento
Além disso, evidencia a capacidade de construção de pipelines completos, desde a ingestão até o consumo analítico, alinhando tecnologia às necessidades de negócio.
Acesso ao dashboard interativo:
Acessar Dashboard no Power BI

