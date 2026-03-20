# ☁️ Pipeline de Análise de Churn e Receita (SaaS) com Azure Databricks

## 📌 Visão Geral
Este projeto demonstra a construção de um pipeline de engenharia e análise de dados de ponta a ponta utilizando **Azure Databricks**. O objetivo principal é analisar o comportamento de cancelamento (Churn) e a receita de uma empresa fictícia de Software as a Service (SaaS).

A arquitetura foi desenhada seguindo as melhores práticas do padrão **Medallion Architecture (Lakehouse)**, transformando dados brutos e não estruturados em tabelas agregadas prontas para o consumo de ferramentas de Business Intelligence (BI).

---

## 🎯 Objetivo de Negócio
Em empresas SaaS, reter clientes é tão importante quanto adquirir novos. Este projeto visa responder às seguintes perguntas de negócio:
1. Qual é a taxa de Churn atual da empresa?
2. Quais são as principais características dos clientes que cancelam?
3. Qual é o impacto financeiro (receita perdida) associado a esses cancelamentos?

---

## 🏗️ Arquitetura do Pipeline (Padrão Medallion)

O pipeline de dados é dividido em três camadas principais:

| Camada | Descrição e Ação | Ferramentas |
| :--- | :--- | :--- |
| **🥉 Bronze (Raw)** | Ingestão dos dados brutos exatamente como vieram da fonte (ex: arquivos CSV/JSON de sistemas transacionais). Mantém o histórico completo sem modificações. | PySpark, Databricks |
| **🥈 Silver (Cleaned)** | Limpeza de dados, tratamento de valores nulos (NaN), remoção de duplicatas, padronização de tipos de dados e formatação de datas. Os dados aqui estão filtrados e confiáveis. | PySpark (DataFrames) |
| **🥇 Gold (Aggregated)** | Dados modelados e agregados com foco em regras de negócio. Criação das métricas finais (KPIs) como `Total_Churn`, `MRR_Perdido` e segmentações de clientes. | PySpark SQL, Delta Lake |

---

## 🚀 Principais Insights e Resultados Gerados
*(Nota: Estes são exemplos baseados na estrutura do projeto. Ajuste os números de acordo com os dados gerados pelo seu script)*

*   **Identificação de Ofensores:** O pipeline permitiu identificar que clientes do plano "Básico" com menos de 3 meses de uso representam a maior fatia do Churn.
*   **Métricas de Receita:** A camada Gold gera uma tabela atualizada automaticamente com o MRR (Monthly Recurring Revenue) perdido por região.
*   **Automação:** O fluxo foi estruturado para rodar de forma contínua, permitindo que a equipe de retenção tome ações proativas.

---

## 🛠️ Tecnologias Utilizadas
*   **Plataforma Cloud:** Microsoft Azure (Databricks)
*   **Linguagem:** Python (PySpark)
*   **Armazenamento:** Delta Lake
*   **Conceitos Aplicados:** ETL/ELT, Lakehouse, Data Cleaning, Data Modeling.

---

## 📂 Estrutura dos Arquivos
*   `01_bronze_ingestion.py`: Script responsável pela ingestão de dados brutos para a camada Bronze.
*   *(Adicione aqui os scripts das camadas Silver e Gold assim que fizer o upload)*

---

## 👤 Autor
**Luiggy Alberto Rezende Collyer**
*   [LinkedIn](https://www.linkedin.com/in/luiggy-alberto-ab2331331 )
*   [Portfólio GitHub](https://github.com/LuiggyARC )

*Analista de Dados focado em transformar dados complexos em inteligência de negócios acionável.*
