# Databricks notebook source
# MAGIC %md
# MAGIC # Análise de Churn e Receita SaaS com Azure Databricks
# MAGIC
# MAGIC ## Objetivo
# MAGIC Este projeto tem como objetivo construir um pipeline analítico utilizando Azure Databricks para analisar churn e receita em uma empresa SaaS fictícia.
# MAGIC
# MAGIC A arquitetura segue o padrão **Lakehouse**, utilizando três camadas principais:
# MAGIC
# MAGIC - Bronze: ingestão de dados brutos
# MAGIC - Silver: limpeza e transformação
# MAGIC - Gold: geração de métricas analíticas
# MAGIC
# MAGIC O projeto demonstra habilidades em engenharia de dados e análise de dados utilizando Spark e Delta Lake.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset
# MAGIC
# MAGIC O dataset utilizado é sintético e foi criado para simular o funcionamento de uma empresa SaaS.
# MAGIC
# MAGIC Ele contém três tabelas principais:
# MAGIC
# MAGIC ### customers
# MAGIC Informações cadastrais dos clientes.
# MAGIC
# MAGIC - customer_id
# MAGIC - signup_date
# MAGIC - region
# MAGIC - age
# MAGIC - gender
# MAGIC
# MAGIC ### subscriptions
# MAGIC Informações da assinatura do cliente.
# MAGIC
# MAGIC - subscription_id
# MAGIC - customer_id
# MAGIC - plan
# MAGIC - monthly_price
# MAGIC - payment_method
# MAGIC - autopay
# MAGIC - start_date
# MAGIC - end_date
# MAGIC - status
# MAGIC
# MAGIC ### transactions
# MAGIC Histórico de transações dos clientes.
# MAGIC
# MAGIC - transaction_id
# MAGIC - customer_id
# MAGIC - transaction_date
# MAGIC - amount
# MAGIC - category

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer — Ingestão de Dados
# MAGIC
# MAGIC Nesta etapa realizamos a ingestão dos dados brutos armazenados em arquivos CSV.
# MAGIC
# MAGIC Os dados são carregados como DataFrames Spark e posteriormente armazenados como **Delta Tables**, garantindo maior performance e confiabilidade.

# COMMAND ----------

customers = spark.read.csv(
"/Workspace/Users/luiggyalberto@gmail.com/customers.csv",
header=True,
inferSchema=True
)

subscriptions = spark.read.csv(
"/Workspace/Users/luiggyalberto@gmail.com/subscriptions.csv",
header=True,
inferSchema=True
)

transactions = spark.read.csv(
"/Workspace/Users/luiggyalberto@gmail.com/transactions.csv",
header=True,
inferSchema=True
)
display(customers)
display(subscriptions)
display(transactions)
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------


customers.write.format("delta").mode("overwrite").saveAsTable("bronze_customers")

subscriptions.write.format("delta").mode("overwrite").saveAsTable("bronze_subscriptions")

transactions.write.format("delta").mode("overwrite").saveAsTable("bronze_transactions")
display(spark.table("bronze_customers"))
display(spark.table("bronze_subscriptions"))
display(spark.table("bronze_transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer — Limpeza e Transformação
# MAGIC
# MAGIC Nesta etapa realizamos a preparação dos dados para análise.
# MAGIC
# MAGIC Principais transformações aplicadas:
# MAGIC
# MAGIC - Conversão de colunas de data
# MAGIC - Padronização de tipos
# MAGIC - Criação da coluna `subscription_days`, que representa o tempo de assinatura do cliente
# MAGIC
# MAGIC Essas transformações tornam os dados adequados para análise e criação de métricas de negócio.

# COMMAND ----------

from pyspark.sql.functions import col, to_date, datediff, current_date, when

silver_customers = spark.table("bronze_customers") \
    .withColumn("signup_date", to_date(col("signup_date")))

silver_subscriptions = spark.table("bronze_subscriptions") \
    .withColumn("start_date", to_date(col("start_date"))) \
    .withColumn("end_date", to_date(col("end_date"))) \
    .withColumn(
        "subscription_days",
        when(col("status") == "churned", datediff(col("end_date"), col("start_date")))
        .otherwise(datediff(current_date(), col("start_date")))
    )

silver_transactions = spark.table("bronze_transactions") \
    .withColumn("transaction_date", to_date(col("transaction_date")))

# COMMAND ----------

silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")
silver_subscriptions.write.format("delta").mode("overwrite").saveAsTable("silver_subscriptions")
silver_transactions.write.format("delta").mode("overwrite").saveAsTable("silver_transactions")

# COMMAND ----------

display(spark.table("silver_customers"))
display(spark.table("silver_subscriptions"))
display(spark.table("silver_transactions"))

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg, count, round

gold_base = spark.table("silver_subscriptions") \
    .join(spark.table("silver_customers"), "customer_id", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer — Métricas de Negócio
# MAGIC
# MAGIC A camada Gold contém tabelas analíticas prontas para consumo por dashboards ou relatórios.
# MAGIC
# MAGIC Nesta etapa calculamos métricas importantes para empresas SaaS, incluindo:
# MAGIC
# MAGIC - Churn Rate
# MAGIC - Receita por plano
# MAGIC - ARPU (Average Revenue per User)
# MAGIC - Receita por região
# MAGIC
# MAGIC Essas métricas ajudam a entender o comportamento dos clientes e identificar oportunidades de crescimento.

# COMMAND ----------

gold_churn_plan = gold_base.groupBy("plan").agg(
    count("*").alias("total_customers"),
    _sum(when(col("status") == "churned", 1).otherwise(0)).alias("churned_customers")
).withColumn(
    "churn_rate",
    round(col("churned_customers") / col("total_customers"), 4)
)

# COMMAND ----------

display(gold_churn_plan)

# COMMAND ----------

gold_revenue_plan = gold_base.groupBy("plan").agg(
    round(_sum("monthly_price"), 2).alias("total_revenue"),
    count("*").alias("total_customers")
)

# COMMAND ----------

display(gold_revenue_plan)

# COMMAND ----------

gold_arpu_plan = gold_base.groupBy("plan").agg(
    round(avg("monthly_price"), 2).alias("arpu")
)

# COMMAND ----------

display(gold_arpu_plan)

# COMMAND ----------

gold_revenue_region = gold_base.groupBy("region").agg(
    round(_sum("monthly_price"), 2).alias("total_revenue"),
    count("*").alias("total_customers")
)

# COMMAND ----------

display(gold_revenue_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insights
# MAGIC
# MAGIC A análise permite identificar padrões importantes no comportamento dos clientes.
# MAGIC
# MAGIC ### Churn por plano
# MAGIC Planos de entrada tendem a apresentar maior churn, indicando menor retenção de clientes.
# MAGIC
# MAGIC ### Receita por plano
# MAGIC Planos premium contribuem significativamente para a receita total.
# MAGIC
# MAGIC ### Receita por região
# MAGIC A análise regional permite identificar mercados com maior potencial de crescimento.
# MAGIC
# MAGIC ### Importância da retenção
# MAGIC Clientes com maior tempo de assinatura geram maior valor acumulado para a empresa.

# COMMAND ----------

gold_base.write.format("delta").mode("overwrite").saveAsTable("gold_base")
gold_churn_plan.write.format("delta").mode("overwrite").saveAsTable("gold_churn_plan")
gold_revenue_plan.write.format("delta").mode("overwrite").saveAsTable("gold_revenue_plan")
gold_arpu_plan.write.format("delta").mode("overwrite").saveAsTable("gold_arpu_plan")
gold_revenue_region.write.format("delta").mode("overwrite").saveAsTable("gold_revenue_region")