# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0e964b95-22ed-4a08-aea1-56b80c25557a",
# META       "default_lakehouse_name": "lh_aurum_silver",
# META       "default_lakehouse_workspace_id": "59a20619-b762-4be2-a91a-b986decbb200",
# META       "known_lakehouses": [
# META         {
# META           "id": "8d38ab7c-6182-42aa-964a-e7364d5d5f42"
# META         },
# META         {
# META           "id": "0e964b95-22ed-4a08-aea1-56b80c25557a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql import functions as F

BRONZE = "lh_aurum_bronze"
bronze_orders  = spark.read.table(f"{BRONZE}.bronze_orders")
bronze_items   = spark.read.table(f"{BRONZE}.bronze_items")
bronze_users   = spark.read.table(f"{BRONZE}.bronze_users")
bronze_targets = spark.read.table(f"{BRONZE}.bronze_targets")

def save(df, name):
    (df.write
       .mode("overwrite")
       .format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(name))
    print(f"{name}: {spark.read.table(name).count()} linhas")

print("bronze_orders:", bronze_orders.count(), "linhas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(bronze_users.select("user_id", "category", "city"))
display(bronze_targets.select("user_id", "category", "city", "monthly_revenue_target"))

display(bronze_items.groupBy("category").count())

display(
    bronze_items.groupBy("item_id")
    .agg(F.countDistinct("category").alias("n_categorias"))
    .where("n_categorias > 1")
)

cols = [c for c in bronze_orders.columns if not c.startswith("_")]
print("Linhas:", bronze_orders.count(),
      "| distintas:", bronze_orders.select(cols).distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_lines = (
    bronze_orders
    .select(
        F.col("order_id").cast("long").alias("order_id"),
        F.to_date("order_date").alias("order_date"),
        F.col("user_id").cast("long").alias("user_id"),
        F.col("product_id").cast("long").alias("item_id"),       # renomeia para casar com o catálogo
        F.col("revenue").cast("decimal(18,2)").alias("revenue"), # dinheiro = decimal, nunca float
    )
    .dropDuplicates()                                            # remove as 9 linhas repetidas
)

save(order_lines, "silver_order_lines")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Grão: 1 linha por PEDIDO. A receita entra uma única vez.
orders = (
    order_lines
    .groupBy("order_id")
    .agg(
        F.max("order_date").alias("order_date"),
        F.max("user_id").alias("user_id"),
        F.max("revenue").alias("revenue"),            # o valor é o mesmo em todas as linhas do pedido
        F.countDistinct("item_id").alias("item_count"),
    )
)
save(orders, "silver_orders")

# Grão: 1 linha por pedido x produto, SEM receita (de propósito)
order_items = order_lines.select("order_id", "item_id").distinct()
save(order_items, "silver_order_items")

# Prova da armadilha resolvida
print("Receita ingênua :", order_lines.agg(F.sum("revenue")).first()[0])
print("Receita correta :", orders.agg(F.sum("revenue")).first()[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

items = (
    bronze_items
    .select(
        F.col("item_id").cast("long").alias("item_id"),
        F.lower(F.trim("category")).alias("category"),   # sem espaços, tudo minúsculo
    )
    .where(F.col("category").isNotNull())               # remove produtos sem categoria
    .dropDuplicates(["item_id"])                        # 1 linha por produto
)

save(items, "silver_items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1) Junta a cidade das DUAS planilhas, lado a lado.
#    targets é a fonte mais confiável: nas 3 divergências entre users e targets,
#    targets estava certo. users entra só como reserva (se targets vier vazio).
customers_raw = (
    bronze_users
    .join(
        bronze_targets.select("user_id", F.col("city").alias("city_targets")),
        on="user_id",
        how="left",
    )
    .select(
        F.col("user_id").cast("long").alias("user_id"),
        F.lower(F.trim("category")).alias("segment"),   # segmento do CLIENTE (bar, restaurant, shop)
        F.col("city").alias("city_users"),
        F.col("city_targets"),
        F.coalesce(F.col("city_targets"), F.col("city")).alias("city_source"),
    )
)

# Mostra as duas versões para você ver as divergências (bom print para o README)
display(customers_raw)

# 2) Normaliza o texto: siglas e grafias quebradas viram o nome canônico.
#    contains("paulo") também pega o texto corrompido ("SÃ£o Paulo", "S�o Paulo").
c = F.lower(F.trim(F.col("city_source")))

city_clean = (
    F.when((c == "sp") | c.contains("paulo"), "São Paulo")
     .when((c == "rj") | c.contains("janeiro"), "Rio de Janeiro")
     .when(c.contains("campinas"), "Campinas")
     .otherwise(F.initcap(F.trim(F.col("city_source"))))
)

customers = (
    customers_raw
    .withColumn("city", city_clean)
    .withColumn(
        "state",
        F.when(F.col("city").isin("São Paulo", "Campinas"), "SP")
         .when(F.col("city") == "Rio de Janeiro", "RJ")
    )
    .select("user_id", "segment", "city", "state")
)

display(customers.orderBy("user_id"))
save(customers, "silver_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

targets = bronze_targets.select(
    F.col("user_id").cast("long").alias("user_id"),
    F.col("monthly_revenue_target").cast("decimal(18,2)").alias("monthly_target"),
)
# cidade e categoria saem daqui: já vivem em silver_customers

save(targets, "silver_targets")
print("Soma das metas mensais:", targets.agg(F.sum("monthly_target")).first()[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Primeiro e último pedido definem o período do calendário
bounds = orders.agg(
    F.min("order_date").alias("first_order"),
    F.max("order_date").alias("last_order"),
)

calendar = (
    bounds
    # 1 linha por dia, do 1º dia do primeiro mês ao último dia do último mês
    .select(
        F.explode(F.sequence(
            F.trunc("first_order", "month"),
            F.last_day("last_order"),
            F.expr("interval 1 day"),
        )).alias("date"),
        "last_order",
    )
    .select(
        (F.year("date") * 10000 + F.month("date") * 100 + F.dayofmonth("date")).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month_num"),
        F.date_format("date", "MMM").alias("month_name"),        # Jan, Feb...
        F.trunc("date", "month").alias("month_start"),
        F.date_format("date", "EEEE").alias("day_of_week"),      # Monday...
        (((F.dayofweek("date") + 5) % 7) + 1).alias("weekday_num"),  # 1 = segunda
        # mês parcial = o último pedido aconteceu antes do fim do mês
        (F.last_day("date") > F.col("last_order")).alias("is_partial_month"),
    )
)

save(calendar, "silver_calendar")
display(calendar.where("is_partial_month").select("month_name").distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

def check(rule, failed_rows, severity="error"):
    """Registra uma regra. error = interrompe o pipeline; warning = só avisa."""
    if failed_rows == 0:
        status = "OK"
    elif severity == "error":
        status = "FALHOU"
    else:
        status = "AVISO"
    results.append((rule, severity, status, int(failed_rows)))
    print(f"[{status}] {rule}: {failed_rows}")

def bad_keys(table, keys):
    """Conta linhas com chave nula + chaves repetidas."""
    df = spark.read.table(table)
    nulls = df.where(" OR ".join(f"{k} IS NULL" for k in keys)).count()
    dups = df.groupBy(*keys).count().where("count > 1").count()
    return nulls + dups

t = spark.read.table

# 1) Cada tabela tem chave única e sem nulos no grão declarado
check("silver_orders: chave order_id",           bad_keys("silver_orders", ["order_id"]))
check("silver_order_items: chave pedido+produto", bad_keys("silver_order_items", ["order_id", "item_id"]))
check("silver_items: chave item_id",             bad_keys("silver_items", ["item_id"]))
check("silver_customers: chave user_id",         bad_keys("silver_customers", ["user_id"]))
check("silver_targets: chave user_id",           bad_keys("silver_targets", ["user_id"]))
check("silver_calendar: chave date_key",         bad_keys("silver_calendar", ["date_key"]))

# 2) Regras de negócio
check("Pedido com mais de um valor de receita",
      t("silver_order_lines").groupBy("order_id")
      .agg(F.countDistinct("revenue").alias("n")).where("n > 1").count())
check("Pedido sem data", t("silver_orders").where("order_date IS NULL").count())
check("Pedido sem cliente cadastrado",
      t("silver_orders").join(t("silver_customers"), "user_id", "left_anti").count())
check("Cliente sem cidade", t("silver_customers").where("city IS NULL").count())
check("Produto sem categoria", t("silver_items").where("category IS NULL").count())

# 3) Avisos: não param o pipeline, mas merecem atenção
check("Item de pedido sem produto no catálogo",
      t("silver_order_items").join(t("silver_items"), "item_id", "left_anti").count(), "warning")
check("Cliente sem meta",
      t("silver_customers").join(t("silver_targets"), "user_id", "left_anti").count(), "warning")
check("Cidade fora da lista conhecida (nova grafia?)",
      t("silver_customers").where("city NOT IN ('São Paulo', 'Rio de Janeiro', 'Campinas')").count(), "warning")

# Guarda o histórico das checagens (append = acumula a cada execução)
dq = (spark.createDataFrame(results, "rule string, severity string, status string, failed_rows int")
      .withColumn("run_at", F.lit(datetime.now())))
dq.write.mode("append").format("delta").saveAsTable("dq_results")

# Interrompe se alguma regra obrigatória falhou -> o pipeline não leva dado ruim para a Gold
failed = [r[0] for r in results if r[2] == "FALHOU"]
if failed:
    raise Exception(f"Regras de qualidade falharam: {failed}")
print("Todas as regras obrigatórias passaram.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
