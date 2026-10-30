# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8d38ab7c-6182-42aa-964a-e7364d5d5f42",
# META       "default_lakehouse_name": "lh_aurum_bronze",
# META       "default_lakehouse_workspace_id": "59a20619-b762-4be2-a91a-b986decbb200",
# META       "known_lakehouses": [
# META         {
# META           "id": "8d38ab7c-6182-42aa-964a-e7364d5d5f42"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "31e98588-d262-4947-a3ff-d92b4651b7ab",
# META       "known_warehouses": [
# META         {
# META           "id": "31e98588-d262-4947-a3ff-d92b4651b7ab",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import os
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

base_path = f"/lakehouse/default/Files/raw/aurum/"
sources = [
    {"file": "orders.xlsx",  "sheet": "orders", "table": "bronze_orders"},
    {"file": "items.xlsx",   "sheet": "items",  "table": "bronze_items"},
    {"file": "users.xlsx",   "sheet": "Sheet1", "table": "bronze_users"},
    {"file": "targets.xlsx", "sheet": "Sheet1", "table": "bronze_targets"},
]

for s in sources:
    file, sheet, table = s["file"], s["sheet"], s["table"]
    path = os.path.join(base_path, file)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Not Found: {path}")

    pdf = pd.read_excel(path, sheet_name=sheet, dtype=str)
    
    pdf.columns = [c.strip().lower().replace(" ", "_") for c in pdf.columns]

    pdf = pdf.astype(object).where(pdf.notna(), None)

    schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    df = spark.createDataFrame(pdf, schema=schema)

    df = (df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(file)))

    (df.write.mode("overwrite")
       .format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(table))

    print(f'{table} - {df.count()} linhas.')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
