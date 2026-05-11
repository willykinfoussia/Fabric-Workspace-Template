# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2b78a9e8-9637-4414-ae85-0c8814a4f3c9",
# META       "default_lakehouse_name": "LH_GOLD",
# META       "default_lakehouse_workspace_id": "74c649b9-b15e-41ec-8695-d13d59eef281",
# META       "known_lakehouses": [
# META         {
# META           "id": "2b78a9e8-9637-4414-ae85-0c8814a4f3c9"
# META         },
# META         {
# META           "id": "c46296fa-e5ae-4a72-bf4b-e745471968f8"
# META         },
# META         {
# META           "id": "867e0bad-c5ea-44a4-81d6-6829e2486a52"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import fabrictools as ft
print(ft.__version__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ─────────────────────────────────────────────────────────────
# CONFIGURATION — Modifier ces paramètres selon votre environnement
# ─────────────────────────────────────────────────────────────
MODE_TEST = True

# Noms des Lakehouses dans Microsoft Fabric
BRONZE_LAKEHOUSE = "LH_BRONZE"
SILVER_LAKEHOUSE = "LH_SILVER"
GOLD_LAKEHOUSE = "LH_GOLD"

PATH_TABLE_ = "Cleaned_"

PATH_PROCESSED_TABLE_ = "Processed_"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = ft.clean_and_write_all_tables(
    source_lakehouse_name=BRONZE_LAKEHOUSE,
    target_lakehouse_name=SILVER_LAKEHOUSE,
    include_schemas=["dbo"],
    continue_on_error=True,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

read_summary = ft.read_lakehouses(
    [
        {
            "name": "source",
            "lakehouse_name": SILVER_LAKEHOUSE,
            "relative_path": PATH_TABLE_,
        }
    ]
)
source = read_summary["source"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = ft.write_lakehouses(
    [
        {
            "name": "target",
            "df": target,
            "lakehouse_name": SILVER_LAKEHOUSE,
            "relative_path": PATH_PROCESSED_TABLE_,
            "mode": "overwrite",
        }
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = ft.make_business_ready(
    source_lakehouse_name=SILVER_LAKEHOUSE,
    target_lakehouse_name=GOLD_LAKEHOUSE,
    tables=["Processed_", "Cleaned_", "Dimension_Date"]
)
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
