# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c46296fa-e5ae-4a72-bf4b-e745471968f8",
# META       "default_lakehouse_name": "LH_SILVER",
# META       "default_lakehouse_workspace_id": "74c649b9-b15e-41ec-8695-d13d59eef281",
# META       "known_lakehouses": [
# META         {
# META           "id": "c46296fa-e5ae-4a72-bf4b-e745471968f8"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "42851e6f-dd2a-472f-87e9-ba257a479276",
# META       "workspaceId": "e0935465-85f7-4785-934c-b3e333131f66"
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

# --- Paramètres (adapter à votre environnement) ---
SILVER_LAKEHOUSE = "LH_SILVER"
PATH_TABLE_ = "Cleaned_"

PATH_PROCESSED_TABLE_ = "Processed_"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dimensions = ft.generate_dimensions(
    lakehouse_name=SILVER_LAKEHOUSE,
    include_date=True,
    include_country=False,
    include_city=False,
    start_date="2024-01-01",              # Remplacez par votre date de début souhaitée
    end_date=str(dt.date.today()),           # Sera converti automatiquement à la date du jour (ex: "2026-04-21")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source = ft.read_lakehouse(SILVER_LAKEHOUSE, PATH_TABLE_)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target = ft.build_tcd(source, 
    rows="Region", 
    columns="Year", 
    values="Sales"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ft.write_lakehouse(
    target,
    lakehouse_name=SILVER_LAKEHOUSE,
    relative_path=PATH_PROCESSED_TABLE_,
    mode="overwrite",
 )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
