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

# CELL ********************

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

source = ft.read_lakehouse(SILVER_LAKEHOUSE, PATH_TABLE_)

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

# CELL ********************

ft.write_lakehouse(
    source,
    lakehouse_name=SILVER_LAKEHOUSE,
    relative_path=PATH_PROCESSED_TABLE_,
    mode="overwrite",
 )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
