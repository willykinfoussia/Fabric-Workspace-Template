# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "867e0bad-c5ea-44a4-81d6-6829e2486a52",
# META       "default_lakehouse_name": "LH_BRONZE",
# META       "default_lakehouse_workspace_id": "74c649b9-b15e-41ec-8695-d13d59eef281",
# META       "known_lakehouses": [
# META         {
# META           "id": "867e0bad-c5ea-44a4-81d6-6829e2486a52"
# META         },
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

# ─────────────────────────────────────────────────────────────
# CONFIGURATION — Modifier ces paramètres selon votre environnement
# ─────────────────────────────────────────────────────────────
MODE_TEST = True

# Noms des Lakehouses dans Microsoft Fabric
BRONZE_LAKEHOUSE = "LH_BRONZE"   # Nom du Lakehouse source
SILVER_LAKEHOUSE = "LH_SILVER"   # Nom du Lakehouse destination

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Exécution du pipeline Bronze → Silver
# La commande suivante :
# 1. lit toutes les tables Bronze dans le schéma `dbo`,
# 2. applique le nettoyage standard (`clean_data`),
# 3. écrit les résultats dans le Lakehouse Silver.
# 
# Le paramètre `continue_on_error=True` permet de poursuivre même si certaines tables échouent.

# CELL ********************

result = ft.clean_and_write_all_tables(
    source_lakehouse_name=BRONZE_LAKEHOUSE,
    target_lakehouse_name=SILVER_LAKEHOUSE,
    include_schemas=["dbo"],
    continue_on_error=True,
)
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
