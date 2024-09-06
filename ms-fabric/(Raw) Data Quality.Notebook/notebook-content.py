# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bbd4eaab-2f02-4dee-bab9-5352dcd559b7",
# META       "default_lakehouse_name": "Raw",
# META       "default_lakehouse_workspace_id": "31d8944c-9e2f-4ae3-a4cb-9bca985e1a21"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Load Data

# CELL ********************

general_payments = spark.read.format("csv").option("header","true").load("Files/dataset/general_payments.csv")
ownership = spark.read.format("csv").option("header","true").load("Files/dataset/ownership.csv")
research_payments = spark.read.format("csv").option("header","true").load("Files/dataset/research_payments.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Helpers

# CELL ********************

def 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Quality Check
# 
# • Does the data contain any null or blank values?
# 
# • Are there any anomalies in the data? Do they have a distinct pattern?
# 
# • Does it contain any duplicate values? What is the ratio of unique values?
# 
# • What is the range of values in the source data? Are the minimum and maximum values within your expected range?

# MARKDOWN ********************

# ## Describe

# CELL ********************

general_payments.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deleted_payments.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ownership.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

research_payments.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Null or Missing Values

# CELL ********************

analyze_null_values(general_payments)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

physician_license_state_code_cols = [
    "Physician_License_State_code1",
    "Physician_License_State_code2",
    "Physician_License_State_code3",
    "Physician_License_State_code4",
    "Physician_License_State_code5"
]

# Construct a condition where all specified columns are null
null_condition = col(physician_license_state_code_cols[0]).isNull()
for col_name in physician_license_state_code_cols[1:]:
    null_condition = null_condition & col(col_name).isNull()

# Filter the DataFrame based on this condition and count the results
general_payments_all_license_state_null = general_payments.filter(null_condition)
general_payments_all_license_state_null.groupBy("Covered_Recipient_Type").count().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    general_payments_all_license_state_null.filter(
    general_payments.Covered_Recipient_Type == "Covered Recipient Physician"
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    general_payments_all_license_state_null.where("Covered_Recipient_Type == 'Teaching Hospital'")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

analyze_null_values(deleted_payments)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

analyze_null_values(research_payments)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

analyze_null_values(ownership)

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
