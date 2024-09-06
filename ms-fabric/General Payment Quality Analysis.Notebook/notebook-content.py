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
# META     },
# META     "environment": {
# META       "environmentId": "75a9bec2-4062-4e0c-a7c4-9ed6cb56cf6b",
# META       "workspaceId": "31d8944c-9e2f-4ae3-a4cb-9bca985e1a21"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Prerequisites

# MARKDOWN ********************

# ##  Dependencies

# CELL ********************

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import hash, col, count, size, split, lit, regexp_replace, lower, when, isnull, sum as _sum, array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, DecimalType

FILE__PATH = "Files/dataset/general_payments.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define Schemas

# CELL ********************

# Creating the schema based on the provided attributes
schema = StructType([

    # An indicator showing if the payment record is New, Added, Changed, or Unchanged in the current publication compared to the previous publication.
    StructField("Change_Type", StringType(), True),

    # An indicator showing if the recipient of the payment or other transfer of value is a physician covered recipient or a teaching hospital
    StructField("Covered_Recipient_Type", StringType(), True),

    # A unique identifying number (CMS Certification Number) of the Teaching Hospital receiving the payment or other transfer of value
    StructField("Teaching_Hospital_CCN", StringType(), True),

    # Open Payments system-generated unique identifier of the teaching hospital receiving the payment or other transfer of value
    StructField("Teaching_Hospital_ID", IntegerType(), True),

    # The name of the teaching hospital receiving the payment or other transfer of value – the name displayed is as listed in the CMS teaching hospital list
    StructField("Teaching_Hospital_Name", StringType(), True),

    # Open Payments system-generated unique identifier for physician profile receiving the payment or other transfer of value
    StructField("Physician_Profile_ID", LongType(), True),

    # First name of the physician (covered recipient) receiving the payment or other transfer of value, as reported by the submitting entity
    StructField("Physician_First_Name", StringType(), True),

    #   he middle name of the physician (covered recipient) receiving the payment or other transfer of value, as reported by the submitting entity
    StructField("Physician_Middle_Name", StringType(), True),

    # Last name of the physician (covered recipient) receiving the payment or other transfer of value, as reported by the submitting entity
    StructField("Physician_Last_Name", StringType(), True),

    # Name suffix of the physician (covered recipient) receiving the payment or other transfer of value, as reported by the submitting entity
    StructField("Physician_Name_Suffix", StringType(), True),

    # The first line of the primary practice/business street address of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value
    StructField("Recipient_Primary_Business_Street_Address_Line1", StringType(), True),

    # The second line of the primary practice/business street address of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value
    StructField("Recipient_Primary_Business_Street_Address_Line2", StringType(), True),

    # The primary practice/business city of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value
    StructField("Recipient_City", StringType(), True),

    # The primary practice/business state or territory abbreviation of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is in the United States
    StructField("Recipient_State", StringType(), True),

    # The 9-digit zip code for the primary practice/business location of the physician or teaching hospital (covered recipient) receiving the 
    StructField("Recipient_Zip_Code", StringType(), True),

    # The primary practice/business address country name of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value
    StructField("Recipient_Country", StringType(), True),

    #The primary practice/business province name of the physician (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is outside the United States, and if applicable
    StructField("Recipient_Province", StringType(), True),

    # The international postal code for the primary practice/business location of the physician (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is outside the United States
    StructField("Recipient_Postal_Code", StringType(), True),

    # The primary type of medicine practiced by the physician (covered recipient)
    StructField("Physician_Primary_Type", StringType(), True),

    # Physician's single-specialty chosen from the standardized "provider taxonomy" code list
    StructField("Physician_Specialty", StringType(), True),

    # The state license number of the covered recipient physician, which is a 2-letter state abbreviation; the record may include up to 5 physician license states if a physician is licensed in multiple states
    StructField("Physician_License_State_code1", StringType(), True),

    # The state license number of the covered recipient physician, which is a 2-letter state abbreviation; the record may include up to 5 physician license states if a physician is licensed in multiple states
    StructField("Physician_License_State_code2", StringType(), True),

    # The state license number of the covered recipient physician, which is a 2-letter state abbreviation; the record may include up to 5 physician license states if a physician is licensed in multiple states
    StructField("Physician_License_State_code3", StringType(), True),

    # The state license number of the covered recipient physician, which is a 2-letter state abbreviation; the record may include up to 5 physician license states if a physician is licensed in multiple states
    StructField("Physician_License_State_code4", StringType(), True),

    # The state license number of the covered recipient physician, which is a 2-letter state abbreviation; the record may include up to 5 physician license states if a physician is licensed in multiple states
    StructField("Physician_License_State_code5", StringType(), True),

    # The textual proper name of the submitting applicable manufacturer or applicable GPO
    StructField("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", StringType(), True),

    # Open Payments system-generated unique identifier of the applicable manufacturer or applicable GPO making payment or other transfer of value
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID", StringType(), True),

    # The textual proper name of the applicable manufacturer or applicable GPO making the payment or other transfer of value
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", StringType(), True),

    # State name of the applicable manufacturer or applicable GPO making the payment or other transfer of value
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", StringType(), True),

    # Country name of the applicable manufacturer or applicable GPO making the payment or other transfer of value
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country", StringType(), True),

    # 
    StructField("Total_Amount_of_Payment_USDollars", DecimalType(12, 2), True),
    StructField("Date_of_Payment", DateType(), True),
    StructField("Number_of_Payments_Included_in_Total_Amount", IntegerType(), True),
    StructField("Form_of_Payment_or_Transfer_of_Value", StringType(), True),
    StructField("Nature_of_Payment_or_Transfer_of_Value", StringType(), True),
    StructField("City_of_Travel", StringType(), True),
    StructField("State_of_Travel", StringType(), True),
    StructField("Country_of_Travel", StringType(), True),
    StructField("Physician_Ownership_Indicator", StringType(), True),
    StructField("Third_Party_Payment_Recipient_Indicator", StringType(), True),
    StructField("Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value", StringType(), True),
    StructField("Charity_Indicator", StringType(), True),
    StructField("Third_Party_Equals_Covered_Recipient_Indicator", StringType(), True),
    StructField("Contextual_Information", StringType(), True),
    StructField("Delay_in_Publication_Indicator", StringType(), True),
    StructField("Record_ID", IntegerType()),
    StructField("Dispute_Status_for_Publication", StringType(), True),
    StructField("Product_Indicator", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological1", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological2", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological3", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological4", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological5", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological1", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological2", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological3", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological4", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological5", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply1", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply2", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply3", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply4", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply5", StringType(), True),
    StructField("Program_Year", StringType(), True),
    StructField("Payment_Publication_Date", DateType(), True),
])

# Creating arrays for each classification and a final array combining them all, excluding the strikethrough values.

# Recipient Details
recipient_details = [
    "Recipient_Province",
    "Recipient_Postal_Code",
    "Recipient_State",
    "Recipient_Zip_Code",
    "Recipient_City",
    "Recipient_Country"
]

# Physician Details
physician_details = [
    "Physician_Name_Suffix",
    "Physician_Middle_Name",
    "Physician_First_Name",
    "Physician_Last_Name",
    "Physician_Primary_Type",
    "Physician_Specialty",
    "Physician_Profile_ID",
    "Physician_Ownership_Indicator"
]

# Drug/Biological/Device Details - Including generation of repeated fields
drug_bio_device_details = []
for i in range(1, 6):
    drug_bio_device_details.extend([
        f"Name_of_Associated_Covered_Drug_or_Biological{i}",
        f"NDC_of_Associated_Covered_Drug_or_Biological{i}",
        f"Name_of_Associated_Covered_Device_or_Medical_Supply{i}"
    ])

# Hospital Details
hospital_details = [
    "Teaching_Hospital_CCN",
    "Teaching_Hospital_ID",
    "Teaching_Hospital_Name"
]

# Payment and Transaction Details
payment_transaction_details = [
    "Total_Amount_of_Payment_USDollars",
    "Date_of_Payment",
    "Form_of_Payment_or_Transfer_of_Value",
    "Nature_of_Payment_or_Transfer_of_Value",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country",
    "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name"
]

# Travel Details
travel_details = [
    "State_of_Travel",
    "City_of_Travel",
    "Country_of_Travel"
]

# Miscellaneous Details
miscellaneous_details = [
    "Charity_Indicator",
    "Covered_Recipient_Type",
    "Record_ID",
    "Payment_Publication_Date",
    "Product_Indicator",
    "Dispute_Status_for_Publication"

]

# Combining all arrays into one
cols_of_interest = (
    recipient_details +
    physician_details +
    drug_bio_device_details +
    hospital_details +
    payment_transaction_details +
    travel_details +
    miscellaneous_details
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Functions

# CELL ********************

def get_schema_field_names(schema: StructType) -> List[str]:
    return [
        field.name for field in schema.fields
    ]

def check_null_values(df: DataFrame, only_include: List[str] = []):
    
    pass

def check_duplicates(
    df: DataFrame, 
    unique_identifier: str
) -> int:

    # Columns to be hashedb
    cols = [
        col(c) for c in df.columns 
        if c != unique_identifier
    ]

    # Add `hash` column that hash all other columns
    df_with_hash = df.withColumn(
        "hash",
        hash(*cols)
    )

    # Repartition the DataFrame based on the `hash` column
    df_repartitioned = df_with_hash.repartition("hash")

    # Define a window specification over the hash column
    window_spec = Window.partitionBy("hash").orderBy(unique_identifier)

    # Count duplicates based on the hash column
    df_dup_count = df_repartitioned.withColumn(
        "dup_count",
        count("*").over(window_spec)
    )

    # Filter to get only duplicate records
    duplicates = df_dup_count.filter(
        col("dup_count") > 1
    ).drop("hash", "dup_count")

    return duplicates.count()


def check_categorical_unique_values(
    df: DataFrame,
    only_include: List[str] = []
):
    # Assuming all string columns are categorical
    categorical_columns = [
        field.name for field in df.schema.fields 
        if field.dataType.simpleString() == 'string'
        and field.name in only_include
    ]

    unique_values = {}

    for column in categorical_columns:
        # Find unique values
        unique_values[column] = df.select(column).dropDuplicates().collect()

    # Print unique values for each categorical column
    for column, values in unique_values.items():
        print(f"Unique values in {column}: {[row[column] for row in values]}")


def compare_schema_with_df_headers(expected_schema: StructType, df: DataFrame):
    # Convert column names to sets for efficient comparison
    expected_columns = set(field.name for field in expected_schema.fields)
    actual_columns = set(df.columns)

    # Use set difference to find missing and extra columns
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns

    return missing_columns, extra_columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema Validation

# CELL ********************

df_only_headers = spark.read.option("header", "true").option("inferSchema", "false").csv(FILE__PATH).limit(0)

missing_columns, extra_columns = compare_schema_with_df_headers(schema, df_only_headers)
print("Missing Columns: ", missing_columns)
print("Extra Columns: ", extra_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

combined_cols = physician_details + hospital_details
df_test = spark.read.format("csv").option("header","true").load(FILE__PATH).select(*combined_cols)
df_test.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_test.select("Physician_Profile_ID").dropDuplicates().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_test.select("Teaching_Hospital_ID").dropDuplicates().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_test.select("Teaching_Hospital_CCN").dropDuplicates().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Load `general_payments.csv` dataset

# CELL ********************

df = spark.read.format("csv").option("header","true").schema(schema).load(FILE__PATH).select(cols_of_interest)
# df now is a Spark DataFrame containing CSV data from "Files/dataset/general_payments.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Quality Checks

# MARKDOWN ********************

# ## Total Records and Columns

# CELL ********************

total_records = df.select("Record_ID").count()
print("Total Records", total_records)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Numerical Columns Statistics

# CELL ********************

df.select("Total_Amount_of_Payment_USDollars").summary().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.filter(
    col("Total_Amount_of_Payment_USDollars") < 1
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_check = [
    "Name_of_Associated_Covered_Drug_or_Biological1",
    "Name_of_Associated_Covered_Device_or_Medical_Supply1",
    "Recipient_Primary_Business_Street_Address_Line1"
]

missing_percentage_checks = [
    (count(when(isnull(c), c)) / lit(total_records) * 100).alias(c + "_missing_percentage") 
    for c in cols_of_interest
]
df_missing_summary = df.agg(*missing_percentage_checks)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_missing_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select("Recipient_Province", "Recipient_State").filter(
    (col("Recipient_Province").isNotNull()) & col("Recipient_State").isNotNull()
).show()

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
