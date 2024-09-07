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

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, DecimalType
general_payment_schema = StructType([
    StructField("Change_Type", StringType(), True),
    StructField("Covered_Recipient_Type", StringType(), True),
    StructField("Teaching_Hospital_CCN", StringType(), True),
    StructField("Teaching_Hospital_ID", IntegerType(), True),
    StructField("Teaching_Hospital_Name", StringType(), True),
    StructField("Physician_Profile_ID", LongType(), True),
    StructField("Physician_First_Name", StringType(), True),
    StructField("Physician_Middle_Name", StringType(), True),
    StructField("Physician_Last_Name", StringType(), True),
    StructField("Physician_Name_Suffix", StringType(), True),
    StructField("Recipient_Primary_Business_Street_Address_Line1", StringType(), True),
    StructField("Recipient_Primary_Business_Street_Address_Line2", StringType(), True),
    StructField("Recipient_City", StringType(), True),
    StructField("Recipient_State", StringType(), True),
    StructField("Recipient_Zip_Code", StringType(), True),
    StructField("Recipient_Country", StringType(), True),
    StructField("Recipient_Province", StringType(), True),
    StructField("Recipient_Postal_Code", StringType(), True),
    StructField("Physician_Primary_Type", StringType(), True),
    StructField("Physician_Specialty", StringType(), True),
    StructField("Physician_License_State_code1", StringType(), True),
    StructField("Physician_License_State_code2", StringType(), True),
    StructField("Physician_License_State_code3", StringType(), True),
    StructField("Physician_License_State_code4", StringType(), True),
    StructField("Physician_License_State_code5", StringType(), True),
    StructField("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country", StringType(), True),
    StructField("Total_Amount_of_Payment_USDollars", DecimalType(12, 2), True),
    StructField("Date_of_Payment", StringType(), True),
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
    StructField("Payment_Publication_Date", StringType(), True),
])

research_payment_schema = StructType([
    StructField("Change_Type", StringType(), True),
    StructField("Covered_Recipient_Type", StringType(), True),
    StructField("Noncovered_Recipient_Entity_Name", StringType(), True),
    StructField("Teaching_Hospital_CCN", StringType(), True),
    StructField("Teaching_Hospital_ID", LongType(), True),
    StructField("Teaching_Hospital_Name", StringType(), True),
    StructField("Physician_Profile_ID", LongType(), True),
    StructField("Physician_First_Name", StringType(), True),
    StructField("Physician_Middle_Name", StringType(), True),
    StructField("Physician_Last_Name", StringType(), True),
    StructField("Physician_Name_Suffix", StringType(), True),
    StructField("Recipient_Primary_Business_Street_Address_Line1", StringType(), True),
    StructField("Recipient_Primary_Business_Street_Address_Line2", StringType(), True),
    StructField("Recipient_City", StringType(), True),
    StructField("Recipient_State", StringType(), True),
    StructField("Recipient_Zip_Code", StringType(), True),
    StructField("Recipient_Country", StringType(), True),
    StructField("Recipient_Province", StringType(), True),
    StructField("Recipient_Postal_Code", StringType(), True),
    StructField("Physician_Primary_Type", StringType(), True),
    StructField("Physician_Specialty", StringType(), True),
    StructField("Physician_License_State_code1", StringType(), True),
    StructField("Physician_License_State_code2", StringType(), True),
    StructField("Physician_License_State_code3", StringType(), True),
    StructField("Physician_License_State_code4", StringType(), True),
    StructField("Physician_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_1_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_1_First_Name", StringType(), True),
    StructField("Principal_Investigator_1_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_1_Last_Name", StringType(), True),
    StructField("Principal_Investigator_1_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_1_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_1_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_1_City", StringType(), True),
    StructField("Principal_Investigator_1_State", StringType(), True),
    StructField("Principal_Investigator_1_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_1_Country", StringType(), True),
    StructField("Principal_Investigator_1_Province", StringType(), True),
    StructField("Principal_Investigator_1_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_1_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_1_Specialty", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_2_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_2_First_Name", StringType(), True),
    StructField("Principal_Investigator_2_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_2_Last_Name", StringType(), True),
    StructField("Principal_Investigator_2_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_2_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_2_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_2_City", StringType(), True),
    StructField("Principal_Investigator_2_State", StringType(), True),
    StructField("Principal_Investigator_2_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_2_Country", StringType(), True),
    StructField("Principal_Investigator_2_Province", StringType(), True),
    StructField("Principal_Investigator_2_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_2_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_2_Specialty", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_3_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_3_First_Name", StringType(), True),
    StructField("Principal_Investigator_3_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_3_Last_Name", StringType(), True),
    StructField("Principal_Investigator_3_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_3_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_3_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_3_City", StringType(), True),
    StructField("Principal_Investigator_3_State", StringType(), True),
    StructField("Principal_Investigator_3_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_3_Country", StringType(), True),
    StructField("Principal_Investigator_3_Province", StringType(), True),
    StructField("Principal_Investigator_3_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_3_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_3_Specialty", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_4_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_4_First_Name", StringType(), True),
    StructField("Principal_Investigator_4_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_4_Last_Name", StringType(), True),
    StructField("Principal_Investigator_4_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_4_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_4_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_4_City", StringType(), True),
    StructField("Principal_Investigator_4_State", StringType(), True),
    StructField("Principal_Investigator_4_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_4_Country", StringType(), True),
    StructField("Principal_Investigator_4_Province", StringType(), True),
    StructField("Principal_Investigator_4_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_4_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_4_Specialty", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_5_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_5_First_Name", StringType(), True),
    StructField("Principal_Investigator_5_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_5_Last_Name", StringType(), True),
    StructField("Principal_Investigator_5_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_5_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_5_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_5_City", StringType(), True),
    StructField("Principal_Investigator_5_State", StringType(), True),
    StructField("Principal_Investigator_5_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_5_Country", StringType(), True),
    StructField("Principal_Investigator_5_Province", StringType(), True),
    StructField("Principal_Investigator_5_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_5_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_5_Specialty", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code5", StringType(), True),
    StructField("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID", LongType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country", StringType(), True),
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
    StructField("Total_Amount_of_Payment_USDollars", DecimalType(12, 2), True),
    StructField("Date_of_Payment", StringType(), True),
    StructField("Form_of_Payment_or_Transfer_of_Value", StringType(), True),
    StructField("Expenditure_Category1", StringType(), True),
    StructField("Expenditure_Category2", StringType(), True),
    StructField("Expenditure_Category3", StringType(), True),
    StructField("Expenditure_Category4", StringType(), True),
    StructField("Expenditure_Category5", StringType(), True),
    StructField("Expenditure_Category6", StringType(), True),
    StructField("Preclinical_Research_Indicator", StringType(), True),
    StructField("Delay_in_Publication_Indicator", StringType(), True),
    StructField("Name_of_Study", StringType(), True),
    StructField("Dispute_Status_for_Publication", StringType(), True),
    StructField("Record_ID", LongType(), True),
    StructField("Program_Year", StringType(), True),
    StructField("Payment_Publication_Date", StringType(), True),
    StructField("ClinicalTrials_Gov_Identifier", StringType(), True),
    StructField("Research_Information_Link", StringType(), True),
    StructField("Context_of_Research", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_general = spark.read.format("csv").option("header","true").schema(general_payment_schema).load("Files/dataset/general_payments.csv")
# df now is a Spark DataFrame containing CSV data from "Files/dataset/general_payments.csv".
display(df_general)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_research = spark.read.format("csv").option("header","true").schema(research_payment_schema).load("Files/dataset/research_payments.csv")
# df now is a Spark DataFrame containing CSV data from "Files/dataset/research_payments.csv".
display(df_research)

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
