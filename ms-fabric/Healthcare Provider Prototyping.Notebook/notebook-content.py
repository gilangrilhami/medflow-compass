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

cols_of_interest = [
    "Physician_Primary_Type",
    "Physician_Specialty",
    "Total_Amount_of_Payment_USDollars",
    "Date_of_Payment",
    "Recipient_City",
    "Recipient_State",
    "Recipient_Country",
    "Record_ID"
]

cols_of_interest_research_specific = [
  "Principal_Investigator_1_Primary_Type",
  "Principal_Investigator_1_Specialty",
  "Principal_Investigator_2_Primary_Type",
  "Principal_Investigator_2_Specialty",
  "Principal_Investigator_3_Primary_Type",
  "Principal_Investigator_3_Specialty",
  "Principal_Investigator_4_Primary_Type",
  "Principal_Investigator_4_Specialty",
  "Principal_Investigator_5_Primary_Type",
  "Principal_Investigator_5_Specialty",
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_general = spark.read.format("csv").option("header","true").schema(general_payment_schema).load("Files/dataset/general_payments.csv").select(
    cols_of_interest
)
df_research = spark.read.format("csv").option("header","true").schema(research_payment_schema).load("Files/dataset/research_payments.csv").select(
    cols_of_interest + cols_of_interest_research_specific
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_general)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_research)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_struct_investigators = df_research.withColumn(
    "investigators", 
    F.array(
        F.struct(
            F.col("Principal_Investigator_1_Primary_Type").alias("Primary_Type"), 
            F.col("Principal_Investigator_1_Specialty").alias("Specialty")
        ),
        F.struct(
            F.col("Principal_Investigator_2_Primary_Type").alias("Primary_Type"), 
            F.col("Principal_Investigator_2_Specialty").alias("Specialty")
        ),
        F.struct(
            F.col("Principal_Investigator_3_Primary_Type").alias("Primary_Type"), 
            F.col("Principal_Investigator_3_Specialty").alias("Specialty")
        ),
        F.struct(
            F.col("Principal_Investigator_4_Primary_Type").alias("Primary_Type"), 
            F.col("Principal_Investigator_4_Specialty").alias("Specialty")
        ),
        F.struct(
            F.col("Principal_Investigator_5_Primary_Type").alias("Primary_Type"), 
            F.col("Principal_Investigator_5_Specialty").alias("Specialty")
        )
    )
)

df_exploded_investigators = df_struct_investigators.withColumn(
    "exploded_investigator", 
    F.explode("investigators")
)

df_type_and_sepciality = df_exploded_investigators.withColumn(
    "Primary_Type", 
    df_exploded_investigators["exploded_investigator"].getItem("Primary_Type")
).withColumn(
    "Specialty", 
    df_exploded_investigators["exploded_investigator"].getItem("Specialty")
)

df_filtered = df_type_and_sepciality.filter(
    F.col("Primary_Type").isNotNull() & F.col("Specialty").isNotNull()
).withColumn("num_investigators", 
    F.size(
        F.filter(
            F.col("investigators"), 
            lambda x: x["Primary_Type"].isNotNull()
        )
    )
).withColumn(
    "Total_Amount_of_Payment_USDollars", 
    F.col("Total_Amount_of_Payment_USDollars") / F.col("num_investigators")
)

df_final = df_filtered.withColumn(
    "Physician_Primary_Type",
    F.coalesce(
        F.col("Physician_Primary_Type"), 
        F.col("Primary_Type")
    )
).withColumn(
    "Physician_Specialty", 
    F.coalesce(
        F.col("Physician_Specialty"), 
        F.col("Specialty")
    )
).select(
    cols_of_interest
)

display(
    df_final
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined = df_general.union(df_final)
display(df_combined)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum as spark_sum, avg, to_date, date_format

df_changed_date = df_combined.withColumn(
    'Date_of_Payment', 
    to_date(
        col('Date_of_Payment'), 'MM/dd/yyyy'
    )
).withColumn(
    'Payment_Month', 
    date_format(
        col('Date_of_Payment'), 'yyyy-MM'
    )
)

specialty_payment_monthly = df_changed_date.groupBy(
    'Physician_Specialty', 'Payment_Month'
).agg(
    spark_sum('Total_Amount_of_Payment_USDollars').alias('Total_Payment')
)

national_average_monthly = df_changed_date.groupBy(
    'Payment_Month'
).agg(
    avg('Total_Amount_of_Payment_USDollars').alias('National_Average')
)

df_comparison = specialty_payment_monthly.join(
    national_average_monthly, 
    on='Payment_Month'
)

display(df_comparison)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt
import ipywidgets as widgets
from IPython.display import display

def plot_specialty_vs_national(selected_specialties):
    plt.figure(figsize=(10, 6))
    
    # Plot the national average
    national_avg = df_pd_comparison.groupby('Payment_Month')['National_Average'].mean()
    plt.plot(national_avg.index, national_avg.values, label='National Average', color='black', linestyle='--')
    
    # Plot each selected specialty
    for specialty in selected_specialties:
        specialty_data = df_pd_comparison[df_pd_comparison['Physician_Specialty'] == specialty]
        plt.plot(specialty_data['Payment_Month'], specialty_data['Total_Payment'].sum(), label=specialty)
    
    plt.title("Specialty Payment vs National Average")
    plt.xlabel("Payment Month")
    plt.ylabel("Total Payment (USD)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

specialty_options = df_pd_comparison['Physician_Specialty'].unique()
specialty_selector = widgets.SelectMultiple(
    options=specialty_options,
    value=[specialty_options[0]],  # Default selection
    description='Specialties',
    disabled=False
)

# Step 5: Create an interactive widget that updates the plot
widgets.interactive(plot_specialty_vs_national, selected_specialties=specialty_selector)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import numpy as np
from scipy.stats import gaussian_kde

data = df_pd_comparison['Total_Payment'].astype("float").values

# Create the distribution plot (histogram)
plt.figure(figsize=(8,6))
plt.hist(data, bins=30, edgecolor='black', alpha=0.5, density=True)

# Add KDE plot
kde = gaussian_kde(data)
x_vals = np.linspace(min(data), max(data), 1000)
plt.plot(x_vals, kde(x_vals), color='red', lw=2)

# Add title and labels
plt.title('Distribution with KDE')
plt.xlabel('Value')
plt.ylabel('Density')

# Show the plot
plt.show()

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
