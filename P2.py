import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Project2').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")     # Default is INFO.


df = spark.read.option("header",True) \
    .csv("P2/registered_companies.csv")
df.printSchema()

df = df.drop("LATEST_YEAR_ANNUAL_RETURN", "LATEST_YEAR_FINANCIAL_STATEMENT")

df = df.withColumn('DATE_OF_REGISTRATION',to_date("DATE_OF_REGISTRATION", 'dd-MM-yyyy'))\
        .withColumn("AUTHORIZED_CAP",col("AUTHORIZED_CAP").cast("int"))\
        .withColumn("PAIDUP_CAPITAL",col("PAIDUP_CAPITAL").cast("int"))
df.printSchema()

df.createOrReplaceTempView("companies")

#1
print("Q.1 Total number of companies in each state --->")
df.groupBy("REGISTERED_STATE").agg(count("CORPORATE_IDENTIFICATION_NUMBER").alias("Number_of_Companies")).show(36,truncate = False)
#---------------------------------------------------------------------------------------------------------------------------------------------
#2
print("Q.2 Total number of companies of each status --->")
df3=spark.sql("Select COMPANY_STATUS, count(CORPORATE_IDENTIFICATION_NUMBER)\
as Number_of_Companies from companies group by COMPANY_STATUS")
#df3.show()
df3.select(when(df3.COMPANY_STATUS=="ACTV","Active") \
        .when(df3.COMPANY_STATUS=="NAEF","Unavailable") \
        .when(df3.COMPANY_STATUS=="ULQD","under liquidtaion")\
        .when(df3.COMPANY_STATUS=="AMAL","Amalgated")\
        .when(df3.COMPANY_STATUS=="STOF","Strike off")\
        .when(df3.COMPANY_STATUS=="DISD","Dissolved")\
        .when(df3.COMPANY_STATUS=="CLLD","Conv to LLP")\
        .when(df3.COMPANY_STATUS=="UPSO","Under process of SO")\
        .when(df3.COMPANY_STATUS=="CLLP","Converted LLP")\
        .when(df3.COMPANY_STATUS=="LIQD","Liquidated")\
        .when(df3.COMPANY_STATUS=="DRMT","Dormant")\
        .when(df3.COMPANY_STATUS=="MLIQ","Vanished")\
        .when(df3.COMPANY_STATUS=="D455","Dormant under 455")
        .when(col("COMPANY_STATUS").isNull() ,"undefined") \
        .otherwise(df3.COMPANY_STATUS).alias("STATUS"),"Number_of_Companies"\
).show()
#---------------------------------------------------------------------------------------------------------------------------------------------
#3
print("3- Total number of companies in each company class --->")

df.groupBy("COMPANY_CLASS").agg(count("CORPORATE_IDENTIFICATION_NUMBER")).show()
#--------------------------------------------------------------------------------------------------------------------------
#4
print("#4 Number of companies in each of principal business activity ----> ")

df.groupBy("PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN") \
    .agg(count("CORPORATE_IDENTIFICATION_NUMBER").alias("Number_of_Companies")) \
    .show(truncate=False)
#--------------------------------------------------------------------------------------------------------------------------
#5 
print("6.Total number of companies registered every year. ----> ")
df.groupBy(year("DATE_OF_REGISTRATION").alias("Year"))\
.agg(count("CORPORATE_IDENTIFICATION_NUMBER").alias("Number_of_Companies"))\
.orderBy("Year").show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#6
print("6.Details of duplicate company names --->")
df.groupBy("COMPANY_NAME").agg(count("COMPANY_NAME").alias("Count")).where(col("Count") > 1).show(200,truncate = False)

#--------------------------------------------------------------------------------------------------------------------------------------------
#7
print("7- Which are Top 50 oldest companies? --->")

df.select("COMPANY_NAME", "DATE_OF_REGISTRATION", "REGISTERED_STATE")\
    .filter(df.DATE_OF_REGISTRATION.isNotNull())\
    .orderBy("DATE_OF_REGISTRATION").show(50)
#---------------------------------------------------------------------------------------------------------------------------------------------
#8
print("#8 List of companies registered between 1990-2020 in Arunchal Pradesh,Lakshadweep,Mizoram,Nagaland ----> ")

df.select("COMPANY_NAME", "COMPANY_STATUS", "DATE_OF_REGISTRATION","REGISTERED_STATE" )\
    .filter((df.DATE_OF_REGISTRATION >= '1990-01-01') & (df.DATE_OF_REGISTRATION <= '2020-12-31')\
         & (df.REGISTERED_STATE.isin('Lakshadweep','Mizoram','Arunachal Pradesh', 'Nagaland')))\
    .show(1500)

#---------------------------------------------------------------------------------------------------------------------------------------------
#9
print("9.List all the private companies which are active in Delhi or Karnataka and has authorized capital greater than 3 CR ---->")
df.select("COMPANY_NAME","COMPANY_STATUS", "COMPANY_CLASS", "REGISTERED_STATE", "AUTHORIZED_CAP")\
    .filter((df.COMPANY_CLASS =='Private') & (df.COMPANY_STATUS=='ACTV') & (df.REGISTERED_STATE.isin('Delhi', 'Karnataka')) & (df.AUTHORIZED_CAP > 30000000))\
    .show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#10
print("10.List all the public companies which are under liquidation or liquidated in India and belong to State-govt or Union Govt registered after 1985 --->")
df.select("COMPANY_NAME","COMPANY_STATUS", "COMPANY_CLASS","DATE_OF_REGISTRATION")\
    .filter((df.COMPANY_STATUS.isin('ULQD','LIQD')) & (df.COMPANY_CLASS == 'Public') & (df.DATE_OF_REGISTRATION > '1985-12-31')).show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#11
print("11- List the names of the company and company class which are neither active in Maharashtra nor Delhi ---->")

df.select("COMPANY_NAME", "COMPANY_STATUS","COMPANY_CLASS", "REGISTERED_STATE")\
    .filter( (~df.REGISTERED_STATE.isin('Maharashtra', 'Delhi')) & (df.COMPANY_STATUS != "ACTV") ).show(1000)
#-------------------------------------------------------------------------------------------------------------------------------------------------------
#12
print("#12 List of companies which are vanished ----->")

df.select('COMPANY_NAME','COMPANY_CLASS','COMPANY_STATUS', 'DATE_OF_REGISTRATION','REGISTERED_STATE','REGISTRAR_OF_COMPANIES')\
    .filter(df.COMPANY_STATUS == 'MLIQ').show()
#---------------------------------------------------------------------------------------------------------------------------------------------
#13
print("#13 List 50 recently registered companies. ----->")

df.select("COMPANY_NAME","COMPANY_STATUS", "DATE_OF_REGISTRATION")\
    .orderBy(col("DATE_OF_REGISTRATION").desc()).show(50)
#---------------------------------------------------------------------------------------------------------------------------------------------
#14
print("14.List the private(one person company) companies and email address of its owner registered in Mumbai or Delhi ROC --->")
df.select("COMPANY_NAME", "COMPANY_CLASS","REGISTERED_STATE","REGISTRAR_OF_COMPANIES", "EMAIL_ADDR")\
    .filter((df.COMPANY_CLASS == 'Private(One Person Company)') & (df.REGISTRAR_OF_COMPANIES.isin('ROC MUMBAI','ROC DELHI'))).show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#15
print("15- List all the companies whose paidup capital is greater than authorized capital --->")

df.select("COMPANY_NAME", "PAIDUP_CAPITAL", "AUTHORIZED_CAP")\
    .filter(df.PAIDUP_CAPITAL > df.AUTHORIZED_CAP).show()
#-------------------------------------------------------------------------------------------------------------------------------------------------
#16
print("#16 List of Private(One Person Company) whose paidup capital is greater than 1 CR ----> ")

df.select('COMPANY_NAME', 'COMPANY_CLASS','AUTHORIZED_CAP', 'PAIDUP_CAPITAL')\
    .filter((df.COMPANY_CLASS == 'Private(One Person Company)') & (df.PAIDUP_CAPITAL > 10000000)).show()

#---------------------------------------------------------------------------------------------------------------------------------------------
#17
print("#17 List the state-govt companies belonging to Gujarat and Karnataka . ----> ")

df.select("COMPANY_NAME", "COMPANY_STATUS","COMPANY_SUB_CATEGORY","REGISTERED_STATE")\
    .filter((df.COMPANY_SUB_CATEGORY=='State Govt company') & (df.REGISTERED_STATE.isin('Gujarat', 'Karnataka'))).show(500)

#---------------------------------------------------------------------------------------------------------------------------------------------
#18
print("18.List the oil companies which are private .")

df.groupBy(year("DATE_OF_REGISTRATION").alias('Year'))\
    .agg(count(when((df.COMPANY_CLASS == 'Private') & (df.COMPANY_NAME.contains('OIL')),True)).alias("Number_of_Private_OIL_Companies"),
         count(when((df.COMPANY_CLASS == 'Public') & (df.COMPANY_NAME.contains('OIL')),True)).alias("Number_of_Public_OIL_Companies") 
    ).orderBy("Year").show(100)
#---------------------------------------------------------------------------------------------------------------------------------------------
#19
print("19- List all private(One person company) companies which are Dormant under section 455 --->")

df.select("COMPANY_NAME", "COMPANY_STATUS", "COMPANY_CLASS")\
    .filter( (df.COMPANY_CLASS == "Private(One Person Company)") & (df.COMPANY_STATUS == "D455") ).show()
#--------------------------------------------------------------------------------------------------------------------------------------------------
#20
print("#20 List of companies which are subsidiaries of foreign company and belong to Tamil Nadu, West Bengal, Delhi ---> ")

df.select('COMPANY_NAME','COMPANY_CLASS', 'COMPANY_SUB_CATEGORY','COMPANY_STATUS', 'DATE_OF_REGISTRATION','REGISTERED_STATE','REGISTRAR_OF_COMPANIES')\
    .filter((df.REGISTERED_STATE.isin('Tamil Nadu', 'West Bengal', 'Delhi')) & (df.COMPANY_SUB_CATEGORY == 'Subsidiary of Foreign Company')).show()
#--------------------------------------------------------------------------------------------------------------------------------------------------
#21
print("#21 Number of companies which are subsidiaries of foreign company registered every year ---->")

df.groupBy(year("DATE_OF_REGISTRATION").alias("Year"))\
    .agg(count(when(df.COMPANY_SUB_CATEGORY == "Subsidiary of Foreign Company",True).alias("Number_of_Foreign_Companies_Registered")\
        ).orderBy("Year").show(100)






# root
#  |-- CORPORATE_IDENTIFICATION_NUMBER: string (nullable = true)
#  |-- COMPANY_NAME: string (nullable = true)
#  |-- COMPANY_STATUS: string (nullable = true)
#  |-- COMPANY_CLASS: string (nullable = true)
#  |-- COMPANY_CATEGORY: string (nullable = true)
#  |-- COMPANY_SUB_CATEGORY: string (nullable = true)
#  |-- DATE_OF_REGISTRATION: string (nullable = true)
#  |-- REGISTERED_STATE: string (nullable = true)
#  |-- AUTHORIZED_CAP: string (nullable = true)
#  |-- PAIDUP_CAPITAL: string (nullable = true)
#  |-- INDUSTRIAL_CLASS: string (nullable = true)
#  |-- PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN: string (nullable = true)
#  |-- REGISTERED_OFFICE_ADDRESS: string (nullable = true)
#  |-- REGISTRAR_OF_COMPANIES: string (nullable = true)
#  |-- EMAIL_ADDR: string (nullable = true)
#  |-- LATEST_YEAR_ANNUAL_RETURN: string (nullable = true)
#  |-- LATEST_YEAR_FINANCIAL_STATEMENT: string (nullable = true)
