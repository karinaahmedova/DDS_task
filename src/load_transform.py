import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

file_store = '/FileStore/files/'
GPPractices_save = '/FileStore/output/GPPractices/'
GPOpeningTimes_save = '/FileStore/output/GPOpeningTimes/'
TransparencyIndicatorsGPPerformance_save = '/FileStore/output/TransparencyIndicatorsGPPerformance/'
AntibioticPrescribingValue_save = '/FileStore/output/AntibioticPrescribingValue/'
OpeningMinutesPerWeek_save = '/FileStore/output/OpeningMinutesPerWeek/'

def load_data(file_name):
  spark = SparkSession.builder.getOrCreate()
  df = spark.read.format("csv").option("delimiter", "¬").option("header",True).option("encoding", "ISO-8859-1").load(file_store + file_name)
  return df

#GPPractice
df = load_data('GPPractices.csv')
df.createOrReplaceTempView("GPPractice")
df = spark.sql("""
  SELECT 
    OrganisationID
    ,OrganisationCode
    ,OrganisationType
    ,SubType
    ,OrganisationName
    ,OrganisationStatus
    ,CAST(IsPimsManaged AS BOOLEAN) AS IsPimsManaged
    ,Address1
    ,Address2
    ,Address3
    ,City
    ,County
    ,Postcode
    ,CAST(Latitude AS FLOAT) AS Latitude
    ,CAST(Longitude AS FLOAT) AS Longitude
  FROM GPPractice
 """)
df.write.format("delta").mode("overwrite").save(GPPractices_save)

#GPOpeningTimes
df = load_data('GPOpeningTimes.csv')
df.createOrReplaceTempView("GPOpeningTimes")
GPO = spark.sql("""
  SELECT 
    OrganisationID
    ,WeekDay
    ,Times
    ,CAST(IsOpen AS BOOLEAN) AS IsOpen
    ,OpeningTimeType
    ,AdditionalOpeningDate
  FROM GPOpeningTimes
 """)
df.write.format("delta").mode("overwrite").save(GPOpeningTimes_save)



#TransparencyIndicatorsGPPerformance
df = load_data('TransparencyIndicatorsGPPerformance.csv')
df.createOrReplaceTempView("TransparencyIndicatorsGPPerformance")
df = spark.sql("""
  SELECT 
    OrganisationID
    ,OrganisationCode
    ,OrganisationName
    ,MetricName
    ,Value
    ,Text
  FROM TransparencyIndicatorsGPPerformance
 """)
df.write.format("delta").mode("overwrite").save(TransparencyIndicatorsGPPerformance_save)

#AntibioticPrescribingValue
df = spark.sql("""
  SELECT DISTINCT
      tig.OrganisationName
    ,gpp.Address1
    ,gpp.Address2
    ,gpp.Address3
    ,gpp.City
    ,gpp.County
    ,gpp.Postcode
    ,CAST(tig.Value AS FLOAT) AS Value
  FROM  delta.`/FileStore/test/karina/test/output/TransparencyIndicatorsGPPerformance` tig
    JOIN delta.`/FileStore/test/karina/test/output/GPPractices` gpp on tig.OrganisationID = gpp.OrganisationID
  WHERE MetricName = "Antibiotic Prescribing" AND CAST(Value AS FLOAT) > 0.5
""")
df.write.format("delta").mode("overwrite").save(AntibioticPrescribingValue_save)

#ReceptionOpeningMinutes
df = spark.sql("""
  SELECT 
  *
  FROM  delta.`/FileStore/test/karina/test/output/GPOpeningTimes`
  WHERE IsOpen = True AND OpeningTimeType = 'Reception'
""").persist()
df = df.withColumn("start", F.split(F.col("Times"), "-")[0])
df = df.withColumn("end", F.split(F.col("Times"), "-")[1])
df = df.withColumn("start_timestamp",F.to_timestamp("start"))
df = df.withColumn("end_timestamp",F.to_timestamp("end"))
df = df.withColumn('InSeconds',F.col("end_timestamp").cast("long") - F.col('start_timestamp').cast("long"))
df = df.withColumn('InMinutes',F.round(F.col('InSeconds')/60))
df = df.groupBy("OrganisationId")\
      .agg(sum("InMinutes").alias("MinutesPerWeek"))\
      .sort(desc("MinutesPerWeek"))
df.write.format("delta").mode("overwrite").save(OpeningMinutesPerWeek_save)