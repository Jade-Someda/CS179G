from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, try_to_timestamp, lit



spark = SparkSession.builder \
    .appName("ChicagoCrimeClean") \
    .getOrCreate()


crime_df = spark.read.option("header", True).csv("/Users/nehagutlapalli/Downloads/Chicago_Crimes (1).csv")
#crime_df.printSchema()
#crime_df.show(5)

crime_df = crimedf.toDF(
    *[c.lower().replace(" ", "") for c in crime_df.columns]
)

crime_df = crime_df.dropna(
    subset=["date", "primary_type", "x", "y"]
)

crime_df = crime_df.withColumn(
    "date",
    try_to_timestamp(col("date"), lit("MM/dd/yyyy hh:mm:ss a"))
)

crime_df = crime_df.withColumn(
    "updated_on",
    try_to_timestamp(col("updated_on"), lit("MM/dd/yyyy hh:mm:ss a"))
)


crime_df = crime_df.withColumn(
    "arrest",
    when(lower(trim(col("arrest"))) == "true", True)
    .when(lower(trim(col("arrest"))) == "false", False)
    .otherwise(None)
)

crime_df = crime_df.withColumn(
    "domestic",
    when(lower(trim(col("domestic"))) == "true", True)
    .when(lower(trim(col("domestic"))) == "false", False)
    .otherwise(None)
)

crime_df = crime_df.dropDuplicates(["id"])


crime_df.write \
     .mode("overwrite") \
    .parquet("/Users/nehagutlapalli/Downloads/clean_chicago_crime")


spark.stop()
