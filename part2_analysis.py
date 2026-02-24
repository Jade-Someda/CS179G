from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, when, hour, month, dayofmonth, count, to_date,
)
import time

PARQUET_PATH = "/home/cs179g/project/CS179G/clean_chicago_crime"
JDBC_URL = "jdbc:mysql://127.0.0.1:3306/cs179g"
JDBC_USER = "root"
JDBC_PASSWORD = ""
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

def main():
    start_total = time.time()

    spark = SparkSession.builder \
        .appName("ChicagoCrimePart2") \
        .getOrCreate()

    print("Reading cleaned data from Parquet...")
    df = spark.read.parquet(PARQUET_PATH)
    df = df.withColumn("hour", hour(col("date")))
    df = df.withColumn("month", month(col("date")))
    df = df.withColumn("day", dayofmonth(col("date")))
    df = df.withColumn("date_only", to_date(col("date")))
    df = df.withColumn("year", col("year").cast("int"))

    hourly_crimes = df.groupBy("hour").agg(count("*").alias("crime_count")).orderBy("hour")
    write_to_mysql(hourly_crimes, "hourly_crimes", spark)

    time_period_crimes = df.withColumn(
        "time_period",
        when((col("hour") >= 8) & (col("hour") <= 15), "Daytime")
        .when((col("hour") >= 16) & (col("hour") <= 23), "Evening")
        .otherwise("Overnight")
    ).groupBy("time_period").agg(count("*").alias("total_crimes"))
    write_to_mysql(time_period_crimes, "time_period_crimes", spark)

    monthly_crimes = df.groupBy("month").agg(count("*").alias("total_crimes")).orderBy("month")
    write_to_mysql(monthly_crimes, "monthly_crimes", spark)

    season_crimes = df.withColumn(
        "season",
        when(col("month").isin(6, 7, 8), "Summer")
        .when(col("month").isin(1, 2), "Late Winter")
        .otherwise("Other")
    ).groupBy("season").agg(count("*").alias("total_crimes"))
    write_to_mysql(season_crimes, "season_crimes", spark)

    christmas = df.filter(col("month") == 12).groupBy("primary_type").agg(count("*").alias("total")).orderBy(col("total").desc())
    write_to_mysql(christmas, "christmas_by_type", spark)

    halloween = df.filter((col("month") == 10) & (col("day") == 31)).groupBy("primary_type").agg(count("*").alias("total")).orderBy(col("total").desc())
    write_to_mysql(halloween, "halloween_by_type", spark)

    thanksgiving_dates = ["2022-11-24", "2023-11-23", "2024-11-28", "2021-11-25", "2020-11-26", "2019-11-28"]
    thanksgiving = df.filter(col("date_only").cast("string").isin(thanksgiving_dates)).groupBy("primary_type").agg(count("*").alias("total")).orderBy(col("total").desc())
    write_to_mysql(thanksgiving, "thanksgiving_by_type", spark)

    holiday_dates = [
        "2024-01-01", "2024-07-04", "2024-12-25", "2023-01-01", "2023-07-04", "2023-12-25",
        "2022-01-01", "2022-07-04", "2022-12-25", "2021-01-01", "2021-07-04", "2021-12-25",
    ]
    holidays_df = spark.createDataFrame([Row(holiday_date=d) for d in holiday_dates]).withColumn("holiday_date", col("holiday_date").cast("date"))
    df_with_holiday = df.join(holidays_df, df["date_only"] == holidays_df["holiday_date"], "left")
    holiday_vs = df_with_holiday.withColumn(
        "day_type", when(col("holiday_date").isNotNull(), "Holiday").otherwise("Non-Holiday")
    ).groupBy("day_type").agg(count("*").alias("total_crimes"))
    write_to_mysql(holiday_vs, "holiday_vs_nonholiday", spark)

    great_recession = df.filter(col("year").isin(2007, 2008, 2009)).groupBy("primary_type").agg(count("*").alias("total")).orderBy(col("total").desc())
    write_to_mysql(great_recession, "great_recession_by_type", spark)

    yearly_crimes = df.groupBy("year").agg(count("*").alias("total_crimes")).orderBy("year")
    write_to_mysql(yearly_crimes, "yearly_crimes", spark)

    df_location = df.filter(col("location_description").isNotNull() & (col("location_description") != ""))
    crimes_by_location = df_location.groupBy("location_description").agg(count("*").alias("total_crimes")).orderBy(col("total_crimes").desc())
    write_to_mysql(crimes_by_location, "crimes_by_location", spark)

    crimes_by_loc_type = df_location.groupBy("location_description", "primary_type").agg(count("*").alias("total")).orderBy("location_description", col("total").desc())
    write_to_mysql(crimes_by_loc_type, "crimes_by_location_and_type", spark)

    df_community = df.filter(col("community_area").isNotNull() & (col("community_area") != ""))
    community_area_crimes = df_community.groupBy("community_area").agg(count("*").alias("total_crimes")).orderBy(col("total_crimes").desc())
    write_to_mysql(community_area_crimes, "community_area_crimes", spark)

    elapsed = time.time() - start_total
    print("Total runtime: {:.2f} seconds".format(elapsed))
    spark.stop()


def write_to_mysql(dataframe, table, spark):
    dataframe.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table) \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASSWORD) \
        .option("driver", JDBC_DRIVER) \
        .mode("overwrite") \
        .save()
    print("Wrote table:", table)


if __name__ == "__main__":
    main()
