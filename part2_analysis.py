from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, when, hour, month, dayofmonth, count, to_date, year,
)
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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
    df = df.withColumn("year", year(col("date")).cast("int"))

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
    

    sport_locations_df = df.filter(
            (col("location_description").isNotNull())&
            (col("location_description") != "") &
            (col("location_description").rlike(r"(?i)\bsports?\b"))
            )

    crimes_sport_locations = sport_locations_df.groupby("location_description", "primary_type").agg(count("*").alias("total_crimes")).orderBy("location_description",col("total_crimes").desc())
    write_to_mysql(crimes_sport_locations, "sport_location_crimes", spark)
    df_location = df.filter(col("location_description").isNotNull() & (col("location_description") != ""))


    theft_counts = df_location.filter(col("primary_type") == "THEFT") \
        .groupBy("location_description") \
        .agg(count("*").alias("total_thefts")) \
        .orderBy(col("total_thefts").desc())


    total_counts = df_location.groupBy("location_description") \
        .agg(count("*").alias("total_crimes")) \
        .orderBy(col("total_crimes").desc())

    theft_vs_total = theft_counts.join(total_counts, on="location_description", how="inner")


    write_to_mysql(theft_vs_total, "theft_by_location", spark)


    # Read holidays from MySQL holidays table
    holidays_df = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "holidays") \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASSWORD) \
        .option("driver", JDBC_DRIVER) \
        .load()
    holidays_df = holidays_df.withColumn("holiday_date", col("holiday_date").cast("date"))
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

    #crimes_by_loc_type = df_location.groupBy("location_description", "primary_type").agg(count("*").alias("total")).orderBy("location_description", col("total").desc())
    #write_to_mysql(crimes_by_loc_type, "crimes_by_location_and_type", spark)


    location_type_counts = df_location.groupBy("location_description", "primary_type").agg(count("*").alias("total"))
    location_totals = df_location.groupBy("location_description").agg(count("*").alias("location_total"))
    location_totals_filtered = location_totals.filter(col("location_total") > 100)
    joined = location_type_counts.join(location_totals_filtered,on="location_description",how="inner")
    windowSpec = Window.partitionBy("location_description").orderBy(col("total").desc())
    ranked = joined.withColumn("rank",row_number().over(windowSpec))
    top_crime_per_location = ranked.filter(col("rank") == 1).drop("rank")
    write_to_mysql(top_crime_per_location, "crimes_by_location", spark) 
    
    df_community = df.filter(col("community_area").isNotNull() & (col("community_area") != ""))
    community_area_crimes = df_community.groupBy("community_area").agg(count("*").alias("total_crimes")).orderBy(col("total_crimes").desc())
    write_to_mysql(community_area_crimes, "community_area_crimes", spark)

    # Downtown areas have higher rates of theft and robbery than residential areas.
    df_area = df.filter(col("community_area").isNotNull() & (col("community_area") != ""))

    downtown_ids = [32, 8, 33, 28]

    df_area = df_area.withColumn(
        "area_type",
        when(col("community_area").isin(downtown_ids), "Downtown")
        .otherwise("Residential")
    )

    area_totals = (
        df_area.groupBy("area_type")
        .agg(count("*").alias("total_crimes"))
    )

    area_tr = (
        df_area.filter(col("primary_type").isin("THEFT", "ROBBERY"))
        .groupBy("area_type")
        .agg(count("*").alias("tr_crimes"))
    )

    downtown_vs_residential = (
        area_tr.join(area_totals, "area_type")
        .withColumn("rate", col("tr_crimes") / col("total_crimes"))
    )

    write_to_mysql(downtown_vs_residential, "downtown_vs_residential_theft_robbery", spark)

    # Public transit locations (train stations, buses) have higher robbery rates than commercial areas.
     # 1)filter out rows with invalid columns 
    df_robbery = df.filter((col("primary_type") == "ROBBERY") & col("location_description").isNotNull())

    # 2) group data into buckets: public transit and commercial areas
    robberies_categorized = df_robbery.withColumn(
    "location_type",
    when(col("location_description").rlike(r"(?i)(TRAIN|BUS|TRANSIT|STATION)"), "Public Transit")
    .when(col("location_description").rlike(r"(?i)(COMMERCIAL|STORE|SHOP|MARKET)"), "Commercial")
    .otherwise("Other"))

    # 3) make comparison
    robbery_by_location = robberies_categorized.groupBy("location_type").agg(count("*").alias("robbery_count"))

    #4) automateically create relation in schema.sql for sql
    write_to_mysql(robbery_by_location, "transit_vs_commercial_robbery_count", spark)


    # More theft incidents occur around airports compared to other areas.

    #1) filter - look for any location_description that contains any word of airport. 
    #it could be airport, AIRPORT, airport terminal etc. 
    theft = df.filter(
    (col("primary_type") == "THEFT") &
    (col("location_description").isNotNull()) 
    )

    #2) categorize #airport v.s anywhere else is certainly biased so was more specific
    #airport v.s (others - commercial, transit, residentical)
    theft_category = theft.withColumn("location_type", 
    # Change the first line to:
    when(col("location_description").rlike(r"(?i)AIR(PORT|CRAFT)"), "Airport")
    .when(col("location_description").rlike(r"(?i)(COMMERCIAL|STORE|SHOP|MARKET)"), "Commercial")
    .when(col("location_description").rlike(r"(?i)(RESIDENTIAL|HOME|HOUSE|NEIGHBORHOOD|APARTMENT)"), "Residential")
    .when(col("location_description").rlike(r"(?i)(TRAIN|BUS|TRANSIT|STATION)"), "Public Transit")
    .otherwise("Uncategorized"))

    #3) evaluate
    theft_by_location = theft_category.groupBy("location_type").agg(count("*").alias("theft_count"))
   
    #4) write to sql
    write_to_mysql(theft_by_location, "airport_theft_count_comparison", spark)
    
    elapsed = time.time() - start_total
    
    print()
    print()
    print("-------------")
    print("Total runtime: {:.2f} seconds".format(elapsed))
    print("-------------")
    print()
    print()

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
