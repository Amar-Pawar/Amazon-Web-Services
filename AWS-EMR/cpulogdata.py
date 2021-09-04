'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-09-03
@Last Modified by: Amar Pawar
@Last Modified time: 2021-09-03
@Title : Script to find Avg hrs, idle hrs with AWS EMR service
/**********************************************************************************
'''
#!/usr/bin/env python 
from pyspark.sql import *
from pyspark.sql.functions import *


if __name__=="__main__":
        
    spark = SparkSession.builder.appName(
        "cpuLogData").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    df = spark.read.csv("s3n://amartest1/*.csv", header=True)
    df2 = df.select("user_name", "DateTime", "keyboard", "mouse")


    # 1) Display users and their record counts
    df10 = df2.groupBy("user_name").count()
    print()
    print("Users and their record counts")
    df10.show()

    # Task 2 : Finding users with highest number of average hours

    print("Users with highest number of average Counts")
    df.createOrReplaceTempView("view1")
    df1 = spark.sql("select user_name from view1 where keyboard != 0 or mouse != 0").groupBy(
        "user_name").count()
    df1.show(truncate=False)

    print("Users with lowest number of average seconds")
    df3 = df1.createOrReplaceTempView("hour_view")
    df4 = spark.sql(
        "select user_name,count,((((count-1)*5)*60)/6) as avg_secs from hour_view")
    df4.show(truncate=False)

    print("Users with highest number of average hours")
    highest_avg_hour = df4.withColumn("average_hours", concat(
        floor(col("avg_secs") % 86400 / 3600), lit(":"),
        floor((col("avg_secs") % 86400) % 3600 / 60), lit(""),

    ))\
        .drop("avg_secs")
    highest_avg_hour.show(truncate=False)

    # Task 3: Finding users with lowest number of average hours

    print("Users with lowest number of average hours")
    lowest_avg_hour = df4.withColumn("average_hours", concat(
        floor(col("avg_secs") % 86400 / 3600), lit(":"),
        floor((col("avg_secs") % 86400) % 3600 / 60), lit(""),
    ))\
        .drop("avg_secs")\
        .sort(asc("average_hours"))

    lowest_avg_hour.show()

    # Task 4: Finding users with highest numbers of idle hours

    print("Users with highest numbers of idle counts")
    df5 = spark.sql("select user_name from view1 where keyboard == 0 and mouse == 0").groupBy(
        "user_name").count()
    df5.show()

    print("Users with highest numbers of idle seconds")
    df5.createOrReplaceTempView('idle_hour_view')
    df6 = spark.sql(
        "select user_name,count,((((count-1)*5)*60)/6) as average_min from idle_hour_view")
    df6.show(truncate=False)

    print("Users with highest numbers of idle hours")
    idle_hour = df6.withColumn("idle_hours", concat(
        floor(col("average_min") % 86400 / 3600), lit(":"),
        floor((col("average_min") % 86400) % 3600 / 60), lit(""),

    ))\
        .drop("average_min")\
        .sort(desc("idle_hours"))
    idle_hour.show(truncate=False)
