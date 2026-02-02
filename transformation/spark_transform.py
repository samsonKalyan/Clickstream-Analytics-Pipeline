from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("clickStream").getOrCreate()

raw_df = spark.read.json("clickstream.json")

#Convert the timestamp col to timestamp

raw_df = raw_df.withColumn("event_time", to_timestamp('timestamp'))

window_spec = Window.partitionBy('user_id').orderBy('event_time')

session_df = raw_df.withColumn("session_diff", unix_timestamp('event_time') - \
                               unix_timestamp(lag('event_time').over(window_spec))) 

#if a session diff is more than 30 min we consider it as new session

session_df = session_df.withColumn("new_session", \
                                   when(col('session_diff')>1800,1).otherwise(0))

session_df.filter(col("new_session")== 1).show()

#count daily active users

daily_users = session_df.groupBy(to_date('event_time').alias("date")) \
    .agg(countDistinct(col('user_id')).alias('daily_active_users'))

daily_users.show()