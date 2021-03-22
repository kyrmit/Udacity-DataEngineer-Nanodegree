import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        This function loads song_data from S3, processes songs and artist tables load them back to S3
        
        Parameters:
            spark       = Spark Session
            input_data  = location of song_data where the file is loaded to process
            output_data = location of the results stored
    """
    song_data = input_data + 'song_data/*/*/*/*.json'

    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])

    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    song_fields = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude",
                      "artist_longitude as longitude"]
    
    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """
    This function loads log_data from S3, processes songs and artist tables and loaded them back to S3.
    Parameters:
            spark       = Spark Session
            input_data  = location of song_data where the file is loaded to process
            output_data = location of the results stored
    """
    
    # get filepath to log data file
    log_data_path = input_data + 'log_data/*.json'

    # read log data file
    log_df = spark.read.json(log_data_path) 
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')
    
    # extract columns for users table
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_schema_table = log_df.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    users_schema_table.write.mode('overwrite').parquet(output_data+'users_schema_table/')

   # extract columns to create time table
    time_schema_table = spark.sql(""" select distinct subquery.starttime_sub as start_time, 
    hour(subquery.starttime_sub) as hour, 
    dayofmonth(subquery.starttime_sub) as day, 
    weekofyear(subquery.starttime_sub) as week, 
    month(subquery.starttime_sub) as month, 
    year(subquery.starttime_sub) as year, 
    dayofweek(subquery.starttime_sub) as weekday 
    from (select totimestamp(timestamp.ts/1000) 
    as starttime_sub from logs_table timestamp 
    where timestamp.ts IS NOT NULL ) subquery """)
    
    # write time table to parquet files partitioned by year and month
    time_schema_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_schema_table/')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(""" select monotonically_increasing_id() as songplay_id, 
    totimestamp(l.ts/1000) as start_time, 
    month(to_timestamp(l.ts/1000)) as month, 
    year(to_timestamp(l.ts/1000)) as year, 
    l.userId as user_id, l.level as level, 
    s.song_id as song_id, s.artist_id as artist_id, 
    l.sessionId as session_id, l.location as location, 
    l.userAgent as user_agent FROM 
    logs_table l JOIN songs_table s on l.artist = s.artist_name and l.song = s.title """)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
