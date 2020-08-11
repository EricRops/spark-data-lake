# import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
import logging

# Bring in Pandas (pandas install is done via bootstrapping at EMR cluster creation)
import pandas as pd

# Get AWS credentials - NOT NEEDED because we SSH directly into the EMR cluster
# config = configparser.ConfigParser()
# # config.read('dl.cfg')
# config.read_file(open('/home/ericr/.aws/credentials.cfg'))
# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

# Setup the logging output print settings (logging to a file)
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(message)s')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('logfile.log', mode='w')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)


def create_spark_session():
    """
    - Create a Spark session to perform the ETL tasks
    """
    spark = SparkSession \
        .builder \
        .appName("SparkETL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - load the songs JSON data from Udacity S3 bucket
    - process the data into Spark Dataframes for the songs and artists tables
    - write each DF to parquet files in S3
    :param spark: Active PySpark session (created from create_spark_session function)
    :param input_data: (str): Udacity S3 bucket top-level path to pull the song and log data from
    :param output_data: (str): S3 bucket path to write final tables to
    :return songs_table: (Spark DataFrame): The songs DF to pass to the process_log_data function
    """
    # get file path to song data file
    # song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file
    logger.info("Reading song data from {} ..........".format(song_data))
    df = spark.read.json(song_data)
    logger.info("Metadata for {} songs successfully loaded into Spark".format(df.count()))

    # extract columns to create songs table
    # Drop rows with duplicate song_ids
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
        .dropDuplicates(['song_id'])
    n_partitions = songs_table.rdd.getNumPartitions()
    logger.info("Songs DataFrame has {} partitions by default".format(n_partitions))

    # write songs table to parquet files partitioned by year and artist
    logger.info("Writing songs table to S3..........")
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(os.path.join(output_data, 'songs'), 'overwrite')
    logger.info('{} rows written to {}songs'.format(songs_table.count(), output_data))

    # extract columns to create artists table
    # Drop rows with duplicate artist_ids
    artists_table = df.select(
        df.artist_id,
        F.col("artist_name").alias("name"),
        F.col("artist_location").alias("location"),
        F.col("artist_latitude").alias("latitude"),
        F.col("artist_longitude").alias("longitude")
        ).dropDuplicates(['artist_id'])
    n_partitions = artists_table.rdd.getNumPartitions()
    logger.info("Artists table has {} partitions by default".format(n_partitions))

    # write artists table to parquet files
    logger.info("Writing artists table to S3..........")
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')
    logger.info('{} rows written to {}artists'.format(artists_table.count(), output_data))

    # Return songs df to create songplays table in next function
    return df


def process_log_data(spark, input_data, output_data, songs_df):
    """
    - load the songplay log JSON data from Udacity S3 bucket
    - process the data into Spark Dataframes for the users, time, and songplays tables
    - write each DF to parquet files in S3
    :param spark: Active PySpark session (created from create_spark_session function)
    :param input_data: (str): Udacity S3 bucket top-level path to pull the song and log data from
    :param output_data: (str): S3 bucket path to write tables to
    :param songs_df: (Spark DataFrame): The DF for the songs raw data, used for joining to create the songplays table
    """
    # log_data = os.path.join(input_data, "log_data/2018/11/2018-11-01-events.json")
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    logger.info("Reading log data from {} ..........".format(log_data))
    df = spark.read.json(log_data)
    logger.info("Metadata for {} songplay events successfully loaded into Spark".format(df.count()))

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table (include timestamp "ts" to keep most recent level status)
    # users_table = df.select(["userId", "firstName", "lastName", "gender", "level", "ts"])
    users_table = df.select(
        F.col("userId").alias('user_id').cast(T.LongType()),
        F.col("firstName").alias('first_name'),
        F.col("lastName").alias('last_name'),
        df.gender,
        df.level,
        df.ts
    )
    # Filter to most recent record ("ts" column) to get the latest paid "level" status for users table
    users_table = users_table.withColumn("row_number", F.row_number() \
                                         .over(Window.partitionBy(users_table.user_id).orderBy(users_table.ts.desc())))
    logger.info("First 10 rows of Users table before filtering ({} total rows):".format(users_table.count()))
    logger.info('{}'.format(users_table.limit(10).toPandas()))
    # Keep only most recent user record, and drop the timestamp and row_number columns
    users_table = users_table.filter(F.col("row_number") == 1).drop("ts", "row_number")
    logger.info("First 10 rows of FINAL Users table ({} total rows):".format(users_table.count()))
    logger.info('{}'.format(users_table.limit(10).toPandas()))

    # write users table to parquet files
    n_partitions = users_table.rdd.getNumPartitions()
    logger.info("Users DataFrame has {} partitions by default".format(n_partitions))
    logger.info("Writing users table to S3..........")
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')
    logger.info('{} rows written to {}users'.format(users_table.count(), output_data))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # extract columns to create time table
    # Drop rows with duplicate start_times
    time_table = df.select(F.col("start_time").cast(T.TimestampType()),
                           F.hour("start_time").alias("hour"),
                           F.dayofmonth("start_time").alias("day"),
                           F.weekofyear("start_time").alias("week"),
                           F.month("start_time").alias("month"),
                           F.year("start_time").alias("year"),
                           F.dayofweek("start_time").alias("weekday")) \
        .dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    n_partitions = time_table.rdd.getNumPartitions()
    logger.info("Time DataFrame has {} partitions by default".format(n_partitions))
    logger.info("Writing time table to S3..........")
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')
    logger.info('{} rows written to {}time'.format(time_table.count(), output_data))

    # extract columns from joined song and log datasets to create songplays table
    # Join by song title, artist name, and durations together
    # THEN select the appropriate columns from each
    # NOTE: df = the logs df; songs_df = the songs data df
    # NOTE: left_outer join being used so that any log data without song data matches are preserved
    songplays_table = df.join(songs_df,
                              (df.song == songs_df.title) &
                              (df.artist == songs_df.artist_name) &
                              (df.length == songs_df.duration), 'left_outer') \
        .select(
            F.monotonically_increasing_id().alias("songplay_id"),
            F.col("start_time").cast(T.TimestampType()),
            F.col("userId").alias('user_id').cast(T.LongType()),
            df.level,
            songs_df.song_id,
            songs_df.artist_id,
            F.col("sessionId").alias("session_id"),
            df.location,
            F.col("useragent").alias("user_agent")
    )

    n_partitions = songplays_table.rdd.getNumPartitions()
    logger.info("Songplays DataFrame has {} partitions by default".format(n_partitions))
    # write songplays table to parquet files partitioned by year and month
    # NOTE: creating year and month columns during the writing step to try preserving the proper schema
    # NOTE: Attempting to drop year and month in the final schema before writing
    logger.info("Writing songplays table to S3..........")
    songplays_table.withColumn("year", F.year("start_time")).withColumn("month", F.month("start_time")) \
        .write.mode("overwrite").partitionBy("year", "month") \
        .parquet(os.path.join(output_data, 'songplays'))
    logger.info('{} rows written to {}songplays'.format(songplays_table.count(), output_data))


def quality_control(spark, input_data, output_data, print_nrows):
    """
    - Read the tables back in as Spark DataFrames
    - Print top rows and schemas to log file to ensure tables are as expected
    - Run some SQL queries and save as CSV files
    :param spark: Active PySpark session (created from create_spark_session function)
    :param input_data: (str): S3 bucket path to pull the final analytics tables from
    :param output_data: (str): Location to write the test CSV files (SQL query results)
    :param print_nrows: (int): how many rows from each table to print to logfile
    """
    table_list = ("songs", "artists", "users", "time", "songplays")
    # pd.set_option('max_colwidth', 800)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    all_tables_dict = {}
    for table in table_list:
        logger.info("Reading parquet files: {}{}/ ..........".format(input_data, table))
        temp_df = spark.read.parquet(input_data + table + '/')
        schema_str = temp_df._jdf.schema().treeString()
        n_partitions = temp_df.rdd.getNumPartitions()
        logger.info("{} total rows loaded. {} table has {} partitions".format(temp_df.count(), table, n_partitions))
        logger.info("First {} rows of {} ({} total rows):".format(print_nrows, table, temp_df.count()))
        logger.info('{}'.format(temp_df.limit(print_nrows).toPandas()))
        logger.info("Schema for {}:".format(table))
        logger.info('{}'.format(schema_str))
        all_tables_dict[table] = temp_df

    # Read in a single partition of songplays table to verify "year" and "month" are not in the schema
    songplays_temp = spark.read.parquet(input_data+"songplays/year=2018/month=11/")
    schema_str = songplays_temp._jdf.schema().treeString()
    logger.info("Schema for songplays with just 1 partition loaded:")
    logger.info('{}'.format(schema_str))

    # Run some queries on the tables and save the results as CSV files
    from sql_queries import songplays_full_data, popular_artists, listening_locations, songs_skew, time_skew
    all_tables_dict["songs"].createOrReplaceTempView("songs")
    all_tables_dict["artists"].createOrReplaceTempView("artists")
    all_tables_dict["time"].createOrReplaceTempView("time")
    all_tables_dict["songplays"].createOrReplaceTempView("songplays")
    if not os.path.exists(output_data):
        os.mkdir(output_data)
    spark.sql(songplays_full_data).toPandas().to_csv(output_data+"songplays_full_data.csv", header=True, index=False, mode="w")
    spark.sql(popular_artists).toPandas().to_csv(output_data+"popular_artists.csv", header=True, index=False, mode="w")
    spark.sql(listening_locations).toPandas().to_csv(output_data+"listening_locations.csv", header=True, index=False, mode="w")
    spark.sql(songs_skew).toPandas().to_csv(output_data+"songs_skew.csv", header=True, index=False, mode="w")
    spark.sql(time_skew).toPandas().to_csv(output_data+"time_skew.csv", header=True, index=False, mode="w")


def main():
    """
    - Create Spark session on Amazon EMR
    - Load song JSON data from Udacity S3 bucket, process data, and write to analytics tables in my S3 bucket (in parquet file format)
    - Load log JSON data from  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    - Read the 5 analytics tables back into Spark
    - Print some test QC statements into a log file to ensure the tables look as expected
    - Run some SQL queries and save as CSV files
    """
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    # Parquet files destination:
    # output_data = "hadoop-test/" # TEMPORARILY write to EMR HDFS storage to save S3 costs
    output_data = "s3://erops-udacity/spark-data-lake/parquet-tables/"
    # CSV query results are written to EMR Local because Pandas does that instead of writing to EMR HDFS
    output_queries = "./queries/"
    songs_df = process_song_data(spark=spark, input_data=input_data, output_data=output_data)
    process_log_data(spark=spark, input_data=input_data, output_data=output_data, songs_df=songs_df)
    # Get some QC info to ensure the data is what we expect (all written to EMR Local)
    quality_control(spark=spark, input_data=output_data, output_data=output_queries, print_nrows=10)

    spark.stop()


if __name__ == "__main__":
    main()
