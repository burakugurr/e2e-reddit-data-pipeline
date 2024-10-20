from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from pyspark.sql.functions import date_format
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType,StringType, FloatType
import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import DateType


def create_spark():
    full_path_to_warehouse = 's3a://warehouse'
    # Bessie için kullanacağımız branch 
    branch_name = "main"
    # Nessie authentication türü. Diğer seçenekler (NONE, BEARER, OAUTH2 or AWS)
    auth_type = "NONE"
    # AWS S3 yerine MinIO kullandığımız için. Spark'a amazona gitme burada kal demek için.
    s3_endpoint = "http://192.168.89.83:9000"
    # MinIO'ya erişim için. Bunlar root olarak docker-compose içinde belirtiliyor. Bu haliyle canlı ortamlarda kullanılmamalıdır.
    accessKeyId='dr6MxGKaZrSAh77gP8T0'
    secretAccessKey='czrPTk1rsUMzebG1UlJ0kki5VeGZPUYR5iIFX0af'
    nessie_url = "http://192.168.89.83:19120/api/v1"

    spark = (
        SparkSession.builder
        .master("spark://192.168.89.83:7077")
        .appName("Spark Unıty Iceberg Demo")
        .config("spark.driver.memory", "16g")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .config('spark.jars.packages','org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.4.0')
        .config("spark.hadoop.fs.s3a.access.key", accessKeyId)
        .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey)
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Spark Amazon S3 varsayılan API'sine değil lokaldeki MinIO'ya gitsin.
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("fs.s3a.connection.ssl.enabled", "false")
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')

        #Configuring Catalog
        .config("spark.sql.catalog.nessie.uri", nessie_url)
        .config("spark.sql.catalog.nessie.ref", branch_name)
        .config("spark.sql.catalog.nessie.authentication.type", auth_type)
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.warehouse", full_path_to_warehouse)
        .config("fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    return spark



def get_data(spark):
    schema = StructType([
        StructField("filename", StringType(), True),
        StructField("SCORE", FloatType(), True),
        StructField("SUBREDDIT", StringType(), True),
        StructField("ETL_DATE" ,StringType(), True),
        StructField("TITLE", StringType(), True),
        StructField("TEXT",StringType(), True),
        StructField("SUBREDDIT_TYPE",StringType(), True),
        StructField("SUBREDDIT_SUBSCRIBER_COUNT",IntegerType(), True),
        StructField("URL" ,StringType(), True),
        StructField("TS" ,StringType(), True),
        StructField("VOTE_RATIO",FloatType(), True),
        StructField("TOPIC",StringType(), True),
        StructField("USER_NAME",StringType(), True),
        StructField("WLS",FloatType(), True)
        ])


    #df_parquet= spark.read.parquet("s3a://reddit/*.parquet", inferSchema=True, schema=schema)
    df= spark.read.parquet("s3a://reddit/*", inferSchema=True, schema=schema)
    return df

def get_most_write_user(df):
    most_write_user = df\
    .filter(df.USER_NAME.isNotNull())\
    .groupBy("USER_NAME")\
    .count()\
    .orderBy("count", ascending=False).coalesce(2)
    return most_write_user

def get_daily_message_df(df):
    daily_message_df = df.groupBy(date_format("ETL_DATE", "yyyy-MM-dd").alias("date")).count()
    return daily_message_df

def get_most_subscriber_per_day_df(df):
    most_subscriber_per_day_df = df.groupBy(date_format("ETL_DATE", "yyyy-MM-dd").alias("date"),
                                df.USER_NAME
                                ).count()
    return most_subscriber_per_day_df

def get_topic_count(df):
    topic_count = df.groupBy("topic").count()
    return topic_count

def get_avg_score_per_topic(df):
    avg_score_per_topic = df.groupBy("topic").avg("SCORE").withColumnRenamed("avg(SCORE)", "AVG_SCORE")
    return avg_score_per_topic


def runner():
    CATALOG_NAME = "nessie"
    DB_NAME = "reddit"
    
    spark = create_spark()
    df = get_data(spark)

    most_write_user = get_most_write_user(df)
    
    daily_message_df = get_daily_message_df(df)

    most_subscriber_per_day_df = get_most_subscriber_per_day_df(df)

    topic_count = get_topic_count(df)

    avg_score_per_topic = get_avg_score_per_topic(df)
    
    # unpartitioned tables
    avg_score_per_topic.write.mode("append").insertInto(f"{CATALOG_NAME}.{DB_NAME}.avg_score_per_topic") 

    topic_count.write.mode("append").insertInto(f"{CATALOG_NAME}.{DB_NAME}.topic_count") 

    #partitioned tables

    most_subscriber_per_day_df\
    .withColumn("date",most_subscriber_per_day_df['date'].cast(DateType()))\
    .orderBy("date")\
    .writeTo(f"{CATALOG_NAME}.{DB_NAME}.most_subscriber_per_day_withpartitions").append()

    most_write_user.orderBy("USER_NAME")\
        .write.mode("append").insertInto(f"{CATALOG_NAME}.{DB_NAME}.most_write_user_withpartitions") 
    daily_message_df.orderBy("date")\
    .withColumn("date",daily_message_df['date'].cast(DateType()))\
    .orderBy("date")\
        .write.mode("append").insertInto(f"{CATALOG_NAME}.{DB_NAME}.daily_message_withpartitions") 



with DAG(
    dag_id="Reddit_Statistics",
    description="Dag for statistics",
    start_date=datetime(2024, 10, 10),
    schedule_interval="0 1 * * *",
    catchup=False
) as dag:
    
    task_python = PythonOperator(
        task_id="Statistic_Update",
        python_callable=runner,
        provide_context=True  
    )
    # Set task dependencies
    task_python

    print("DAG DONE")