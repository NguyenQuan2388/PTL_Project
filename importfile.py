from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MinIO to Iceberg") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,org.apache.iceberg:iceberg-aws-bundle:1.4.1") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://localhost:8181") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse") \
    .getOrCreate()
    
#
# Read CSV file from MinIO
csv_file_path = "s3a://mybucket/raw/customers-100.csv"

df = spark.read.format("csv").option("header", "true").load(csv_file_path)

df.show()

df.write.format("iceberg").mode("append").save("demo.dwh.user2")

# Stop the SparkSession
spark.stop()

