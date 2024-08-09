from pyspark.sql import SparkSession

# Initialize Spark session with Iceberg REST support
spark = SparkSession.builder \
    .appName("CSV to Iceberg") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-aws-bundle:1.6.0") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://localhost:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

# Read CSV file into DataFrame
csv_file_path = "s3a://mybucket/raw/username.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file_path)

# Write DataFrame to Iceberg table (create the table if it doesn't exist)
df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .save("demo.warehouse.user2")

# Stop the Spark session
spark.stop()

