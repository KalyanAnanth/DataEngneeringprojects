import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Initialize Glue and Spark
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read file from S3 (CSV example)
s3_path = "s3://glue-etl-09142025/raw_data/titanic.csv"
#s3://glue-etl-09142025/raw_data/titanic.csv

df = spark.read.option("header", "true").csv(s3_path)

# Show data
df.show(10)

# (Optional) Write back to S3 in Parquet
df.write.mode("overwrite").parquet("s3://glue-etl-09142025/enrich_data/titanic_results.parquet")
#s3://glue-etl-09142025/enrich_data/