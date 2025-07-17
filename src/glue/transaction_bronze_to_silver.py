import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, lower, trim
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Load account table from Glue Catalog
dyf_transaction = glueContext.create_dynamic_frame.from_catalog(
    database="bronze_vpbank_db",
    table_name="transaction"
)

# Step 2: Convert to DataFrame
df_transaction = dyf_transaction.toDF()

# Step 3: Basic validation - drop rows with null account_id or u_id
df_clean = df_transaction.filter(
    (col("sender_bank_id").isNotNull()) &
    (col("sender_account_id").isNotNull()) &
    (col("receiver_bank_id").isNotNull()) &
    (col("receiver_account_id").isNotNull()) &
    (col("sender_currency").isNotNull()) &
    (col("receiver_currency").isNotNull()) &
    (col("sender_amount").isNotNull()) &
    (col("receiver_amount").isNotNull()) &
    (col("request_time").isNotNull()) &
    (col("complete_time").isNotNull()) &
    (col("description").isNotNull()) &
    (col("status").isNotNull()) &
    (col("transaction_type").isNotNull()) &
    (col("transaction_id").isNotNull()) &
    (col("direction").isNotNull())
)

# Step 4: Standardization
df_clean = df_clean.withColumn("status", lower(trim(col("status")))) \
                   .withColumn("request_time", to_timestamp(col("request_time"), "dd-MM-yyyy HH:mm:ss")) \
                   .withColumn("complete_time", to_timestamp(col("complete_time"), "dd-MM-yyyy HH:mm:ss"))

# Step 5: Repartition by partition keys
df_clean = df_clean.repartition("year", "month")

# Step 6: Convert back to DynamicFrame
dyf_clean = DynamicFrame.fromDF(df_clean, glueContext, "dyf_clean")

# Step 7: Write to Silver S3 location in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=dyf_clean,
    connection_type="s3",
    connection_options={
        "path": "s3://vpbank-silver/transaction/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet"
)

job.commit()