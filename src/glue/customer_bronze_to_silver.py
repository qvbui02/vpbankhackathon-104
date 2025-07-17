import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, trim, lower, to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Load customer table from Glue Catalog
dyf_customer = glueContext.create_dynamic_frame.from_catalog(
    database="bronze_vpbank_db",
    table_name="customer"
)

# Step 2: Convert to DataFrame for validation and cleaning
df_customer = dyf_customer.toDF()

# Step 3: Basic validation - drop rows with null u_id, email, or dob
df_clean = df_customer.filter(
    (col("u_id").isNotNull()) &
    (col("email").isNotNull()) &
    (col("dob").isNotNull())
)

# Step 4: Standardization
df_clean = df_clean.withColumn("email", lower(trim(col("email")))) \
                   .withColumn("gender", lower(trim(col("gender")))) \
                   .withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))

# Step 5: Repartition by partition keys
df_clean = df_clean.repartition("year", "month")

# Step 6: Convert back to DynamicFrame
dyf_clean = DynamicFrame.fromDF(df_clean, glueContext, "dyf_clean")

# Step 7: Write to Silver S3 location in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=dyf_clean,
    connection_type="s3",
    connection_options={
        "path": "s3://vpbank-silver/customer/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet"
)

job.commit()