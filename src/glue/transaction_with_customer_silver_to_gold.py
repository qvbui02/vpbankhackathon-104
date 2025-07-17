import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, current_date, year, month, dayofmonth

# Job params
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Load DynamicFrames from Silver zone (S3)
dyf_transaction = glueContext.create_dynamic_frame.from_catalog(
    database="silver-vpbank-db",
    table_name="transaction"
)
dyf_account = glueContext.create_dynamic_frame.from_catalog(
    database="silver-vpbank-db",
    table_name="account"
)
dyf_customer = glueContext.create_dynamic_frame.from_catalog(
    database="silver-vpbank-db",
    table_name="customer"
)

# Step 2: Convert to DataFrames
df_transaction = dyf_transaction.toDF()
df_account = dyf_account.toDF()
df_customer = dyf_customer.toDF()

print("Original transactions count:", df_transaction.count())
print("Original accounts count:", df_account.count())
print("Original customers count:", df_customer.count())

# --------------------------
# Join Sender Side
# --------------------------
# Rename account columns for sender
df_sender = df_account.select(
    col("account_id").alias("account_sender_account_id"),
    col("u_id").alias("sender_u_id"),
    col("account_status").alias("sender_account_status"),
    col("created_date").alias("sender_account_created_date")
)

# Join transaction with sender account
df_txn_sender = df_transaction.join(
    df_sender,
    df_transaction["sender_account_id"] == df_sender["account_sender_account_id"],
    how="left"
)

print("After sender_account join:", df_txn_sender.count())

# Rename customer columns for sender
df_sender_cust = df_customer.select(
    col("u_id").alias("customer_sender_u_id"),
    col("first_name").alias("sender_first_name"),
    col("middle_name").alias("sender_middle_name"),
    col("last_name").alias("sender_last_name"),
    col("email").alias("sender_email"),
    col("number").alias("sender_phone")
)

df_sender_cust.show(2)

# Join with sender customer data
df_txn_sender = df_txn_sender.join(
    df_sender_cust,
    df_txn_sender["sender_u_id"] == df_sender_cust["customer_sender_u_id"],
    how="left"
)

print("After sender_customer join:", df_txn_sender.count())
# --------------------------
# Join Receiver Side
# --------------------------
# Rename account columns for receiver
df_receiver = df_account.select(
    col("account_id").alias("receiver_account_id"),
    col("u_id").alias("receiver_u_id"),
    col("account_status").alias("receiver_account_status"),
    col("created_date").alias("receiver_account_created_date")
    # Add other account columns you need, renaming them appropriately
)

# Join with receiver account
df_txn_both = df_txn_sender.join(
    df_receiver,
    df_txn_sender["receiver_account_id"] == df_receiver["receiver_account_id"],
    how="left"
).drop(df_receiver["receiver_account_id"])  # Drop duplicate column from receiver table

print("After receiver_account join:", df_txn_both.count())

# Rename customer columns for receiver
df_receiver_cust = df_customer.select(
    col("u_id").alias("receiver_u_id"),
    col("first_name").alias("receiver_first_name"),
    col("middle_name").alias("receiver_middle_name"),
    col("last_name").alias("receiver_last_name"),
    col("email").alias("receiver_email"),
    col("number").alias("receiver_phone") 
)

# Join with receiver customer data
df_txn_both = df_txn_both.join(
    df_receiver_cust,
    df_txn_both["receiver_u_id"] == df_receiver_cust["receiver_u_id"],
    how="left"
).drop(df_receiver_cust["receiver_u_id"])  # Drop duplicate column from receiver customer table

print("After receiver_customer join:", df_txn_both.count())

# Select final columns
df_final = df_txn_both.select(
    "transaction_id", "transaction_type", "status", "direction",
    "sender_account_id", "sender_u_id", 
    "sender_first_name", "sender_middle_name", "sender_last_name", 
    "sender_email", "sender_phone",
    "receiver_account_id", "receiver_u_id", 
    "receiver_first_name", "receiver_middle_name", "receiver_last_name", 
    "receiver_email", "receiver_phone",
    "sender_amount", "receiver_amount", "request_time", "complete_time"
)

df_final = df_final.withColumn("year", year(col("request_time")).cast("string")) \
                   .withColumn("month", month(col("request_time")).cast("string"))

# Step 5: Repartition by partition keys
df_final = df_final.repartition("year", "month")

# --------------------------
# Write to Gold Zone
# --------------------------
dyf_final = DynamicFrame.fromDF(df_final, glueContext, "dyf_final")

glueContext.write_dynamic_frame.from_options(
    frame=dyf_final,
    connection_type="s3",
    connection_options={
        "path": "s3://vpbank-gold/transactions_with_customers/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet"
)

job.commit()