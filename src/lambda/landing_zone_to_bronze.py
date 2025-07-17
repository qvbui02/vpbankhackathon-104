import boto3
import urllib.parse
import re
import logging

# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # You can set to DEBUG for more detail

s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Received event: %s", event)
    
    for record in event['Records']:
        src_bucket = record['s3']['bucket']['name']
        src_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        filename = src_key.split('/')[-1]

        logger.info("Processing file: %s from bucket: %s", filename, src_bucket)

        match = re.match(r"([A-Za-z0-9_]+)_(\d{2})(\d{2})(\d{4})\.json$", filename)
        if not match:
            logger.warning("Skipped invalid filename: %s", filename)
            continue

        table, day, month, year = match.groups()
        month_no_trailling_zeros = str(int(month))

        dest_key = f"{table}/year={year}/month={month_no_trailling_zeros}/{filename}"

        try:
            logger.info("Copying %s to bucket vpbank-bronze with key %s", src_key, dest_key)
            s3.copy_object(
                Bucket='vpbank-bronze',
                CopySource={'Bucket': src_bucket, 'Key': src_key},
                Key=dest_key
            )
            logger.info("Successfully copied %s", filename)

            logger.info("Deleting %s from bucket %s", src_key, src_bucket)
            s3.delete_object(Bucket=src_bucket, Key=src_key)
            logger.info("Successfully deleted %s", filename)

        except Exception as e:
            logger.error("Error processing %s: %s", filename, str(e), exc_info=True)