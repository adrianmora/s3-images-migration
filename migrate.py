#!/usr/bin/env python

import sys
import os
import logging
import argparse
import psycopg2
import boto3
from config import *


def list_s3_objects(s3_conn, bucket, prefix):
    try:
        s3_bucket=s3_conn.Bucket(bucket)
        for s3_bucket_object in s3_bucket.objects.filter(Prefix=prefix):
            print(s3_bucket_object.key)
    except Exception as e:
        logging.error(f"Error while listing s3 objects: {e}")
        sys.exit(1)


def move_s3_objects(s3_conn, bucket_legacy, bucket_prod, prefix_legacy, prefix_prod):
    try:
        s3_legacy=s3_conn.Bucket(bucket_legacy)
        s3_production=s3_conn.Bucket(bucket_prod)

        # Iterate over all the objects under the Legacy bucket prefix
        for s3_bucket_object in s3_legacy.objects.filter(Prefix=prefix_legacy):
            copy_source = {
                'Bucket': bucket_legacy,
                'Key': s3_bucket_object.key
            }
            new_key=s3_bucket_object.key.replace(prefix_legacy, prefix_prod)
            s3_production.copy(copy_source, new_key)

            # then delete the original object after copying to destination bucket
            s3_bucket_object.delete()
            print('s3://'+ bucket_legacy + '/'+ s3_bucket_object.key +' object moved to s3://'+ bucket_prod +'/'+ new_key)
    except Exception as e:
        logging.error(f"Error while moving objects between s3 buckets: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='This script moves avatars from legacy bucket to production bucket and updates the database references accordingly.')
    parser.add_argument('-d', '--dry-run', metavar='', help='performs a test without applying changes')
    args = parser.parse_args()
    
    # Connect to db
    try:
        conn = psycopg2.connect(DB_CONN_STRING)
    except Exception as e:
        logging.error(f"Error while connecting to the database: {e}")
        sys.exit(1)


    # Initialize s3 resource
    try:
        s3 = boto3.resource('s3',
                            endpoint_url=S3_ENDPOINT_URL,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_DEFAULT_REGION
                            )
    except Exception as e:
        logging.error(f"Error while connecting to S3: {e}")
        sys.exit(1)

    # Move avatars from legacy S3 bucket to production S3 bucket and update Postgresql DB
    #list_s3_objects(s3, S3_LEGACY_BUCKET_NAME, LEGACY_PREFIX)
    move_s3_objects(s3, S3_LEGACY_BUCKET_NAME, S3_PRODUCTION_BUCKET_NAME, LEGACY_PREFIX, PRODUCTION_PREFIX)

