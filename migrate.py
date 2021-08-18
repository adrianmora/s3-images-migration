#!/usr/bin/env python

import sys
import os
import logging
import argparse
import psycopg2
import boto3
from config import *

def move_avatars(s3_conn, db_conn, legacy_rows):
    try:
        for row in legacy_rows:
            move_s3_object(s3_conn, row[1])
            update_db_row(db_conn, row[0], row[1])
    except Exception as e:
        logging.error(f"Error during the migration process : {e}")
        sys.exit(1)

def check_db_rows(db_conn):
    try:
        cur = db_conn.cursor()
        cur.execute("SELECT id, path FROM images.avatars WHERE path LIKE %s LIMIT 250", [LEGACY_PREFIX+'%'])
        print("Number of legacy avatars: ", cur.rowcount)
        legacy_paths = cur.fetchall()
        return legacy_paths
        cur.close()
    except Exception as e:
        logging.error(f"Error running through rows in the database: {e}")
        sys.exit(1)

def update_db_row(db_conn, row_id, path):
    try:
        # Update the prefix from image/ to avatar/
        new_path = path.replace(LEGACY_PREFIX, PRODUCTION_PREFIX)
        cur = db_conn.cursor()
        cur.execute("UPDATE images.avatars SET path = %s WHERE id = %s", (new_path, row_id))
        db_conn.commit()
        print('Database entry for ' + path + ' updated to ' + new_path)
    except Exception as e:
        logging.error(f"Error updating paths in the database: {e}")
        sys.exit(1)

def list_s3_objects(s3_conn, bucket, prefix):
    try:
        s3_bucket = s3_conn.Bucket(bucket)
        for s3_bucket_object in s3_bucket.objects.filter(Prefix=prefix):
            print(s3_bucket_object.key)
    except Exception as e:
        logging.error(f"Error while listing s3 objects: {e}")
        sys.exit(1)

def move_s3_objects(s3_conn):
    try:
        s3_legacy=s3_conn.Bucket(S3_LEGACY_BUCKET_NAME)
        s3_production=s3_conn.Bucket(S3_PRODUCTION_BUCKET_NAME)

        # Iterate over all the objects under the Legacy bucket prefix
        for s3_bucket_object in s3_legacy.objects.filter(Prefix=LEGACY_PREFIX):
            copy_source = {
                'Bucket': S3_LEGACY_BUCKET_NAME,
                'Key': s3_bucket_object.key
            }
            new_key=s3_bucket_object.key.replace(LEGACY_PREFIX, PRODUCTION_PREFIX)
            s3_production.copy(copy_source, new_key)

            # then delete the original object after copying to destination bucket
            s3_bucket_object.delete()
            print('s3://'+ S3_LEGACY_BUCKET_NAME + '/'+ s3_bucket_object.key +' moved to s3://'+ S3_PRODUCTION_BUCKET_NAME +'/'+ new_key)
    except Exception as e:
        logging.error(f"Error while moving objects between s3 buckets: {e}")
        sys.exit(1)

def move_s3_object(s3_conn, path):
    try:
        s3_legacy=s3_conn.Bucket(S3_LEGACY_BUCKET_NAME)
        s3_production=s3_conn.Bucket(S3_PRODUCTION_BUCKET_NAME)

        copy_source = {
            'Bucket': S3_LEGACY_BUCKET_NAME,
            'Key': path
        }
        new_key=path.replace(LEGACY_PREFIX, PRODUCTION_PREFIX)
        s3_production.copy(copy_source, new_key)

        s3_legacy.objects.filter(Prefix=path).delete()
        print('s3://'+ S3_LEGACY_BUCKET_NAME + '/'+ path +' moved to s3://'+ S3_PRODUCTION_BUCKET_NAME +'/'+ new_key)
    except Exception as e:
        logging.error(f"Error while moving object to S3 production bucket: {e}")
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
    #dry_run = False
    #list_s3_objects(s3, S3_LEGACY_BUCKET_NAME, LEGACY_PREFIX)
    #list_s3_objects(s3, S3_PRODUCTION_BUCKET_NAME, PRODUCTION_PREFIX)
    #move_s3_objects(s3)
    legacy_rows = check_db_rows(conn)
    move_avatars(s3, conn, legacy_rows)
    print("Migration completed!")
