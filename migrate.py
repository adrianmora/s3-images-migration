#!/usr/bin/env python

import sys
import os
import logging
import argparse
import psycopg2
import boto3
from config import *
import threading
import botocore
from time import sleep


def check_avatars_status(s3_conn, db_conn):
    try:
        cur = db_conn.cursor()
        cur.execute("SELECT id, path FROM images.avatars")
        print("Number of registered avatars: ", cur.rowcount)
        avatars_paths = cur.fetchall()
        
        # Initialize variables
        s3_legacy=s3_conn.Bucket(S3_LEGACY_BUCKET_NAME)
        s3_production=s3_conn.Bucket(S3_PRODUCTION_BUCKET_NAME)
        prod_avatars = 0
        legacy_avatars = 0
        prod_objects = 0
        legacy_objects = 0

        # Iterate over all avatars registered in the database
        for path in avatars_paths:
            if LEGACY_PREFIX in path[1]:
                legacy_avatars += 1
                try:
                    s3.Object(S3_LEGACY_BUCKET_NAME, path[1]).load()
                    legacy_objects += 1
                except botocore.exceptions.ClientError as e:
                    print("Object not found in S3 legacy bucket : ", path[1])
            else:
                prod_avatars += 1
                try:
                    s3.Object(S3_PRODUCTION_BUCKET_NAME, path[1]).load()
                    prod_objects += 1
                except botocore.exceptions.ClientError as e:
                    print("Object not found in S3 production bucket : ", path[1])
        
        # Report results and abort if inconsistencies are found
        print("-----------------------------------")
        print("Legacy avatars: ", legacy_avatars)
        print("Objects in S3 legacy bucket: ", legacy_objects)
        print("Production avatars: ", prod_avatars)
        print("Objects in S3 production bucket: ", prod_objects)
        print("-----------------------------------")

        if (legacy_avatars != legacy_objects or prod_avatars != prod_objects):            
            return 1
        else:
            return 0
    except Exception as e:
        logging.error(f"Error checking avatars status : {e}")
        sys.exit(1)

def move_avatar(s3_conn, db_conn, row_id, path):
    try:
        move_s3_object(s3_conn, path)
        update_db_row(db_conn, row_id, path)
    except Exception as e:
        logging.error(f"Error during the migration process : {e}")
        sys.exit(1)

def check_db_rows(db_conn):
    try:
        cur = db_conn.cursor()
        cur.execute("SELECT id, path FROM images.avatars WHERE path LIKE %s", [LEGACY_PREFIX+'%'])
        print("Avatars to migrate: ", cur.rowcount)
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
    parser.add_argument('-d', '--dry-run', action='store_true', help='performs a test without applying changes')
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
      

    # Perform checks to evaluate consistency between DB entries and S3 objects
    result = check_avatars_status(s3, conn)
    
    # Proceed only if checks are reported successful
    if result == 0:
        print("No inconsistencies detected. Migration can proceed!")
        # Start migration process
        if not args.dry_run:
            # Identify which avatars need to be migrated
            legacy_rows = check_db_rows(conn)
            # Move avatars from legacy S3 bucket to production S3 bucket and update Postgresql DB
            input("Press Enter to continue...")
            for avatar in legacy_rows:
                t = threading.Thread(target=move_avatar, name=avatar, args=(s3, conn, avatar[0], avatar[1]))
                t.start()
            sleep(5)
            print("--------------------")
            print("Migration completed!")
            print("--------------------")
            print("Running checks once again.")
            check_avatars_status(s3, conn)
        else:
            # Executing dry-run will only perform the check process
            print("Dry-run completed! Run script with no arguments to apply the migration process (python3 migrate.py)")
    else:
        print("Migration aborted! Inconsistencies have been found between database entries and S3 objects, please review before proceeding")

    sys.exit(0)
