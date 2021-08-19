# s3-images-migration

Based on https://gist.github.com/gonzalosr/309f2274e5bcce04f0a789d69faa7d60

## What is this?

A simple Python script intended to migrate objects between AWS S3 buckets as well as maintaining consistency of these objects with the use of an AWS RDS Postgresql database which stores S3 path references for each object 

## How to use it

1. Run `pip3 install -r requirements.txt` to install dependencies
2. Run `python3 migrate.py` to trigger the migration process

For a dry-run (only checks) add the following argument

1. Run `python3 migrate.py -d` or `python3 migrate.py --dry-run`

### Requirements

This script has been tested on an Amazon Linux machine (Linux Red Hat based), although the basic configurations will be similar on anuy Linux based machine

* For setting up the environment the following system packages have been required

`sudo yum install python3 python3-devel`

`sudo yum install postgresql postgresql-libs postgresql-devel gcc`

## Setting up the environment

The initial steps required to set up the environment before testing are the following:

1. To load S3 bucket and database with avatars obejects and references run `python seeder.py <num_avatars>`
2. Run the SQL scripts `schema.sql` and `user.sql` with a Psql based client

