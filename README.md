# Udacity-DEND-P3

### Project: Data Warehouse on AWS
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a data engineer, I'm tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, 
and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

### Project Description
In this project, I'll apply what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. 
To complete the project, I will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

### Project Datasets
I'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

### Projects files

- **create_table.py** is where I'll create fact and dimension tables for the star schema in Redshift.
- **etl.py** is where I'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- **sql_queries.py** is where I'll define you SQL statements, which will be imported into the two other files above.
