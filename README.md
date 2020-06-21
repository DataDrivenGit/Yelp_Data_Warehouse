## Data Engineering Nanodegree - Capstone Project
### Purpose
This is the final project for the Data Engineer Nanodegree. Udacity gives us the option to use their suggested project or pick one dataset and scope it by ourselves. In my case I went for the second option. The dataset I will use on this project is from a service called Yelp, which basically stores business reviews given by customers.

## Datasets
The dataset was found on [Kaggle](https://www.kaggle.com/yelp-dataset/yelp-dataset) and was uploaded by Yelp team for a competition called Yelp Dataset Challenge which they were looking users to analyse their data and find interesting patterns or insights using NLP techniques (sentiment analysis for instance) and graph mining.

According to the description, in total there are:

8,021,122 user reviews
Information on 209,393 businesses

### Source Files
I have used 3 Json and one CSV files as a data source:

yelp_academic_dataset_business.json
yelp_academic_dataset_review.json
yelp_academic_dataset_tip.json
yelp_academic_dataset_user.csv

I eventually pre-process yelp_academic_dataset_user.json to make it a csv file, as the project request at least two different files format.

### Storage
The files were uploaded to a S3 bucket, which is open for access. The total space utilised on that bucket is approximately 8 gb, which is a considerable amount of data.

Project Scope
The scope of this project is to read data from Amazon S3 and load it on Amazon Redshift, later process the data in order to create dimensions and facts.

Finally some data quality checks are applied.

## Tooling
The tools utilised on this project are the same as we have been learning during the course of this Nanodegree.

**Amazon S3** for File Storage
**Amazon Redshift** for Data Storage

Those tools are widely utilised and considered industry standards. The community is massive and the tools provide support to several features.

## Fact/Dim

1. **fact_review** - have information about reviews and ratings (it has foreign key to all 4 dimention table)

2. **fact_tip** - have information about tip and complements (it has foreign key to all 4 dimention table)

3. **dim_user** stores information about the users.

4. **dim_datetime** has information about datetimes. It makes easier to process aggregation by time.

5. **dim_location** stores information about city, state and postal codes for aggrigating

6. **dim_business** has information about the business that receive reviews or tips by Yelp users.

## Scenarios
The following scenarios were requested to be addressed:

The data was increased by 100x. That wouldn't be a technical issue as both Amazon tools are commonly utilised in huge amount of data. Eventually the Redshift cluster would have to grow.

The pipelines would be run on a daily basis by 7 am every day. That's perfectly plausible and could be done utilising Airflow DAG· definitions.

The database needed to be accessed by 100+ people. That wouldn't be a problem as Redshift is highly scalable.

## ETL Pipeline

**create_tables.py** drops and creates the tables, which needs to be run before the ETL scripts to reset the tables.

**etl.py** loads the data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

**sql.py** contains all the sql queries, and is imported into the two files above.

**README.md** provides documentation on the project.

## How to Run pipeline.
1. create_tables.py  as $ python create_tables.py
2. etl.py as $ python etl.py