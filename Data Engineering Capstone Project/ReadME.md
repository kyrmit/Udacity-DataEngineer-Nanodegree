# Udacity Data Engineering Capstone Project

#### Project Summary
- This project aims to be able to answers questions on US immigration such as what are the most popular cities for immigration, what is the gender distribution of the immigrants, what is the visa type distribution of the immigrants, what is the average age per immigrant and what is the average temperature per month per city. We extract data from 3 different sources, the I94 immigration dataset of 2016, city temperature data from Kaggle and US city demographic data from OpenSoft. We design 4 dimension tables: Cities, immigrants, monthly average city temperature and time, and 1 fact table: Immigration. We use Spark for ETL jobs and store the results in parquet for downstream analysis.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

#### Scope 
- This project aims to be able to answers questions on US immigration such as what are the most popular cities for immigration, what is the gender distribution of the immigrants, what is the visa type distribution of the immigrants, what is the average age per immigrant and what is the average temperature per month per city. We extract data from 3 different sources, the I94 immigration dataset of 2016, city temperature data from Kaggle and US city demographic data from OpenSoft. We design 4 dimension tables: Cities, immigrants, monthly average city temperature and time, and 1 fact table: Immigration. We use Spark for ETL jobs and store the results in parquet for downstream analysis.

#### Describe and Gather Data 
* I94 immigration data comes from theUS National Tourism and Trade Office website. It is provided in SAS7BDAT format which is a binary database storage format.

The temperature data is a Kaggle data set that includes temperatures in cities around the world. It can be found here: https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data

###### Immigration Key Notes:

* i94yr = 4 digit year,
* i94mon = numeric month,
* i94cit = 3 digit code of origin city,
* i94port = 3 character code of destination USA city,
* arrdate = arrival date in the USA,
* i94mode = 1 digit travel code,
* depdate = departure date from the USA,
* i94visa = reason for immigration, The temperature data set comes from Kaggle. It is in csv format.,

###### Temperature Key Notes:

* AverageTemperature = average temperature,
* City = city name,
* Country = country name,
* Latitude= latitude,
* Longitude = longitude 
### Conceptual Data Model

We follow a star schema data model to first create the staging tables and then create the Facts and Dimensions table

Here are the tables of the schema:

## Staging Tables
### staging_i94_df 
   - id
   - date
   - city_code
   - state_code
   - age
   - gender
   - visa_type
   - count

### staging_temp_df
   - year
   - month
   - city_code
   - city_name
   - avg_temperature
   - lat
   - long

### staging_demo_df
   - city_code
   - state_code
   - city_name
   - median_age
   - pct_male_pop
   - pct_female_pop
   - pct_veterans
   - pct_foreign_born
   - pct_native_american
   - pct_asian
   - pct_black
   - pct_hispanic_or_latino
   - pct_white
   - total_pop
    
## Dimension Tables
### immigrant_df
   - id
   - gender
   - age
   - visa_type

### city_df
   - city_code
   - state_code
   - city_name
   - median_age
   - pct_male_pop
   - pct_female_pop
   - pct_veterans
   - pct_foreign_born
   - pct_native_american
   - pct_asian
   - pct_black
   - pct_hispanic_or_latino
   - pct_white
   - total_pop
   - lat
   - long

### monthly_city_temp_df
   - city_code
   - year
   - month
   - avg_temperature
    
### time_df
   - date
   - dayofweek
   - weekofyear
   - month
    
## Fact Table
### immigration_df
   - id
   - state_code
   - city_code
   - date
   - count
   
#### Steps necessary to pipeline the data into the chosen data model

1. Clean the data on nulls, data types, duplicates, etc
2. Load staging tables for staging_i94_df, staging_temp_df and staging_demo_df
3. Create dimension tables for immigrant_df, city_df, monthly_city_temp_df and time_df
4. Create fact table immigration_df with information on immigration count, mapping id in immigrant_df, city_code in city_df and monthly_city_temp_df and date in time_df ensuring referential integrity
5. Save processed dimension and fact tables in parquet for downstream query

#### Discussions
Spark is chosen for this project as it is known for processing large amount of data fast (with in-memory compute), scale easily with additional worker nodes, with ability to digest different data formats (e.g. SAS, Parquet, CSV), and integrate nicely with cloud storage like S3 and warehouse like Redshift.

The data update cycle is typically chosen on two criteria. One is the reporting cycle, the other is the availabilty of new data to be fed into the system. For example, if new batch of average temperature can be made available at monthly interval, we might settle for monthly data refreshing cycle.

There are also considerations in terms of scaling existing solution.
* **If the data was increased by 100x:**
We can consider spinning up larger instances of EC2s hosting Spark and/or additional Spark work nodes. With added capacity arising from either vertical scaling or horizontal scaling, we should be able to accelerate processing time.

* **If the data populates a dashboard that must be updated on a daily basis by 7am every day:**
We can consider using Airflow to schedule and automate the data pipeline jobs. Built-in retry and monitoring mechanism can enable us to meet user requirement.

* **If the database needed to be accessed by 1000+ people:**
We can consider hosting our solution in production scale data warehouse in the cloud, with larger capacity to serve more users, and workload management to ensure equitable usage of resources across users.