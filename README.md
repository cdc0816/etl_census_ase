# Simple Python ETL Using a REST API Data Source
## Introduction
This project demonstrates a simple extract, transform, and load (ETL) process that uses Python ([ase_etl_0-1.py](https://github.com/cdc0816/etl_census_ase/blob/main/ase_etl_0-1.py)), SQL ([create_tables.sql](https://github.com/cdc0816/etl_census_ase/blob/main/create_tables.sql)), and YAML configuration ([conf.yml](https://github.com/cdc0816/etl_census_ase/blob/main/conf.yml)) to build a data pipeline from the 2016 Annual Survey of Entrepreneurs (ASE) Company Summary REST API. It performs these tasks on both the main dataset and its nested resources, extracting the dataset measures and attributes specified in a YAML configuration file, transforming the data by normalizing and unpivoting JSON objects, and loading the data into a SQLLite relational database. 

## Problem Overview
REST APIs are a popular method for exposing data given their ease of use with the JSON data format, security advantages, broad programming language compatibility, and more. They are accessible by most popular programming languages, including Python. This popular language is also a good choice for performing data ETL as it provides many widely-known libraries for working with data, such as pandas. Configuration-driven ETL is a best practice as it protects the core code base from changes. Although there are many different ways to hold configurations, YAML files are simple, user-friendly, and easy to read even for non-developers. Loading the data into a relational database allows the use of structured query language (SQL) to access this data, among other benefits. This code uses a small, self-contained SQLLite3 database.

## Dimensional Model
This REST API contains nested resources for the attributes, so it's helpful to load them into separate tables (e.g. dimensions) and create database reference keys (e.g. primary and foreign keys) that will allow the main table that contains the measures (e.g. fact table) to join to the dimensions via these keys. Dimensional models allow for easy query writing and reporting. For simplicity, the database tables in this example are Type I, meaning they are truncated and loaded with data every time and reflect the "current state" of the data. 

Here is a database diagram of all tables and their relationships:

![ASE Rest API Database Diagram](https://github.com/cdc0816/etl_census_ase/blob/main/ase_data_model.jpg)
    
## Pre-Requisites
### Python Libraries
    pip install pyaml
    pip install pandas
    pip install requests
    pip install pysqlite3

## Source Data
The ASE dataset contains information about employer businesses by sector, sex, ethnicity, race, veteran status, years in business, receipts size of firm, and employment size of firm for each state in the U.S. The API is free to use and does not require a key to access. It exposes data from the 50 largest metropolitan areas and aggregated data, but this demo does not extract at these levels of granularity. 

The attribute values are determined based on the person(s) owning majority stock or equity in the firm. When aggregating the data, please note that if a firm's owner(s) indicated more than one race or ethnicity, their results were included in each of the indicated race's estimates.

### References
* API Documentation: https://www.census.gov/programs-surveys/ase/technical-documentation/api.html
* ASE Survey Information: https://www.census.gov/programs-surveys/ase/about.html
