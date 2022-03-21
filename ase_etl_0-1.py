#!/usr/bin/env python
''' 2016 Annual Survey of Entrepreneurs (ASE) Company Summary REST API ETL
This code extracts, transforms, and loads data from U.S. Census Bureaus' Annual Survey of Entrepreneurs' (ASE) 
2016 Company Summary REST API. It performs these tasks on both the main dataset and its nested resources.

This dataset contains information about employer businesses by sector, sex, ethnicity, race,
veteran status, years in business, receipts size of firm, and employment size of firm for the U.S.
'''

__author__ = 'Christina Coleman'
__email__ = 'cdc0816@gmail.com'
__copyright__ = 'Copyright 2022'
__date__ = '2022/03/18'
__deprecated__ = False
__license__ = 'MIT'
__maintainer__ = 'developer'
__version__ = '0.0.1'

import pandas as pd
import requests as r
import yaml
import sqlite3 as sql

def extract_data(conf_file):
    '''Set variables with configuration file then extract REST API data'''

    #Get variable settings from configuration file
    with open(conf_file) as file:
        conf = yaml.safe_load(file)
        print(conf)
        # Create string with attributes list
        attr_l = conf.get('attr')
        attributes = ','.join(str(a) for a in attr_l)
        print('Attributes: ', attributes)

        # Create string with measures list
        meas_l = conf.get('meas')
        measures = ','.join(str(a) for a in meas_l)
        print('Measures: ', measures)

        # Set API request url
        base_url = conf.get('base_url')
        api_param = conf.get('api_param')
        api_param = attributes + ',' + measures + api_param
        http_method = '?get='
        request_url = base_url + http_method + api_param
        assert isinstance(request_url, str)

        file.close()

    #Extract api data into dataframe
    response = r.get(request_url)

    if response.status_code == 200:
        print('Successful connection to ', request_url)
        data = response.json()
        meas_df = pd.DataFrame(data)
        rowcount = len(meas_df.index)
        print('Row Count: ' + str(rowcount))
    elif response.status_code == 404:
        print('404 Not Found for ', request_url)

    #Make second row header
    meas_df.columns = meas_df.iloc[0]
    meas_df = meas_df[1:]

    #Extract attribute lookups into dictionary
    attr_df = []
    for attr in attr_l:
        if attr == 'NAME': #This attribute value is included directly in the dataset
            continue
        else:
            attr_url = base_url+'/variables/'+attr+'.json'
            attr_data = r.get(attr_url).json()
            a_df = pd.DataFrame(attr_data)
            attr_df.append(a_df)

    attr_df = pd.concat(attr_df)
    return meas_df,attr_df

def transform_data(attr_df):
    '''Transform data by transposing attribute json to dataframes'''
    #For each attribute dataset, transpose json to dataframe
    attr_df = attr_df.reset_index()  # make sure indexes pair with number of rows
    attr_dict = {}
    for index,row in attr_df.iterrows():
        values = row['values']
        values_norm = pd.json_normalize(values)
        values_pivot = values_norm.melt()
        attr = row['name']
        attr_dict[attr] = values_pivot
    return attr_dict

def load_data(all_data):
    #Connect to database
    connection = sql.connect('database.db')
    cursor = connection.cursor()

    #Run SQL script to create tables
    sql_create_tables = open('create_tables.sql')
    sql_create_tables_str = sql_create_tables.read()
    cursor.executescript(sql_create_tables_str)

    #Insert into tables
    cursor.execute('delete from FACT_COMPANY_SUMMARY')
    cursor.executemany('insert into FACT_COMPANY_SUMMARY ('
                       'FACT_COMPANY_SUMMARY_KEY, STATE_NAME, DIM_NAICS_KEY, DIM_YEARS_IN_BUSINESS_KEY,'
                       'DIM_SALES_SIZE_KEY, DIM_EMPLOYMENT_SIZE_KEY, PAID_EMPLOYEES_CNT,'
                       'TOTAL_FIRM_EMPLOYEES_CNT, ANNUAL_PAYROLL_AMT, TOTAL_FIRM_SALES_SIZE_AMT) '
                       'values(?,?,?,?,?,?,?,?,?,?)', list(all_data['MEAS'].to_records(index=False)))

    cursor.execute('delete from DIM_NAICS')
    cursor.executemany('insert into DIM_NAICS (DIM_NAICS_KEY, NAICS_VALUE) values(?,?)'
                       , list(all_data['NAICS2012'].to_records(index=False)))

    cursor.execute('delete from DIM_YEARS_IN_BUSINESS')
    cursor.executemany('insert into DIM_YEARS_IN_BUSINESS (DIM_YEARS_IN_BUSINESS_KEY, YEARS_IN_BUSINESS_VALUE) values(?,?)'
                       , list(all_data['YIBSZFI'].to_records(index=False)))

    cursor.execute('delete from DIM_SALES_SIZE')
    cursor.executemany('insert into DIM_SALES_SIZE (DIM_SALES_SIZE_KEY, SALES_SIZE_VALUE) values(?,?)'
                       , list(all_data['RCPSZFI'].to_records(index=False)))

    cursor.execute('delete from DIM_EMPLOYMENT_SIZE')
    cursor.executemany('insert into DIM_EMPLOYMENT_SIZE (DIM_EMPLOYMENT_SIZE_KEY, EMPLOYMENT_SIZE_VALUE) values(?,?)'
                       , list(all_data['EMPSZFI'].to_records(index=False)))

    # Commit your changes in the database
    connection.commit()

    # Close the connection
    connection.close()
    return

def main():
    #Create dictionary to hold all data
    all_data = {}

    #Extract
    extract = extract_data(r'C:\Users\cdc81\PycharmProjects\Census_Demo\conf.yml')
    all_data['MEAS'] = extract[0]
    all_data['RAW_ATTR'] = extract[1]
    attr_df = all_data['RAW_ATTR']

    #Transform
    attr_dict = transform_data(attr_df)

    #Load
    all_data.update(attr_dict)
    load_data(all_data)

if __name__ == '__main__':
   main()