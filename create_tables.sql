drop table if exists FACT_COMPANY_SUMMARY;
create table FACT_COMPANY_SUMMARY (
    FACT_COMPANY_SUMMARY_KEY int,
    STATE_NAME varchar(52),
    DIM_NAICS_KEY char(6),
    DIM_YEARS_IN_BUSINESS_KEY char(3),
    DIM_SALES_SIZE_KEY char(3),
    DIM_EMPLOYMENT_SIZE_KEY char(3),
    PAID_EMPLOYEES_CNT int,
    TOTAL_FIRM_EMPLOYEES_CNT int,
    ANNUAL_PAYROLL_AMT int,
    TOTAL_FIRM_SALES_SIZE_AMT int,
    DW_INSERT_DTM timestamp(3) default current_timestamp
    )
;

drop table if exists DIM_NAICS;
create table DIM_NAICS (
    DIM_NAICS_KEY char(6),
    NAICS_VALUE varchar(300),
    DW_INSERT_DTM timestamp(3) default current_timestamp
    )
;

drop table if exists DIM_YEARS_IN_BUSINESS;
create table DIM_YEARS_IN_BUSINESS (
    DIM_YEARS_IN_BUSINESS_KEY char(3),
    YEARS_IN_BUSINESS_VALUE varchar(50),
    DW_INSERT_DTM timestamp(3) default current_timestamp
    )
;

drop table if exists DIM_SALES_SIZE;
create table DIM_SALES_SIZE (
    DIM_SALES_SIZE_KEY char(3),
    SALES_SIZE_VALUE varchar(75),
    DW_INSERT_DTM timestamp(3) default current_timestamp
    )
;

drop table if exists DIM_EMPLOYMENT_SIZE;
create table DIM_EMPLOYMENT_SIZE (
    DIM_EMPLOYMENT_SIZE_KEY char(3),
    EMPLOYMENT_SIZE_VALUE varchar(75),
    DW_INSERT_DTM timestamp(3) default current_timestamp
    )
;
