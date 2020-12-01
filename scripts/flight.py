import pandas as pd
import numpy as np
from datetime import date
import pyspark
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# Function to union spark dataframes
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def reduceFlight(df):
    # Get data related to JFK Airport
    df = df[(df['destination'] == 'KJFK') | (df['origin'] == 'KJFK') | (df['destination'] == 'KLGA') | (df['origin'] == 'KLGA') | (df['destination'] == 'KEWR') | (df['origin'] == 'KEWR')].select('day', 'origin', 'destination')

    # Group by column 'day' and count number of flights, then transform date column
    df = df.groupBy('day').count()
    df = df.withColumn('date', (col('day').cast('date'))).select('date', 'count')

    return df

# import data
# 2019
data_1903 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20190301_20190331.csv"))
data_1904 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20190401_20190430.csv"))
data_1905 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20190501_20190531.csv"))
data_1906 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20190601_20190630.csv"))
data_1907 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20190701_20190731.csv"))
data_1908 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20190801_20190831.csv"))
data_1909 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20190901_20190930.csv"))
data_1910 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20191001_20191031.csv"))

df_19 = unionAll(data_1903, data_1904, data_1905, data_1906, data_1907, data_1908, data_1909, data_1910)

# 2020
data_2003 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20200301_20200331.csv"))
data_2004 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20200401_20200430.csv"))
data_2005 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20200501_20200531.csv"))
data_2006 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20200601_20200630.csv"))
data_2007 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20200701_20200731.csv"))
data_2008 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20200801_20200831.csv"))
data_2009 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20200901_20200930.csv"))
data_2010 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/flight/flightlist_20201001_20201031.csv"))

df_20 = unionAll(data_2003, data_2004, data_2005, data_2006, data_2007, data_2008, data_2009, data_2010)

# covid-19 positive
covid = pd.read_csv('/home/wx650/project/data/ny-covid19-positive/ny-covid19-positive.csv')[['date', 'positive', 'positiveIncrease']]

# process data
# reduce flight
df_19 = reduceFlight(df_19).orderBy('date')
df_20 = reduceFlight(df_20)

# Adding data of March 1st to covid dataset
covid.loc[271] = ['2020-03-01', 0, 0]

# Convert string to date
covid['date'] = pd.to_datetime(covid['date']).dt.date

# Find data ranging from 2020-03-01 to 2020-10-31
start_date, end_date = date(2020, 3, 1), date(2020, 10, 31)
mask = (covid['date'] >= start_date) & (covid['date'] <= end_date)
covid = covid[mask].sort_values(by='date')

# Spark COVID-19 datafram from covid
scovid = spark.createDataFrame(covid)

# Join with COVID dataframe
df_20 = df_20.join(scovid, df_20['date'] == scovid['date']).select(df_20['date'], 'count', 'positive').orderBy('date')

# output result
df_19.select('*').write.save('project/output/flight_2019.out', format='csv')
df_20.select('*').write.save('project/output/flight_2020_covid.out', format='csv')
