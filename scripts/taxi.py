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

# Clean & aggregate taxi data
def reduceTaxi(df, year, isYellow):

    # Find data ranging from 2020(19)-03-01 to 2020(19)-06-30 (exist dirty data like 2018-12-31)
    if year == 2019:
        start_date, end_date = date(2019, 3, 1), date(2019, 6, 30)
    elif year == 2020:
        start_date, end_date = date(2020, 3, 1), date(2020, 6, 30)
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    df = df[mask]

    if isYellow:
        # Group by date and agg on date count and average passenger_count
        df = df.groupBy('date').agg(count('date').alias('count'), avg('passenger_count').alias('avg_cnt'))
    else:
        df = df.groupBy('date').count()

    return df

# Import covid-19 positive data
covid = pd.read_csv('/home/wx650/project/data/ny-covid19-positive/ny-covid19-positive.csv')[['date', 'positive', 'positiveIncrease']]

# Import yellow taxi tripdata
# 2019.03 - 2019.06
yellow_1903 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/yellow_tripdata_2019-03.csv"))
yellow_1904 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/yellow_tripdata_2019-04.csv"))
yellow_1905 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/yellow_tripdata_2019-05.csv"))
yellow_1906 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/yellow_tripdata_2019-06.csv"))

yellow_19 = unionAll(yellow_1903, yellow_1904, yellow_1905, yellow_1906).select('tpep_pickup_datetime', 'passenger_count')
# add one column 'date'
yellow_19 = yellow_19.withColumn('date', (col('tpep_pickup_datetime').cast('date')))

# 2020.03 - 2020.06
yellow_2003 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/yellow_tripdata_2020-03.csv"))
yellow_2004 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/yellow_tripdata_2020-04.csv"))
yellow_2005 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/yellow_tripdata_2020-05.csv"))
yellow_2006 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/yellow_tripdata_2020-06.csv"))

yellow_20 = unionAll(yellow_2003, yellow_2004, yellow_2005, yellow_2006).select('tpep_pickup_datetime', 'passenger_count')
# add one column 'date'
yellow_20 = yellow_20.withColumn('date', (col('tpep_pickup_datetime').cast('date')))

# Import fhv tripdata
# 2019.03 - 2019.06
fhv_1903 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/fhv_tripdata_2019-03.csv"))
fhv_1904 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/fhv_tripdata_2019-04.csv"))
fhv_1905 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/fhv_tripdata_2019-05.csv"))
fhv_1906 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/fhv_tripdata_2019-06.csv"))

fhv_19 = unionAll(fhv_1903, fhv_1904, fhv_1905, fhv_1906).select('pickup_datetime')
# add one column 'date'
fhv_19 = fhv_19.withColumn('date', (col('pickup_datetime').cast('date')))

# 2020.03 - 2020.06
fhv_2003 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/fhv_tripdata_2020-03.csv"))
fhv_2004 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/fhv_tripdata_2020-04.csv"))
fhv_2005 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/fhv_tripdata_2020-05.csv"))
fhv_2006 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/taxi/fhv_tripdata_2020-06.csv"))

fhv_20 = unionAll(fhv_2003, fhv_2004, fhv_2005, fhv_2006).select('pickup_datetime')
# add one column 'date'
fhv_20 = fhv_20.withColumn('date', (col('pickup_datetime').cast('date')))


# Process data
# Reduce taxi
yellow_19 = reduceTaxi(yellow_19, 2019, True).orderBy('date')
yellow_20 = reduceTaxi(yellow_20, 2020, True)
fhv_19 = reduceTaxi(fhv_19, 2019, False).orderBy('date')
fhv_20 = reduceTaxi(fhv_20, 2020, False)

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

# Taxi data join with COVID datafram
yellow_20 = yellow_20.join(scovid, yellow_20['date'] == scovid['date']).select(yellow_20['date'], 'count', 'avg_cnt', 'positive').orderBy('date')
fhv_20 = fhv_20.join(scovid, fhv_20['date'] == scovid['date']).select(fhv_20['date'], 'count', 'positive').orderBy('date')

# Output result
yellow_19.select('*').write.save('project/output/yellow_2019.out', format='csv')
yellow_20.select('*').write.save('project/output/yellow_2020_covid.out', format='csv')
fhv_19.select('*').write.save('project/output/fhv_2019.out', format='csv')
fhv_20.select('*').write.save('project/output/fhv_2020_covid.out', format='csv')
