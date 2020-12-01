import pandas as pd
from datetime import date
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

def reduceMTA(df, year):
    df = df.select('C/A', 'UNIT', 'SCP', 'DATE', 'TIME', 'ENTRIES')
    df = df.withColumn('date2', to_date(unix_timestamp('DATE', 'MM/dd/yyyy').cast('timestamp')))

    # get difference bewteen data
    column_list = ['C/A', 'UNIT', 'SCP']
    win_spec = Window.partitionBy([col(x) for x in column_list]).orderBy(['DATE', 'TIME'])
    df = df.withColumn("entry_diff", df.ENTRIES - lag("ENTRIES", 1).over(win_spec))

    # clean data
    if year == 2019:
        start_date, end_date = date(2019, 3, 1), date(2019, 10, 31)
    elif year == 2020:
        start_date, end_date = date(2020, 3, 1), date(2020, 10, 31)
    mask = (df['date2'] >= start_date) & (df['date2'] <= end_date) & (df['entry_diff'] > 0) & (df['entry_diff'] < 6000)
    df = df[mask]

    # output result
    df = df.groupBy("date2").agg(sum("entry_diff").alias('count'))

    return df

# read & select data
# import covid-19 positive data
covid = pd.read_csv('/home/wx650/project/data/ny-covid19-positive/ny-covid19-positive.csv')[['date', 'positive', 'positiveIncrease']]

# mta data
df_19 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/mta/turnstile_2019.txt"))
df_20 = (spark.read.format("csv").options(header="true").load("/user/wx650/project/data/mta/turnstile_2020.txt"))

# process data
df_19 = reduceMTA(df_19, 2019).orderBy('date2')
df_20 = reduceMTA(df_20, 2020)


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
df_20 = df_20.join(scovid, df_20['date2'] == scovid['date']).select(scovid['date'], 'count', 'positive').orderBy('date')

# output result
df_19.select('*').write.save('project/output/mta_2019.out', format='csv')
df_20.select('*').write.save('project/output/mta_2020_covid.out', format='csv')
