import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyspark
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

def reducemta(year):
    # mta dataset
    if year == 2019:
        mta = pd.read_csv('/home/zw2315/project/turnstile_2019.txt', sep = ',',header = None)
    elif year == 2020:
        mta = pd.read_csv('/home/zw2315/project/turnstile_2019.txt', sep = ',',header = None)

    mta.columns =['C/A','unit','SCP','Station','Linename','division','date','time','desc','entries','exits']
    # process data
    mta['datetime'] = pd.to_datetime(mta.date + ' ' + mta.time, format='%m/%d/%Y  %H:%M:%S')
    mta['turnstile'] = mta['C/A'] + '-' + mta['unit'] + '-' + mta['SCP']
    mta_sorted = mta.sort_values(['turnstile', 'datetime'])
    mta_sorted = mta_sorted.reset_index(drop = True)

    # agg dataframe
    turnstile_grouped = mta_sorted.groupby(['turnstile'])
    mta_sorted['entries_diff'] = turnstile_grouped['entries'].transform(pd.Series.diff)
    mta_sorted['exits_diff'] = turnstile_grouped['exits'].transform(pd.Series.diff)

    # clean data 
    mta_sorted['entries_diff'] = mta_sorted['entries_diff'].fillna(0)
    mta_sorted['exits_diff'] = mta_sorted['exits_diff'].fillna(0)
    mta_sorted['entries_diff'][mta_sorted['entries_diff'] < 0] = 0 
    mta_sorted['exits_diff'][mta_sorted['exits_diff'] < 0] = 0 
    mta_sorted['entries_diff'][mta_sorted['entries_diff'] >= 6000] = 0 
    mta_sorted['exits_diff'][mta_sorted['exits_diff'] >= 6000] = 0

    # create spark dataframe
    mta_processed = mta_sorted[['turnstile','date', 'entries_diff']]
    mta_spark = spark.createDataFrame(mta_processed)
    mta_spark = mta_spark.groupBy("date").sum("entries_diff")

    # output result
    if year == 2019:
        mta_spark.select('*').write.save('mta_2019.out', format='csv')
    elif year == 2020:
        mta_spark.select('*').write.save('mta_2020.out', format='csv')

reducemta(2019)
reducemta(2020)