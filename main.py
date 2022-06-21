# Exact Assignment for Data Engineer

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import urllib.request
import os.path
import mysql.connector as mysql
from mysql.connector import Error

sc = SparkContext(master='local[2]')
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


""" Extract """

# Extract yellow cab data from the website https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
url_loc = {}  # Map download url to the file location
for m in range(1, 13):
    month = "{:02d}".format(m)
    file_name = 'yellow_tripdata_%s-%s.parquet' % (2020, month)
    url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/%s' % file_name
    loc = './data/%s' % file_name
    url_loc[url] = loc

# Download the files to the data directory in the current working space
file_loc = []
for url, loc in url_loc.items():
    file_loc.append(loc)
    if os.path.isfile(loc):
        continue
    urllib.request.urlretrieve(url, loc)

# Read in the data
df = spark.read.parquet(file_loc[0]).withColumn("airport_fee", col("airport_fee").cast("double"))

# There are differences in schema for column airport_fee.
# Thus, we transform the column to type double in each file
for i in list(range(1, 12)):
    df = df.union(spark.read.parquet(file_loc[i]).withColumn("airport_fee", col("airport_fee").cast("double")))


""" Transform """

# Drop duplicate rows
df = df.dropDuplicates()

# Drop column airport_fee as it misses most values
df = df.drop("airport_fee")

# Drop rows with missing values in the columns passenger_count, RatecodeID, store_and_fwd_flag
# and congestion_surcharge. These columns contain null values in the exact same rows.
df = df.where(df.passenger_count.isNotNull() & df.RatecodeID.isNotNull() & \
              df.store_and_fwd_flag.isNotNull() & df.congestion_surcharge.isNotNull())

# Calculate the duration of the trip in hours and remove rows with negative duration time
df = df.withColumn("duration_hrs",(unix_timestamp("tpep_dropoff_datetime") - unix_timestamp('tpep_pickup_datetime'))/3600)
df = df.where(df.duration_hrs > 0)

# Remove rows with 0 or fewer passengers
# Remove rows with a trip distance of 0 or lower
# Remove rows with PULocationID or DOLocationID outside of the range [1, 263]
# Fare amount should be at least $2.50 (initial charge)
# Remove rows with negative tip_amount, mta_tax, tolls_amount and extra
df = df.where(df.passenger_count > 0) \
    .where(df.trip_distance > 0) \
    .where((df.PULocationID.between(1,263)) & (df.DOLocationID.between(1,263))) \
    .where((df.fare_amount >= 2.50) & (df.total_amount >= 2.50)) \
    .where(df.tip_amount >= 0) \
    .where(df.mta_tax >= 0) \
    .where(df.tolls_amount >= 0) \
    .where(df.extra >= 0)

# Add columns with pickup year, month, weekday, hour and quarter
df = df.withColumn("pickup_year", expr("cast(year(tpep_pickup_datetime) as int)")) \
    .withColumn("pickup_month", expr("cast(month(tpep_pickup_datetime) as int)")) \
    .withColumn("pickup_day", expr("cast(day(tpep_pickup_datetime) as int)")) \
    .withColumn("pickup_dayOfWeek", date_format("tpep_pickup_datetime", "E")) \
    .withColumn("pickup_hour", expr("cast(hour(tpep_pickup_datetime) as int)")) \
    .withColumn("pickup_quarter", expr("cast(quarter(tpep_pickup_datetime) as int)"))

# Remove entries from dates other than 2020
df = df.where(df.pickup_year == 2020) \
    .drop("pickup_year")

# Add columns with speed and tip rate
df = df.withColumn("speed_mhrs", df.trip_distance/df.duration_hrs) \
    .withColumn("tip_rate", (df.tip_amount/df.total_amount)*100)

# Remove entries where the tip rate is equal to or more than 50% of the total amount
# Remove entries where the speed is equal to or exceeds 125 mph
df = df.where(df.tip_rate < 50) \
    .where(df.speed_mhrs < 125)

# This column is dropped because it continues to give an error when loading it into a database
df = df.drop("tpep_dropoff_datetime")

# Save output in PARQUET format
df.write \
    .format('parquet') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .save("./transformed_data")


""" Load """

# Load data into database nyc_yellow_taxi
try:
    connection = mysql.connect(host='localhost',
                               database='nyc_yellow_taxi',
                               user='root')

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
except Error as e:
    print("Error while connecting to MySQL", e)

# Check if the nyc_yellow_taxi_trips table is in the database and add if not.
cursor.execute("show tables;")
tables = cursor.fetchall()
if len([t for t in tables if "nyc_yellow_taxi_trips" in t]) == 0:
    df.write.format('jdbc').options(
        url='jdbc:mysql://localhost:3306/nyc_yellow_taxi',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='nyc_yellow_taxi_trips',
        user='root').mode('append').save()
connection.close()

