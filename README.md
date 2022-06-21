# Exact NYC Yellow Taxi 2020 Data Engineering Assignment

The goal of this assignment was to write an ETL process that is able to download all NYC Yellow Taxi data from 2020 from a webpage, transform the data and load it into a MySQL database. The transformations are written in PySpark and the data are loaded into a database using mysql-connector-python (main.py).

First, the data is cleaned to improve data integrity. Duplicate entries and entries with missing or erroneous values are removed, including:
  - pickup/drop-off date not in the selected period (2020).
  - zero passenger counts.
  - negative trip distance and total fare amount.

Subsequently, certain columns are transformed into useful features, such as:
  - duration of each trip in hours.
  - the month, day, weekday and hour of each trip.
  - the speed of the trip in miles per hour.
  - the tip rate.

  After the transformations, the data are saved in PARQUET files in a local directory (./transformed_data) and loaded into a database. A REST API (app.py) is then created to make some insights available to the consumer. The RESTful endpoint is created using Flask and pymysql, and is able to return two GET requests, including:
   - in which DOLocationID the tip rate is highest per quarter of 2020.
    GET /api/tip/2020/<int:quarter>/max
   - at which hour of the day the speed of the taxis is highest.
    GET /api/speed/2020/<int:month>/<int:day>/max
   
The requests were tested using Postman.