# Exact Assignment for Data Engineer

from flask import Flask, request, jsonify, render_template
from flask_restful import Resource, Api
import pymysql

""" Serve insights via a REST API"""

# Create Flask application instance
app = Flask(__name__)

# Set main page
@app.route("/") #For default route
def main():
    return render_template("index.html")

# Connect to database
api = Api(app)
mydb = pymysql.connect(host="localhost",
                       user="root",
                       database="nyc_yellow_taxi")
cursor = mydb.cursor()

# Identify in which DOLocationID has the highest tip rate per quarter
@app.route('/api/tip/2020/<int:quarter>/max', methods=['GET']) #/api/tip/2020/01//max
def maximum_tip(quarter: int):
    if request.method == "GET":
        cursor.execute("SELECT DOLocationID, pickup_quarter, tip_rate FROM nyc_yellow_taxi_trips WHERE tip_rate=(SELECT MAX(tip_rate) FROM nyc_yellow_taxi_trips WHERE pickup_quarter = %s)", (quarter))
        results = cursor.fetchall()
        return jsonify(DOLocationID=results[0][0], maxTipPercentage=round(results[0][2],1))

# Identify at which hour of the day the speed is the highest
@app.route('/api/speed/2020/<int:month>/<int:day>/max', methods=['GET']) #/api/speed/2020/01/01/max
def maximum_speed(month: int, day: int):
    if request.method == "GET":
        cursor.execute("SELECT pickup_month, pickup_day, pickup_hour, MAX(speed_mhrs) FROM nyc_yellow_taxi_trips WHERE pickup_month = %s AND pickup_day = %s GROUP BY pickup_hour", (month, day))
        results = cursor.fetchall()
        all_results = [{'hour': res[2], 'maxSpeed': round(res[3], 0)} for res in results]
        return jsonify(tripSpeeds=all_results)

# Run the application on localhost with port 8000
if(__name__ == "__main__"):
    app.run(host='localhost', port=8000, debug=True)