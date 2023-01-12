import sqlite3
import requests
from tqdm import tqdm
from flask import Flask, request, render_template
import json 
import numpy as np
import pandas as pd

app = Flask(__name__)

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/') # by default all endpoints have method GET
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/average_duration')
def avg_dur_trips():
    conn = make_connection()
    res = get_avg_dur_trips(conn)['AvgDur']
    avg = res.values[0]
    return (f'''The average duration of bike riding is {avg:.2f} minutes per trip''')

@app.route('/trips/total_duration/<bike_id>')
def total_dur_bike_id(bike_id):
    conn = make_connection()
    res = get_total_dur_bike_id(bike_id, conn)['total']
    total = res.values[0]
    return (f'''The total riding duration of bike with id {bike_id} is {total} minutes''')

@app.route('/stations_summary', methods=['POST']) 
def stations_summary():
    """
    I'd like to make a summary of bike riding activities in each departure station
    by reporting the number of riding session (id) and the average duration of the riding session (duration_minutes).
    The thing is I could not make the json format into something like this
    {
        station1: {
            freq: x,
            avgDur: y
        },
        station2: {
            freq: x,
            avgDur: y
        },
    }
    """
    input_data = request.get_json() # Get the input as dictionary
    specified_date = input_data['year'] # Select specific items (period) from the dictionary (the value will be "2015-08")

    # Subset the data with query 
    conn = make_connection()
    query = f"SELECT * FROM trips WHERE start_time LIKE '{specified_date}%'"
    selected_data = pd.read_sql_query(query, conn, index_col='start_station_name')

    # Make the aggregate
    result = selected_data.groupby('start_station_name').agg({
        'id' : 'count', 
        'duration_minutes' : 'mean'
    })
    
    result.rename(columns = {'id':'Number of Riding', 'duration_minutes':'Average Duration'}, inplace = True)

    # Return the result
    return result.to_json(double_precision=2, indent=4)

@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

############ FUNCTIONS ############

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_avg_dur_trips(conn):
    query = f"""SELECT AVG(duration_minutes) AS AvgDur FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_total_dur_bike_id(bike_id, conn):
    query = f"""SELECT SUM(duration_minutes) AS total FROM trips WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

if __name__ == '__main__':
    app.run(debug=True, port=5000)