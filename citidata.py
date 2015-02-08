count = count+1
#Get the Citibike Data
import requests
r = requests.get('http://www.citibikenyc.com/stations/json')

#Clean Data
key_list = [] #unique list of keys for each station listing
for station in r.json()['stationBeanList']:
	for k in station.keys():
       		if k not in key_list:
       			key_list.append(k)

from pandas.io.json import json_normalize

df = json_normalize(r.json()['stationBeanList'])

#Explore the other data variables. Are there any test stations? 
print "The number of test stations is", len(df[df['testStation'] == True].values)

#How many stations are "In Service"? How many are "Not In Service"? 
print "The number of stations in service is", len(df[df['statusValue'] == 'In Service'].values)
print "The number of stations not in service is", len(df[df['statusValue'] == 'Not In Service'].values)

#Any other interesting variables values that need to be accounted for?

import numpy as np

#What is the mean number of bikes in a dock? What is the median? 
print "The mean number of bikes in a dock is", np.mean(df.availableBikes)
print "The median number of bikes in a dock is", np.median(df.availableBikes) 

#How does this change if we remove the stations that aren't in service?
print "The mean number of bikes in a dock in stations that are in service is", np.mean(df.availableBikes [df.statusValue == 'In Service'])
print "The median number of bikes in a dock in stations that are in service is", np.median(df.availableBikes [df.statusValue == 'In Service'])


#Storing to a SQL DataBase
import sqlite3 as lite

con = lite.connect('citi_bike.db')
cur = con.cursor()


#We will first create a stationary reference table that will not change with time.
with con:
	cur.execute('DROP TABLE IF EXISTS citibike_reference')
	cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT );')
	

sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
with con:
	cur = con.cursor()
	for station in r.json()['stationBeanList']:
       		cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

#We will extract the column station id and convert it into a data type for sqlite
station_ids = df['id'].tolist()

station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create the table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE IF NOT EXISTS available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")

import time
from dateutil.parser import parse
import collections

exec_time = parse(r.json()['executionTime'])

with con:
	cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))

id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station

#loop through the stations in the station list
for station in r.json()['stationBeanList']:
	id_bikes[station['id']] = station['availableBikes']

#iterate through the defaultdict to update the values in the database
with con:
	for k, v in id_bikes.iteritems():
        	cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
