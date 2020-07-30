# Trial python program to read Evohome data from MQTT
import paho.mqtt.client as mqtt
import json
import datetime
from pymongo import MongoClient
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
user = config['DB_Credentials']['user']
password = config['DB_Credentials']['pass']
url = config['DB_Credentials']['url']
database = config['DB_Credentials']['database']
collection = config['DB_Credentials']['collection']

print (user)
print (password)
print (url)
print (database)
print (collection)

client = MongoClient("mongodb+srv://" + user + ":" + password + "@" + url + "/?retryWrites=true&w=majority")
db = client[database]
coll = db[collection]

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
 
    # Subscribing in on_connect() - if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("evohome/status/thermostat/#")
    # client.subscribe("evohome/status/thermostat/woonkamer")
 

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    payload = json.loads (msg.payload)
    temp = payload['val']
    timestampms = payload['lc']
    timestamp = timestampms / 1000
    readableDatetime = datetime.datetime.fromtimestamp(timestamp)
    reqTemp = payload['state']['changeableValues']['heatSetpoint']['value']
    reqStatus = payload['state']['changeableValues']['heatSetpoint']['status']
    reqUntil = payload['state']['changeableValues']['heatSetpoint']['nextTime']
    
    print("Topic: " + msg.topic)
    print ("Datetime: " + readableDatetime.strftime('%d-%m-%Y %H:%M:%S'))
    print ("Temp: " + str(temp))
    print ("Request Temp: " + str(reqTemp) + "\tuntil " + reqUntil + "\tStatus: " + reqStatus)

    rec = coll.find_one({"zone": msg.topic,
                          "date": readableDatetime.strftime('%d-%m-%Y')})
    if not rec:
        rec = coll.insert_one({"zone": msg.topic,
                          "date": readableDatetime.strftime('%d-%m-%Y')})
    
    coll.update_one({"zone": msg.topic, "date": readableDatetime.strftime('%d-%m-%Y')},
                           {"$push" : {"readings" : 
                           { "currentTemp": temp,"requestedTemp": reqTemp, "until" : reqUntil, "status" : reqStatus}}})


# Create an MQTT client and attach our routines to it.
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
 
client.connect("localhost", 1883, 60)
 
# Process network traffic and dispatch callbacks. This will also handle
# reconnecting. Check the documentation at
# https://github.com/eclipse/paho.m
# for information on how to use other loop*() functions
client.loop_forever()
