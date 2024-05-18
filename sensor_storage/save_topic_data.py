import sqlite3
import mysql.connector
import json
import os
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()
def getEnvVar(token_name):
    token = os.getenv(token_name)
    if token == None:
        print (f"Environment variable {token_name} not set. Exiting.")
        exit(1)
    return token

print("connecting to db and getting schema in place if not exists")
# SQLite database setup
conn = sqlite3.connect('IoT.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS MQTT_MESSAGES
             (timestamp DATETIME, topic TEXT, message TEXT)''')

print("reading environment variables")
# MQTT broker settings
broker_address = getEnvVar("MQTT_BROKER")
port = int(getEnvVar("MQQT_BROKER_PORT"))
username = getEnvVar("MQTT_USER")
password = getEnvVar("MQTT_PASS")

# MariaDB
# MariaDB database details
db_host = getEnvVar("MYSQL_HOST")
db_user = getEnvVar("MYSQL_USER")
db_password = getEnvVar("MYSQL_PASSWORD")
db_name  = getEnvVar("MYSQL_DB")
table_name  = getEnvVar("MYSQL_TABLE")

# Connect to MariaDB
db_connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)
db_cursor = db_connection.cursor()

# InfluxDB connection parameters
influx_token = getEnvVar('INFLUXDB_TOKEN')
influx_org = getEnvVar('INFLUXDB_ORG')
influx_bucket = getEnvVar('INFLUXDB_BUCKET')
influx_url = getEnvVar('INFLUXDB_HOST')

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("#")  # Subscribe to all topics using wildcard #

# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    print("Received message" + str(msg.payload) + " on topic: " + msg.topic)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        saveToMariaDB(timestamp, msg.topic, msg.payload.decode('utf-8'))
    except:
        print(f"Couldn't save to MariaDB: ({timestamp},{msg.topic},{msg.payload})")

    try:
        timestamp,value = t, v = msg.payload.decode('utf-8').split(" - ")  # messages look like <timestamp> - <value>
    except:
        print(f"Couldn't get timestamp and value from: {msg.payload}")
    else:
        try:
            saveToInfluxDB(timestamp, msg.topic, round(float(value),2))
        except:
            print(f"Couldn't save to InfluxDB: ({timestamp},{msg.topic},{value})")

    saveToSQLite(msg)

# Function to insert data into MariaDB table
def saveToMariaDB(timestamp, topic, raw_message):
    sql = f"INSERT INTO {table_name} (timestamp,topic, message) VALUES (%s, %s, %s)"
    val = (timestamp, topic, raw_message)
    db_cursor.execute(sql, val)
    db_connection.commit()
    print("Data inserted into MariaDB table")

def saveToSQLite(msg):
    message_data = {
        'topic': msg.topic,
        'message': msg.payload.decode('utf-8'),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    c.execute("INSERT INTO MQTT_MESSAGES (topic, message, timestamp) VALUES (?, ?, ?)", (msg.topic, json.dumps(message_data), message_data['timestamp']))
    conn.commit()
    
def saveToInfluxDB(timestamp, topic, value):
    
    # Create InfluxDB client
    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    
    # Create InfluxDB query API
    query_api = client.query_api()
            
    # Create InfluxDB write API
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # Create a Point for the temperature reading with specified timestamp and tags
    point = Point("MQTT_MESSAGES") \
        .field("value", value) \
        .tag("topic", topic) \
        .time(timestamp)
    
    # Write the data point to the 'weather' bucket
    write_api.write(bucket=influx_bucket, record=point)
    
    print(f"Sensor reading {value} saved to InfluxDB with tags: topic={topic}, timestamp={timestamp}")
    

print("setting up mqtt client")
# MQTT client setup
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username, password)
client.connect(broker_address, port, 60)

print("and away we go...")
# Start the MQTT client loop
client.loop_forever()
