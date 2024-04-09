import sqlite3
import json
import os
import paho.mqtt.client as mqtt
from datetime import datetime

# SQLite database setup
conn = sqlite3.connect('IoT.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS MQTT_MESSAGES
             (timestamp DATETIME, topic TEXT, message TEXT)''')

# MQTT broker settings
broker_address = os.environ["MQTT_BROKER"]
port = int(os.environ["MQQT_BROKER_PORT"])
username = os.environ["MQTT_USER"]
password = os.environ["MQTT_PASS"]

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("#")  # Subscribe to all topics using wildcard #

# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    print("Received message on topic: " + msg.topic)

    message_data = {
        'topic': msg.topic,
        'message': msg.payload.decode('utf-8'),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    c.execute("INSERT INTO MQTT_MESSAGES (topic, message, timestamp) VALUES (?, ?, ?)", (msg.topic, json.dumps(message_data), message_data['timestamp']))
    conn.commit()
        


# MQTT client setup
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username, password)
client.connect(broker_address, port, 60)

# Start the MQTT client loop
client.loop_forever()
