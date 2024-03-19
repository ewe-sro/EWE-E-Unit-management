from datetime import datetime

import paho.mqtt.client as mqtt
import requests
import time
import csv
import re
import os

now = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
print(f"[{now}] Script started")



#######################################
############# LOAD CONFIG #############
#######################################

from utils import load_config

#config_path = "/data/user-app/charging_data/charging_data.conf"
config_path = "charging_data.conf"
config = load_config(config_path)

###########################################
############# END LOAD CONFIG #############
###########################################



#######################################
############# SET LOGGING #############
#######################################

from utils import set_logging
import logging

set_logging(config)

###########################################
############# END SET LOGGING #############
###########################################



########################################################
############# SQLALCHEMY DATABASE SETTINGS #############
########################################################

from utils import set_db
from models import Controller, State, Session, Email

engine = set_db(config)

############################################################
############# END SQLALCHEMY DATABASE SETTINGS #############
############################################################



###################################################
############# MQTT AND API CONNECTION #############
###################################################

# MQTT broker settings
broker_address = config["Mqtt"]["Host"]
broker_port = int(config["Mqtt"]["Port"])

# REST API settings
api_address = config["RestApi"]["Host"]
api_port = config["RestApi"]["Port"]

# Topic to find out if vehicle is connected
topic = "charging_controllers/+/data/iec_61851_state"

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(topic)
        logging.info(f"Subscribed to topic: {topic}")
    else:
        logging.error(f"Failed to connect to MQTT broker with result code {rc}")

###############################################
############# END MQTT CONNECTION #############
###############################################
        

        
############################################
############# CHARGER API CALL #############
############################################
        
def call_charger_api(api_call):
    # Make a REST API call to get the current energy data
    api_url = f"http://{api_address}:{api_port}/api/v1.0/{api_call}"
    response = requests.get(api_url)

    # If the API response is successful
    if response.status_code == 200:
        logging.info(f"API call was successful, URL: {api_url}")

        # Parse the response JSON data
        return response.json()
    
    else:
        logging.error(f"API call failed, URL: {api_url}")

        return False

################################################
############# END CHARGER API CALL #############
################################################
    


####################################################
############# ON VEHICLE STATUS CHANGE #############
####################################################

from utils import insert_data, get_controller

def on_vehicle_status_changed(client, userdata, message):
    logging.info(f"Message received from topic {message.topic}")

    vehicle_state = message.payload.decode("utf-8")
    # Vehicle connected states
    connected_vehicle_state = ["B1", "B2", "C1", "C2", "D1", "D2"]
    
    # Define a regular expression pattern to find a device_uid from message topic
    pattern = r"/([^/]+)/"
    # Find device_uid from the message topic
    match = re.search(pattern, message.topic)

    # Extract the device_uid if a match is found
    if match:
        device_uid = match.group(1)

    # Check if controller record is in database
    controller_exists = get_controller(device_uid, engine)

    if not controller_exists:
        # Get the charging point data from API
        charging_point_url = "charging-points"
        charging_point_data = call_charger_api(charging_point_url)
        charging_point_name = ""
    
        # If the API call was successful
        if charging_point_data != False:
            # Loop over the charging points and their data
            for charging_point, data in charging_point_data["charging_points"].items():
               # Check if the device_uid matches current device_uid variable
               if data["charging_controller_device_uid"] == device_uid:
                   charging_point_name = data["charging_point_name"]
    
        controller_record = Controller(
            device_uid=device_uid,
            charging_point_name=charging_point_name
        )
    
        # Insert the controller record to database
        insert_data(controller_record, engine)

    # Get the starting energy data from API
    energy_url = f"charging-controllers/{device_uid}/data?param_list=energy"
    energy_data = call_charger_api(energy_url)

    # Get the RFID data from API
    rfid_url = f"charging-controllers/{device_uid}/data?param_list=rfid"
    rfid_data = call_charger_api(rfid_url)

    ############ DUMP DATA TO CSV FILE #############
    data_folder_path = config["AppSettings"]["FileFolder"]

    # Check if folder for data DOESN'T exists
    if not os.path.isdir(data_folder_path):
        # Create the device_states folder
        os.makedirs(data_folder_path)

    # If vehicle is connected
    if vehicle_state in connected_vehicle_state:
        print(f"[{now}] EV connected! deviceUid: {device_uid}")
        logging.info(f"EV connected to deviceUid: {device_uid}")

        # If the API calls were successful
        if energy_data and rfid_data != False:

            # Get the last known state of the device
            last_known_state = None

            # If the state file doesn't exist or last known state is 'disconnected'
            if last_known_state and last_known_state == "disconnected":

                # If the API call was successful
                if charging_point_data != False:
                    # Collect all the data from API calls in a dictionary
                    #session_record = Session(
                    #    rfid_tag = rfid_data["rfid"]["tag"],
                    #    rfid_timestamp = rfid_data["rfid"]["timestamp"],
                    #    start_real_power_Wh = energy_data["energy"]["energy_real_power"]["value"],
                    #    end_real_power_Wh = "",
                    #    consumption_Wh = "",
                    #    start_timestamp = energy_data["energy"]["timestamp"],
                    #    end_timestamp = "",
                    #    duration = "",
                    #    device_uid = device_uid
                    #)
#
                    #insert_data(session_record, engine)

                    # Set the state record
                    state_record = State(
                        state = "connected",
                        device_uid = device_uid
                    )

                    # Set the last known state to 'connected'
                    insert_data(state_record, engine)

        else:
            logging.error(f"API calls were unsuccessful, exiting")


    # If vehicle is NOT connected
    else:
        print(f"[{now}] EV disconnected! deviceUid: {device_uid}")
        logging.info(f"EV disconnected from deviceUid: {device_uid}")

        # Set the state record
        state_record = State(
            state = "disconnected",
            device_uid = device_uid
        )

        # Set the last known state to 'connected'
        insert_data(state_record, engine)

        # If the API calls were successful
        #if energy_data and rfid_data != False:
#
        #    # CSV file already exists
        #    if os.path.isfile(csv_file_name):
        #        logging.info(f"CSV file already exists, adding data to file: {csv_file_name}")
#
        #        # Read current CSV data and assign corresponding variables
        #        current_data, edit_row, start_real_power, end_timestamp = read_csv_data(csv_file_name, device_uid)
#
        #        # If charging session data was found and the endTimestamp is empty
        #        if edit_row is not None and end_timestamp == "":
#
        #            # Update the corresponding row with current data
        #            edit_row["endRealPowerWh"] = energy_data["energy"]["energy_real_power"]["value"]
        #            edit_row["consumptionWh"] = energy_data["energy"]["energy_real_power"]["value"] - start_real_power
        #            edit_row["endTimestamp"] = energy_data["energy"]["timestamp"]
#
        #            # Write the data to the CSV file
        #            with open(csv_file_name, "w", newline="") as csv_file:
        #                 # Pass the file object and dictionary to DictWriter()
        #                writer = csv.DictWriter(csv_file, fieldnames=edit_row.keys())
#
        #                # Write the header row
        #                writer.writeheader()
#
        #                # Loop over the data and create a new dictionary for the CSV file
        #                for row in current_data:
        #                    # If the current row is the edited row, paste the edited data
        #                    if row["id"] == edit_row["id"]:
        #                        # Write the row with data
        #                        writer.writerow(edit_row)
        #                    else:
        #                        writer.writerow(row)
#
        #                csv_file.close()
#
        #            logging.info(f"Data was successfully added to the CSV file, id of the charging session: {edit_row['id']}")
        #            logging.info(f"Changing the last known state to 'disconnected' deviceUid: {device_uid}")
#
        #else:
        #    logging.error(f"API calls were unsuccessful, exiting")

########################################################
############# END ON VEHICLE STATUS CHANGE #############
########################################################



#######################################################
############# SET LAST KNOWN DEVICE STATE #############
#######################################################

def set_last_known_state(device_uid, state):
    state_folder_path = config["AppSettings"]["FileFolder"] + ".device_states/"
    state_file_path = state_folder_path + device_uid + ".state"

    # Check if folder for device states DOESN'T exists
    if not os.path.isdir(state_folder_path):
        # Create the device_states folder
        os.makedirs(state_folder_path)

    # If the device state changed to 'connected'
    if state == "connected" or state == "disconnected":
        # Open the state file in overwrite mode
        file = open(state_file_path, "w")

        file.write(state)

        logging.info(f"Setting device state deviceUid: {device_uid}, state: {state}")

    else:
        logging.info(f"Incorrect device state deviceUid: {device_uid}, state: {state}")

###########################################################
############# END SET LAST KNOWN DEVICE STATE #############
###########################################################
    


#########################################
############# READ CSV DATA #############
#########################################

def read_csv_data(csv_file, device_uid):
    # Open the existing CSV
    open_csv = open(csv_file, "r")
    # Read the CSV data
    reader = csv.DictReader(open_csv)
    # Put the data inside a list
    current_data = list(reader)
    # Set the variables for data updating
    highest_id = 0
    edit_row = None
    start_real_power = None
    # Set the variable for checking if the charging session is completed already
    end_timestamp = ""

    # Loop over the data to find correct row for edit
    for row in current_data:
        # If the deviceUid is the same and id is higher that currently highest known id
        if row["deviceUid"] == device_uid and int(row["id"]) > highest_id:
            edit_row = row
            highest_id = int(row["id"])
            start_real_power = int(row["startRealPowerWh"])
            end_timestamp = row["endTimestamp"]

    return current_data, edit_row, start_real_power, end_timestamp

#############################################
############# END READ CSV DATA #############
#############################################



########################################################
############# GET HIGHEST ID FROM CSV DATA #############
########################################################

def get_highest_id(csv_file):
    # Open the existing CSV
    open_csv = open(csv_file, "r")
    # Read the CSV data
    reader = csv.DictReader(open_csv)
    # Put the data inside a list
    current_data = list(reader)
    # Set the variables for data updating
    highest_id = 0

    # Loop over the data to find the highest id
    for row in current_data:
        # If id is higher than currently highest known id
        if int(row["id"]) > highest_id:
            highest_id = int(row["id"])

    return highest_id

############################################################
############# END GET HIGHEST ID FROM CSV DATA #############
############################################################



# Create an MQTT client instance
client = mqtt.Client()

# Set up callbacks
client.on_connect = on_connect
client.message_callback_add(topic, on_vehicle_status_changed)

# Connect to the broker
client.connect(broker_address, broker_port, 60)

# Start the MQTT loop to handle incoming messages
client.loop_start()

# Keep the script running
try:
    while True:
        pass

except KeyboardInterrupt:
    logging.info("Script terminated by user")
    client.disconnect()
    client.loop_stop()