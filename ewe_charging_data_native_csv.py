from datetime import datetime

import csv
import re
import os

now = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
print(f"[{now}] Script started")



#######################################
############# LOAD CONFIG #############
#######################################

from utils import load_config

config = load_config()

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



###################################################
############# MQTT AND API CONNECTION #############
###################################################

import paho.mqtt.client as mqtt

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
        
import requests
        
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
    
from utils import read_csv_data, get_highest_id, get_last_known_state, set_last_known_state

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

    global record_id

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

    # Path to the CSV file
    csv_file_name = data_folder_path + "charging_data.csv"

    # If vehicle is connected
    if vehicle_state in connected_vehicle_state:
        print(f"[{now}] EV connected! deviceUid: {device_uid}")
        logging.info(f"EV connected to deviceUid: {device_uid}")

        # If the API calls were successful
        if energy_data and rfid_data != False:

            # Get the last known state of the device
            last_known_state = get_last_known_state(device_uid, config)

            # If the state file doesn't exist or last known state is 'disconnected'
            if last_known_state is False or last_known_state == "disconnected":

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
                
                # CSV file already exists
                if os.path.isfile(csv_file_name):
                    logging.info(f"CSV file already exists, adding data to file: {csv_file_name}")

                    # Get the current highest id of charging session
                    highest_id = get_highest_id(csv_file_name)
                    record_id = highest_id + 1

                    # Collect all the data from API calls in a dictionary
                    data = {
                        "id": record_id,
                        "deviceUid": device_uid,
                        "chargingPointName": charging_point_name,
                        "rfidTag": rfid_data["rfid"]["tag"],
                        "rfidTimestamp": rfid_data["rfid"]["timestamp"],
                        "startRealPowerWh": energy_data["energy"]["energy_real_power"]["value"],
                        "endRealPowerWh": "",
                        "consumptionWh": "",
                        "startTimestamp": energy_data["energy"]["timestamp"],
                        "endTimestamp": "",
                        "duration": "",
                    }

                    # Write the data to the CSV file
                    with open(csv_file_name, "a", newline="") as csv_file:
                        # Pass the file object and dictionary to DictWriter()
                        writer = csv.DictWriter(csv_file, fieldnames=data.keys())

                        # Write the row with data
                        writer.writerow(data)

                        csv_file.close()

                    logging.info(f"Data was successfully added to the CSV file")

                # CSV file doesn't exists
                else:

                    logging.info(f"CSV wasn't found, creating a new CSV file: {csv_file_name}")

                    # Set record id
                    record_id = 1

                    # Collect all the data from API calls in a dictionary
                    data = {
                        "id": record_id,
                        "deviceUid": device_uid,
                        "chargingPointName": charging_point_name,
                        "rfidTag": rfid_data["rfid"]["tag"],
                        "rfidTimestamp": rfid_data["rfid"]["timestamp"],
                        "startRealPowerWh": energy_data["energy"]["energy_real_power"]["value"],
                        "endRealPowerWh": "",
                        "consumptionWh": "",
                        "startTimestamp": energy_data["energy"]["timestamp"],
                        "endTimestamp": "",
                        "duration": "",
                    }

                    # Write the data to the CSV file
                    with open(csv_file_name, "w", newline="") as csv_file:
                        # Pass the file object and dictionary to DictWriter()
                        writer = csv.DictWriter(csv_file, fieldnames=data.keys())

                        # Write the header row
                        writer.writeheader()

                        # Write the row with data
                        writer.writerow(data)

                        csv_file.close()

                    # Increase the number of ID
                    record_id += 1

                    logging.info(f"Data was successfully added to the CSV file")

                # Set the last known state to 'connected'
                set_last_known_state(device_uid, "connected", config)
                logging.info(f"Changing the last known state to 'connected' deviceUid: {device_uid}")

        else:
            logging.error(f"API calls were unsuccessful, exiting")


    # If vehicle is NOT connected
    else:
        print(f"[{now}] EV disconnected! deviceUid: {device_uid}")
        logging.info(f"EV disconnected from deviceUid: {device_uid}")

        # Set the last known state to 'disconnected'
        set_last_known_state(device_uid, "disconnected", config)

        # If the API calls were successful
        if energy_data and rfid_data != False:

            # CSV file already exists
            if os.path.isfile(csv_file_name):
                logging.info(f"CSV file already exists, adding data to file: {csv_file_name}")

                # Read current CSV data and assign corresponding variables
                current_data, edit_row, start_real_power, start_timestamp, end_timestamp = read_csv_data(csv_file_name, device_uid)

                # If charging session data was found and the endTimestamp is empty
                if edit_row is not None and end_timestamp == "":
                    # Calculate the charging duration
                    start_datetime = datetime.fromisoformat(start_timestamp)
                    end_datetime = datetime.fromisoformat(energy_data["energy"]["timestamp"])

                    duration = end_datetime - start_datetime

                    # Update the corresponding row with current data
                    edit_row["endRealPowerWh"] = energy_data["energy"]["energy_real_power"]["value"]
                    edit_row["consumptionWh"] = energy_data["energy"]["energy_real_power"]["value"] - start_real_power
                    edit_row["endTimestamp"] = energy_data["energy"]["timestamp"]
                    edit_row["duration"] = duration

                    # Write the data to the CSV file
                    with open(csv_file_name, "w", newline="") as csv_file:
                         # Pass the file object and dictionary to DictWriter()
                        writer = csv.DictWriter(csv_file, fieldnames=edit_row.keys())

                        # Write the header row
                        writer.writeheader()

                        # Loop over the data and create a new dictionary for the CSV file
                        for row in current_data:
                            # If the current row is the edited row, paste the edited data
                            if row["id"] == edit_row["id"]:
                                # Write the row with data
                                writer.writerow(edit_row)
                            else:
                                writer.writerow(row)

                        csv_file.close()

                    logging.info(f"Data was successfully added to the CSV file, id of the charging session: {edit_row['id']}")
                    logging.info(f"Changing the last known state to 'disconnected' deviceUid: {device_uid}")

        else:
            logging.error(f"API calls were unsuccessful, exiting")

########################################################
############# END ON VEHICLE STATUS CHANGE #############
########################################################



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