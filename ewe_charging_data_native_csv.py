#######################################
# VERSION 0.1
#
# @ 2024 EWE s.r.o.
# WWW: mobility.ewe.cz
#######################################



from datetime import datetime, timedelta

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
        # Parse the response JSON data
        return response.json()
    
    else:
        logging.error(f"API call failed, URL: {api_url}")

        return False

################################################
############# END CHARGER API CALL #############
################################################



############################################
############# SAVE DATA TO CSV #############
############################################

from utils import is_between_dates

record_id = 1

# Path to the CSV file
data_folder_path = config["AppSettings"]["FileFolder"]
csv_file_name = data_folder_path + "charging_data.csv"

def save_to_csv(data, state):
    global record_id

    # Check if folder for data DOESN'T exists
    if not os.path.isdir(data_folder_path):
        # Create the data folder
        os.makedirs(data_folder_path)

    if state == "connected":
        # CSV file already exists
        if os.path.isfile(csv_file_name):
            logging.info(f"CSV file already exists, adding data to file: {csv_file_name}")
    
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

    elif state == "disconnected":
        # CSV file already exists
        if os.path.isfile(csv_file_name):
            logging.info(f"CSV file already exists, adding data to file: {csv_file_name}")

            # Read current CSV data and assign corresponding variables
            current_data, edit_row, start_real_power, start_timestamp, end_timestamp, rfid_timestamp, rfid_tag = read_csv_data(csv_file_name, data["deviceUid"])

            # If charging session data was found and the endTimestamp is empty
            if edit_row is not None and end_timestamp == "":
                # Calculate the charging duration
                start_datetime = datetime.fromisoformat(start_timestamp)
                end_datetime = datetime.fromisoformat(data["endTimestamp"])

                duration = end_datetime - start_datetime

                # Check if an RFID chip was used and pair it to the charging session
                if (rfid_timestamp == "" and rfid_tag == "") and data["rfidTimestamp"] != "":
                    # Convert RFID timestamp to datetime object
                    rfid_datetime = start_datetime = datetime.fromisoformat(data["rfidTimestamp"])

                    if not is_between_dates(start_datetime, end_datetime, rfid_datetime):
                        data["rfidTimestamp"] = None
                        data["rfidTag"] = None

                elif rfid_timestamp != "" and rfid_tag != "":
                    data["rfidTimestamp"] = rfid_timestamp
                    data["rfidTag"] = rfid_tag


                # Update the corresponding row with current data
                edit_row["rfidTag"] = data["rfidTimestamp"]
                edit_row["rfidTimestamp"] = data["rfidTag"]
                edit_row["endRealPowerWh"] = data["endRealPowerWh"]
                edit_row["consumptionWh"] = data["endRealPowerWh"] - start_real_power
                edit_row["endTimestamp"] = data["endTimestamp"]
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

################################################
############# END SAVE DATA TO CSV #############
################################################



####################################################
############# ON VEHICLE STATUS CHANGE #############
####################################################
    
from utils import read_csv_data, get_highest_id, get_last_known_state, set_last_known_state, save_to_emm

def on_vehicle_status_changed(client, userdata, message):
    global record_id

    config = load_config()
    
    emm_api_host = config["EmmSettings"]["Host"]
    emm_api_key = config["EmmSettings"]["ApiKey"]
    emm_api_url = f"{emm_api_host}/api/public/charging-session"

    logging.info(f"Message received from topic {message.topic}")

    vehicle_state = message.payload.decode("utf-8")
    # Vehicle connected states
    connected_vehicle_state = ["B1", "B2", "C1", "C2", "D1", "D2"]


    ##############################################
    ############# GET THE DEVICE UID #############
    ##############################################
    
    # Define a regular expression pattern to find a device_uid from message topic
    pattern = r"/([^/]+)/"
    # Find device_uid from the message topic
    match = re.search(pattern, message.topic)

    # Extract the device_uid if a match is found
    if match:
        device_uid = match.group(1)

    ##################################################
    ############# END GET THE DEVICE UID #############
    ##################################################


    ########################################
    ############# GET API DATA #############
    ########################################

    # Get the starting energy data from API
    energy_url = f"charging-controllers/{device_uid}/data?param_list=energy"
    energy_data = call_charger_api(energy_url)

    # Get the charging point data from API
    charging_point_url = "charging-points"
    charging_point_data = call_charger_api(charging_point_url)

    # Set the valirables for charging point data
    charging_point_id = ""
    charging_point_name = ""

    # Loop over the charging points and their data
    for index, data in charging_point_data["charging_points"].items():
       # Check if the device_uid matches current device_uid variable
       if data["charging_controller_device_uid"] == device_uid:
           charging_point_id = data["id"]
           charging_point_name = data["charging_point_name"]

    point_config_url = f"charging-points/{charging_point_id}/config"
    point_config_data = call_charger_api(point_config_url)

    # Get the RFID data from API
    rfid_url = f"charging-controllers/{point_config_data['rfid_reader_device_uid']}/data?param_list=rfid"
    rfid_data = call_charger_api(rfid_url)

    ############################################
    ############# END GET API DATA #############
    ############################################

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

            # Get the time difference between RFID timestamp and start timestamp of the charging session
            if rfid_data["rfid"]["timestamp"] != "":
                rfid_difference = abs(datetime.fromisoformat(rfid_data["rfid"]["timestamp"]) - datetime.fromisoformat(energy_data["energy"]["timestamp"]))
        
                # Check if RFID timestamp is within 60 seconds of the start of the charging session
                if rfid_difference > timedelta(seconds=60):
                    rfid_data["rfid"]["tag"] = ""
                    rfid_data["rfid"]["timestamp"] = ""
        
            # If data rfidTimestamp is empty set to RFID data to empty
            else:
                rfid_data["rfid"]["tag"] = ""
                rfid_data["rfid"]["timestamp"] = ""

            # Get the last known state of the device
            last_known_state = get_last_known_state(device_uid, config)

            # If the state file doesn't exist or last known state is 'disconnected'
            if last_known_state is False or last_known_state == "disconnected":

                if os.path.isfile(csv_file_name):
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
                    "endRealPowerWh": None,
                    "consumptionWh": None,
                    "startTimestamp": energy_data["energy"]["timestamp"],
                    "endTimestamp": None,
                    "duration": None
                }
                
                # Save the data to CSV
                save_to_csv(data, "connected")

                # If EMM API is configured also save to EMM web app
                if emm_api_host != "" and emm_api_key != "" and emm_api_url != "":
                    save_to_emm(data, emm_api_url, emm_api_key)

                # Set the last known state to 'connected'
                set_last_known_state(device_uid, "connected", config)
                logging.info(f"Changing the last known state to 'connected' deviceUid: {device_uid}")

        else:
            logging.error(f"API calls were unsuccessful, exiting")


    # If vehicle is NOT connected/disconnected
    else:
        print(f"[{now}] EV disconnected! deviceUid: {device_uid}")
        logging.info(f"EV disconnected from deviceUid: {device_uid}")

        # Set the last known state to 'disconnected'
        set_last_known_state(device_uid, "disconnected", config)
        logging.info(f"Changing the last known state to 'disconnected' deviceUid: {device_uid}")

        # If the API calls were successful
        if energy_data and rfid_data != False:
            # Update the corresponding row with current data
            data = {
                "deviceUid": device_uid,
                "rfidTag": rfid_data["rfid"]["tag"],
                "rfidTimestamp": rfid_data["rfid"]["timestamp"],
                "endRealPowerWh": energy_data["energy"]["energy_real_power"]["value"],
                "endTimestamp": energy_data["energy"]["timestamp"]
            }

            # Save the data to CSV
            save_to_csv(data, "disconnected")

            # If EMM API is configured also save to EMM web app
            if emm_api_host != "" and emm_api_key != "" and emm_api_url != "":
                save_to_emm(data, emm_api_url, emm_api_key)

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