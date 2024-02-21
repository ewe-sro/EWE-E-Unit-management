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

import configparser

config_path = "charging_data.conf"

# Check if config file exists
if os.path.isfile(config_path):
    # Parse the config file
    config = configparser.ConfigParser()
    config.read("charging_data.conf")

else:
    # If the config file wasn't found terminate the script
    print(f"Config file not found, expected filename: {config_path}")
    print("Terminating the script")
    exit()

###########################################
############# END LOAD CONFIG #############
###########################################



#######################################
############# SET LOGGING #############
#######################################

from logging.handlers import RotatingFileHandler
import logging

# Set the log location
log_folder_path = config["LogSettings"]["LogFolder"]
log_file_path = config["LogSettings"]["LogFile"]
log_location = log_folder_path + log_file_path

# Check if folder for log DOESN'T exists
if not os.path.isdir(log_folder_path):
    # Create the log folder
    os.makedirs(log_folder_path)

# Set the log file max size
log_max_size = int(config["LogSettings"]["LogFileQuotaMBytes"]) * 1024 * 1024
# Set the log format
log_format = "%(asctime)s %(levelname)s %(message)s"

# Set the log handler
rfh = RotatingFileHandler(
    log_location,
    mode="a",
    maxBytes=log_max_size,
    backupCount=int(config["LogSettings"]["LogFileSplits"]),
    encoding=None,
    delay=0
)

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        rfh
    ]
)

###########################################
############# END SET LOGGING #############
###########################################



###########################################
############# SQLITE DATABASE #############
###########################################
        
import sqlalchemy as db
from models import Base, Controller, State, Session, Email

database_name = config["DatabaseSettings"]["DatabaseName"]

engine = db.create_engine(f"sqlite:///{database_name}")

# Create the database schema
Base.metadata.create_all(engine)

###########################################
############# END SQLITE DATABASE #########
###########################################



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
    
        

#######################################################
############# GET LAST KNOWN DEVICE STATE #############
#######################################################
    
def get_last_known_state(device_uid):
    state_folder_path = config["AppSettings"]["FileFolder"] + ".device_states/"
    state_file_path = state_folder_path + device_uid + ".state"

    # Check if folder for device states DOESN'T exists
    if not os.path.isdir(state_folder_path):
        # Create the device_states folder
        os.makedirs(state_folder_path)

    # State file already exists
    if os.path.isfile(state_file_path):
        # Open the state file in read mode
        file = open(state_file_path, "r")

        state = file.read()

        # If the state is valid
        if state == "connected" or state == "disconnected":
            return state
        else:
            return False
        
    else:
        return False

###########################################################
############# END GET LAST KNOWN DEVICE STATE #############
###########################################################



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
    


####################################################
############# ON VEHICLE STATUS CHANGE #############
####################################################

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
    csv_file_path = data_folder_path + "charging_data_{}.csv"

    # Set the CSV file name
    current_month = datetime.now().strftime("%m%y")
    csv_file_name = csv_file_path.format(current_month)

    # If vehicle is connected
    if vehicle_state in connected_vehicle_state:
        print(f"[{now}] EV connected! deviceUid: {device_uid}")
        logging.info(f"EV connected to deviceUid: {device_uid}")

        # If the API calls were successful
        if energy_data and rfid_data != False:

            # Get the last known state of the device
            last_known_state = get_last_known_state(device_uid)

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
                set_last_known_state(device_uid, "connected")
                logging.info(f"Changing the last known state to 'connected' deviceUid: {device_uid}")

        else:
            logging.error(f"API calls were unsuccessful, exiting")


    # If vehicle is NOT connected
    else:
        print(f"[{now}] EV disconnected! deviceUid: {device_uid}")
        logging.info(f"EV disconnected from deviceUid: {device_uid}")

        # Set the last known state to 'disconnected'
        set_last_known_state(device_uid, "disconnected")

        # If the API calls were successful
        if energy_data and rfid_data != False:

            # CSV file already exists
            if os.path.isfile(csv_file_name):
                logging.info(f"CSV file already exists, adding data to file: {csv_file_name}")

                # Read current CSV data and assign corresponding variables
                current_data, edit_row, start_real_power, end_timestamp = read_csv_data(csv_file_name, device_uid)

                # If charging session data was found and the endTimestamp is empty
                if edit_row is not None and end_timestamp == "":

                    # Update the corresponding row with current data
                    edit_row["endRealPowerWh"] = energy_data["energy"]["energy_real_power"]["value"]
                    edit_row["consumptionWh"] = energy_data["energy"]["energy_real_power"]["value"] - start_real_power
                    edit_row["endTimestamp"] = energy_data["energy"]["timestamp"]

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
            


############################################
############# GET EMAIL STATUS #############
############################################
    
def get_email_status(month):
    email_folder_path = config["AppSettings"]["FileFolder"] + ".emails/"
    email_file_path = email_folder_path + month + ".txt"

    # Check if folder for email status DOESN'T exists
    if not os.path.isdir(email_folder_path):
        # Create the emails folder
        os.makedirs(email_folder_path)

    # Email file already exists
    if os.path.isfile(email_file_path):
        return True
        
    else:
        return False

################################################
############# END GET EMAIL STATUS #############
################################################
    


############################################
############# SET EMAIL STATUS #############
############################################

def set_email_status(month):
    email_folder_path = config["AppSettings"]["FileFolder"] + ".emails/"
    email_file_path = email_folder_path + month + ".txt"

    # Check if folder for device states DOESN'T exists
    if not os.path.isdir(email_folder_path):
        # Create the device_states folder
        os.makedirs(email_folder_path)

    # If the device state changed to 'connected'
    if not os.path.isfile(email_file_path):
        # Open the state file in overwrite mode
        file = open(email_file_path, "w")

        file.write(f"Email pro odbobí {month} odeslán: {now}")

################################################
############# END SET EMAIL STATUS #############
################################################



###############################################
############# GET LAST MONTH CODE #############
###############################################
        
def get_last_month_code():
    # Get current date and find the number of last month
    d = datetime.now()
    month, year = (d.month-1, d.year) if d.month != 1 else (12, d.year-1)
    d = d.replace(month=month, year=year)
    last_month = d.strftime("%m%y")

    return last_month

###################################################
############# END GET LAST MONTH CODE #############
###################################################



######################################
############# SEND EMAIL #############
######################################

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# Get the last month and year in mmyy format 
last_month = get_last_month_code()
    
# Email configuration
email_sender = config["EmailSettings"]["EmailSender"]
email_receiver = config["EmailSettings"]["EmailReceiver"]
email_subject = f"EWE - Charging data - {last_month}"
email_message = f"V příloze vám zasíláme souhrn dat nabíjecí stanice za období {last_month}"
email_password = config["EmailSettings"]["EmailPassword"]
email_server = config["EmailSettings"]["EmailServer"]
email_port = config["EmailSettings"]["EmailPort"]

# Function to send email with the CSV attachment
def send_email(attachment_path, attachment_name):
    msg = MIMEMultipart()
    msg["From"] = email_sender
    msg["To"] = email_receiver
    msg["Subject"] = email_subject

    # Check if the attachment file exists
    if os.path.isfile(attachment_path):
        # Attach the CSV file
        with open(attachment_path, "rb") as attachment:
            attach = MIMEApplication(attachment.read(), _subtype="csv")
            attach.add_header("Content-Disposition", "attachment", filename=attachment_name)
            msg.attach(attach)

        # Attach the email text
        html_part = MIMEText(email_message)
        msg.attach(html_part)

        # Connect to the SMTP server and send the email
        with smtplib.SMTP("ms.ewe.cz", 25) as server:
            server.starttls()
            server.login(email_sender, email_password)  # Replace with your email password
            server.sendmail(email_sender, email_receiver, msg.as_string())

        set_email_status(last_month)

        logging.info(f"Email successfuly sent to: {email_receiver}")

    else:
        logging.error(f"The attachment for the email not found: {attachment_path}")

##########################################
############# END SEND EMAIL #############
##########################################



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
        # Send email and reset the record_id at the beginning of each month
        if datetime.now().day == 15 and datetime.now().hour == 8 and datetime.now().minute == 0 and datetime.now().second == 0:
            last_month = get_last_month_code()

            # If the email hasn't been sent yet
            if get_email_status(last_month) == False:
                # Set the email attachment path and name
                attachment_name = f"charging_data_{last_month}.csv"
                attachment_path = csv_file_path = config["AppSettings"]["FileFolder"] + attachment_name

                # Send the email and create a record about sending
                send_email(attachment_path, attachment_name)

                # Wait 1 second
                time.sleep(1)

        pass

except KeyboardInterrupt:
    logging.info("Script terminated by user")
    client.disconnect()
    client.loop_stop()