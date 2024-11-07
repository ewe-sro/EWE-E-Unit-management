from datetime import datetime
import os

now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

#######################################
############# LOAD CONFIG #############
#######################################

import configparser

# config_path = "/data/user-app/charging_data/charging_data.conf"
config_path = "./charging_data.conf"


def load_config():
    # Check if config file exists
    if os.path.isfile(config_path):
        # Parse the config file
        config = configparser.ConfigParser()
        config.read(config_path)

        return config

    else:
        # If the config file wasn't found terminate the script
        print(f"[{now}] Config file not found, expected filename: {config_path}")
        print(f"[{now}] Terminating the script")
        exit()


###########################################
############# END LOAD CONFIG #############
###########################################


#######################################
############# SET LOGGING #############
#######################################

from logging.handlers import RotatingFileHandler
import logging


def set_logging(config):
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
        delay=0,
    )

    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[rfh])


###########################################
############# END SET LOGGING #############
###########################################


#######################################################
############# GET LAST KNOWN DEVICE STATE #############
#######################################################


def get_last_known_state(device_uid, config):
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


def set_last_known_state(device_uid, state, config):
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
    start_timestamp = ""

    # Set the variable for checking if the charging session is completed already
    end_timestamp = ""

    # RFID variables
    rfid_timestamp = ""
    rfid_tag = ""

    # Loop over the data to find correct row for edit
    for row in current_data:
        # If the deviceUid is the same and id is higher that currently highest known id
        if row["deviceUid"] == device_uid and int(row["id"]) > highest_id:
            edit_row = row
            highest_id = int(row["id"])
            start_real_power = int(row["startRealPowerWh"])
            start_timestamp = row["startTimestamp"]
            end_timestamp = row["endTimestamp"]
            rfid_timestamp = row["rfidTimestamp"]
            rfid_tag = row["rfidTag"]

    return (
        current_data,
        edit_row,
        start_real_power,
        start_timestamp,
        end_timestamp,
        rfid_timestamp,
        rfid_tag,
    )


#############################################
############# END READ CSV DATA #############
#############################################


########################################################
############# GET HIGHEST ID FROM CSV DATA #############
########################################################

import csv


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
        id = int(row["id"])

        # If id is higher than currently highest known id
        if id > highest_id:
            highest_id = id

    return highest_id


############################################################
############# END GET HIGHEST ID FROM CSV DATA #############
############################################################


############################################
############# SAVE DATA TO EMM #############
############################################

import requests
import json


def save_to_emm(data, api_url, api_key):
    try:
        # Request headers
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

        requests.post(api_url, data=json.dumps(data), headers=headers)

    except:
        logging.error(f"API call to EMM failed, URL: {api_url}")


################################################
############# END SAVE DATA TO EMM #############
################################################


###############################################
############# IS DATETIME BETWEEN #############
###############################################


def is_between_dates(start_datetime, end_datetime, target_datetime):
    return start_datetime <= target_datetime <= end_datetime


###################################################
############# END IS DATETIME BETWEEN #############
###################################################
