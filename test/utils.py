from typing import Dict, Optional, Any
from datetime import datetime
import os

now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

#######################################
############# LOAD CONFIG #############
#######################################

import configparser

# config_path = "/data/user-app/charging_data/charging_data.conf"
config_path = "./charging_data.conf"


def load_config() -> Optional[Dict[str, str]]:
    """
    Load config data from a .conf file.

    Returns:
        Dict containing the config data if successful, None if failed
    """

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


def set_logging(config) -> None:
    """
    Sets up logging for record error and info message into a log file.

    Args:
        config: Dictionary containing configuration values

    Returns:
        Dict containing the config data if successful, None if failed
    """

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
    log_format = "%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s"

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


def get_last_known_state(
    device_uid: str, config
) -> Optional[str]:
    """
    Gets the last known charging state of a charging controller.

    Args:
        device_uid: Charging controller ID
        config: Dictionary containing configuration values

    Returns:
        The last known charging state of the charging controller - 'connected' or 'disconnected' if successful, None if failed
    """

    state_folder_path = config["AppSettings"]["FileFolder"] + ".device_states/"
    state_file_path = state_folder_path + device_uid + ".state"

    # Check if folder for device states DOESN'T exists, if so create it
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
            return None

    else:
        return None


###########################################################
############# END GET LAST KNOWN DEVICE STATE #############
###########################################################


#######################################################
############# SET LAST KNOWN DEVICE STATE #############
#######################################################


def set_last_known_state(
    device_uid: str, state: str, config
) -> None:
    """
    Sets the last known charging state of a charging controller.

    Args:
        device_uid: Charging controller ID
        state: The state that will be saved
        config: Dictionary containing configuration values

    Returns:
        None
    """
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
############# SEND API REQUEST #############
############################################

import requests


def send_request(
    url: str,
    method: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Optional[Dict[str, Any]]:
    """
    Make an HTTP request with error logging. Continues execution on error.

    Args:
        url: The URL to send the request to
        method: HTTP method to use (GET, POST, PUT, DELETE, PATCH)
        headers: Optional dictionary of HTTP headers
        params: Optional dictionary of query parameters
        data: Optional dictionary of form data
        json: Optional dictionary of JSON data
        timeout: Request timeout in seconds
    Returns:
        Dict containing the response data if successful, None if failed
    """

    # Validate the supplied method
    method = method.upper()
    if method not in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
        logging.error(f"Invalid HTTP method: {method}")
        return None

    # Set default headers if none provided
    if headers is None:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        # Send the API request
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=data,
            json=json,
            timeout=timeout,
        )

        # Log but don't raise for bad status codes
        if response.status_code >= 400:
            logging.error(
                f"HTTP {response.status_code} error occurred: {response.text}. URL: {url}"
            )
            return None

        # Try to return JSON response
        try:
            return response.json()

        except ValueError:
            # Return text response if not JSON
            return response.text

    except requests.exceptions.ConnectionError:
        logging.error(
            f"Failed to connect to the server. Please check your internet connection. URL: {url}"
        )
        return None

    except requests.exceptions.Timeout:
        logging.error(f"Request timed out after {timeout} seconds. URL: {url}")
        return None

    except requests.exceptions.RequestException as err:
        logging.error(f"Request failed: {str(err)}. URL: {url}")
        return None


################################################
############# END SEND API REQUEST #############
################################################


############################################
############# SAVE DATA TO EMM #############
############################################

import json


def save_to_emm(data, api_url, api_key):
    try:
        # Request headers
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

        requests.post(api_url, data=json.dumps(data), headers=headers)

    except Exception as e:
        logging.error(f"API call to EMM failed, URL: {api_url}, {e}")


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


#################################################
############# GET CHARGING POINT ID #############
#################################################


def get_charging_point(controller_id: str, api_url: str):
    # Default return values
    charging_point_id: Optional[str] = None
    charging_point_name: Optional[str] = None

    # Send an API request to the provided URL
    charging_point_data = send_request(url=api_url, method="GET")

    # If the API response is successful
    if charging_point_data is None:
        return charging_point_id, charging_point_name

    # Loop over the charging points and their data
    for index, data in charging_point_data["charging_points"].items():
        # Check if the device_uid matches current device_uid variable
        if data["charging_controller_device_uid"] == controller_id:
            charging_point_id = data["id"]
            charging_point_name = data["charging_point_name"]

    return charging_point_id, charging_point_name


#####################################################
############# END GET CHARGING POINT ID #############
#####################################################