from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import os

now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

#######################################
############# LOAD CONFIG #############
#######################################

import configparser

PROD_PATH = "/data/user-app/charging_data/charging_data.conf"
DEV_PATH = "./charging_data.conf"

# Automatically choose the config based on what actually exists
config_path = PROD_PATH if os.path.exists(PROD_PATH) else DEV_PATH


def load_config() -> Optional[configparser.ConfigParser]:
    """
    Load config data from a .conf file.

    Returns:
        configparser.ConfigParser object if successful, None if failed
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


def get_last_known_state(device_uid: str, config) -> Optional[str]:
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


def set_last_known_state(device_uid: str, state: str, config) -> None:
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
    """
    Gets the highest integer from a CSV file in column ID.

    Args:
        csv_file: The path to the CSV file, from which we'll get the highest ID

    Returns:
        highest_id = either 0 or the highest found integer in ID column
    """
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

Response = requests.models.Response


def send_request(
    url: str,
    method: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Optional[Response]:
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
        The response if successful, None if failed
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

        # Return the response
        return response

    except requests.exceptions.ConnectionError as err:
        logging.error(
            f"Failed to connect to the server. Please check your internet connection: {str(err)}. URL: {url}"
        )
        return None

    except requests.exceptions.Timeout:
        logging.error(
            f"Request timed out after {timeout} seconds: {str(err)}. URL: {url}"
        )
        return None

    except requests.exceptions.RequestException as err:
        logging.error(f"Request failed: {str(err)}. URL: {url}")
        return None


################################################
############# END SEND API REQUEST #############
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


def get_charging_point(controller_id: str, api_url: str) -> Tuple[Optional[str], Optional[str]]:
    charging_point_id: Optional[str] = None
    charging_point_name: Optional[str] = None

    # Send an API request to the provided URL
    charging_point_response = send_request(url=api_url, method="GET")

    # If the API response is successful
    if charging_point_response is None:
        return charging_point_id, charging_point_name
    
    try:
        charging_point_data = charging_point_response.json()

        for index, data in charging_point_data["charging_points"].items():
            if data["charging_controller_device_uid"] == controller_id:
                charging_point_id = data["id"]
                charging_point_name = data["charging_point_name"]

                break

    except ValueError:
        logging.error(f"Failed to parse JSON from charging point API response: {charging_point_response.text}")

    return charging_point_id, charging_point_name


#####################################################
############# END GET CHARGING POINT ID #############
#####################################################


###################################################
############# SQLITE QUEUE MANAGEMENT #############
###################################################

import sqlite3
import json

# The queue database file name 
QUEUE_DB_NAME = "data_queue.db"


def _get_queue_db_path(config) -> str:
    """
    Constructs the full file path for the SQLite queue database.

    Args:
        config: Dictionary containing configuration values, specifically 'AppSettings'.
    Returns:
        The full path to the SQLite database file.
    """

    data_folder_path = config["AppSettings"]["FileFolder"]
    return os.path.join(data_folder_path, QUEUE_DB_NAME)


def initialize_queue_db(config) -> None:
    """
    Initializes the SQLite database for the charging session queue.
    Creates the 'charging_session' table if it does not already exist.

    Args:
        config: Dictionary containing configuration values, specifically 'AppSettings'.
    Returns:
        None
    """

    # Get the database full file path
    db_path = _get_queue_db_path(config)

    # Initialize the SQLite connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the database table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS charging_session (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL UNIQUE, -- The 'id' from your charging data
            device_uid TEXT NOT NULL,
            payload TEXT NOT NULL,
            type TEXT NOT NULL, -- 'start' or 'end'
            status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'sent', 'failed'
            attempts INTEGER DEFAULT 0,
            last_attempt_at TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()

    # Close the database connection
    conn.close()

    logging.info(f"Initialized SQLite queue database at {db_path}")


def add_to_queue(config, session_id: int, device_uid: str, payload: Dict[str, Any], session_type: str) -> None:
    """
    Adds a charging session event (start or end) to the SQLite queue.
    If an entry with the same session_id and type already exists,
    its payload is updated, and its status is reset to 'pending' for re-transmission.

    Args:
        config: Dictionary containing configuration values.
        session_id: The unique identifier for the charging session.
        device_uid: The unique identifier of the charging device.
        payload: A dictionary containing the full charging session data,
                 which will be stored as a JSON string.
        session_type: The type of the session event, either 'start' or 'end'.
    Returns:
        None
    """

    # Get the database full file path
    db_path = _get_queue_db_path(config)

    # Initialize the SQLite connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Convert payload dict to JSON string for storage
    payload_json = json.dumps(payload)
    current_time = datetime.now().isoformat()

    # Check if a session with this session_id and type already exists
    # This is crucial to avoid duplicates when retrying `save_to_queue` due to other failures
    cursor.execute("""
        SELECT id FROM charging_session
        WHERE session_id = ? AND type = ?
    """, (str(session_id), session_type))
    existing_entry = cursor.fetchone()

    if existing_entry:
        # If it exists, update it (e.g., if we are retrying an 'end' session that was already 'pending')
        # For simplicity, we'll just update the payload and reset status to pending for re-transmission
        cursor.execute("""
            UPDATE charging_session
            SET payload = ?, status = 'pending', attempts = 0, last_attempt_at = NULL, created_at = ?
            WHERE session_id = ? AND type = ?
        """, (payload_json, current_time, str(session_id), session_type))
        logging.info(f"Updated existing charging session in queue: ID {session_id}, Type {session_type}")
    else:
        # Otherwise, insert a new entry
        cursor.execute("""
            INSERT INTO charging_session (session_id, device_uid, payload, type, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (str(session_id), device_uid, payload_json, session_type, current_time))
        logging.info(f"Added charging session to queue: ID {session_id}, Type {session_type}")

    conn.commit()
    conn.close()


def get_pending_queue_items(config, max_attempts: int = 5) -> List[Dict]:
    """
    Retrieves a list of charging session events from the queue that are
    either 'pending' or 'failed' and have not exceeded the maximum number of attempts.
    Items are ordered by their creation time.

    Args:
        config: Dictionary containing configuration values.
        max_attempts: The maximum number of attempts after which a failed item
                      will no longer be retrieved from the queue.
    Returns:
        A list of dictionaries, where each dictionary represents a queued item
        with its details (session_id, device_uid, payload, type,
        attempts, last_attempt_at, and queue_db_id). Returns an empty list if no items.
    """

    # Get the database full file path
    db_path = _get_queue_db_path(config)

    # Initialize the SQLite connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch items that are pending or failed but haven't exceeded max_attempts, ordered by creation time
    cursor.execute("""
        SELECT session_id, device_uid, payload, type, attempts, last_attempt_at, id
        FROM charging_session
        WHERE status IN ('pending', 'failed') AND attempts < ?
        ORDER BY created_at ASC
    """, (max_attempts,))
    rows = cursor.fetchall()
    conn.close()

    items = []
    for row in rows:
        try:
            items.append({
                "queue_db_id": row[6], # The internal SQLite primary key
                "session_id": int(row[0]),
                "device_uid": row[1],
                "payload": json.loads(row[2]), # Convert JSON string back to dict
                "type": row[3],
                "attempts": row[4],
                "last_attempt_at": row[5]
            })
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON payload for queue item: {row[2]}")
        except ValueError:
            logging.error(f"Error converting session_id to int for queue item: {row[0]}")
    return items

def update_queue_item_status(config, queue_db_id: int, status: str, increment_attempts: bool = False) -> None:
    """
    Updates the status of a specific item in the SQLite queue.
    Optionally increments the 'attempts' count and updates 'last_attempt_at'.

    Args:
        config: Dictionary containing configuration values.
        queue_db_id: The internal SQLite primary key of the queue item to update.
        status: The new status for the item ('pending', 'sent', 'failed').
        increment_attempts: If True, the 'attempts' count will be increased by one.
    Returns:
        None
    """


     # Get the database full file path
    db_path = _get_queue_db_path(config)

    # Initialize the SQLite connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    current_time = datetime.now().isoformat()

    if increment_attempts:
        cursor.execute("""
            UPDATE charging_session
            SET status = ?, attempts = attempts + 1, last_attempt_at = ?
            WHERE id = ?
        """, (status, current_time, queue_db_id))
    else:
        cursor.execute("""
            UPDATE charging_session
            SET status = ?, last_attempt_at = ?
            WHERE id = ?
        """, (status, current_time, queue_db_id))
    conn.commit()
    conn.close()

#######################################################
############# END SQLITE QUEUE MANAGEMENT #############
#######################################################