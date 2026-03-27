from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
import os
import time

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

    except requests.exceptions.Timeout as err:
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


def get_db_connection(config):
    """
    Returns a connection with optimized per-session settings.
    """

    db_path = _get_queue_db_path(config)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # 20-second timeout to handle concurrency
    conn = sqlite3.connect(db_path, timeout=20)
    
    # synchronous=NORMAL provides the best balance between performance and safety in WAL mode
    conn.execute("PRAGMA synchronous=NORMAL;")

    # Make SQLite return dictionaries
    conn.row_factory = sqlite3.Row
    
    return conn


def initialize_queue_db(config) -> None:
    """
    Initializes the SQLite database for the charging session queue.
    Creates the 'charging_session' table if it does not already exist.

    Args:
        config: Dictionary containing configuration values, specifically 'AppSettings'.
    Returns:
        None
    """

    db_path = _get_queue_db_path(config)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with get_db_connection(config) as conn:
        cursor = conn.cursor()

        # Enable WAL mode for concurrent database writes
        cursor.execute("PRAGMA journal_mode=WAL;")

        # 'charging_session' database table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS charging_session (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                charging_session_id TEXT NOT NULL,
                device_uid TEXT NOT NULL,
                payload TEXT NOT NULL,
                type TEXT NOT NULL, -- 'start' or 'end'
                status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'sent', 'failed'
                attempts INTEGER DEFAULT 0,
                last_attempt_at TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(charging_session_id, type)
            )
        """)

        # 'rfid_event' database table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rfid_event (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                claimed_by_session_id TEXT DEFAULT NULL,
                created_at TEXT NOT NULL
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rfid_timestamp ON rfid_event (timestamp);
        """)

        # 'device_status' database table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS device_status (
                device_uid TEXT PRIMARY KEY,
                status TEXT NOT NULL, -- 'connected', 'disconnected'
                updated_at TEXT NOT NULL
            )
        """)

        # 'controller_telemetry' database table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS controller_telemetry (
                device_uid TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

    logging.info(f"Initialized SQLite queue database with WAL mode")


def save_rfid_event(config, tag: str, timestamp: str):
    """Stores every RFID scan into a buffer."""

    with get_db_connection(config) as conn:
        cursor = conn.cursor()

        # Avoid duplicate entries for the exact same scan
        cursor.execute("SELECT id FROM rfid_event WHERE tag = ? AND timestamp = ?", (tag, timestamp))
        
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO rfid_event (tag, timestamp, created_at) VALUES (?, ?, ?)",
                (tag, timestamp, datetime.now().isoformat())
            )

            logging.info(f"Saved RFID scan to database: {tag} at {timestamp}")


def find_and_claim_rfid(config, session_id: str, start_timestamp: str):
    """
    Finds the single unclaimed RFID tag closest to the start_timestamp 
    within a window of -65 seconds to +65 seconds.
    """

    start_datetime = datetime.fromisoformat(start_timestamp)

    # Define the total window for a valid pairing
    min_window = (start_datetime - timedelta(seconds=65)).isoformat()
    max_window = (start_datetime + timedelta(seconds=65)).isoformat()
    
    # Try to pair RFID up to 4 times (total wait: ~6 seconds)
    for attempt in range(4):
        with get_db_connection(config) as conn:
            cursor = conn.cursor()

            # We use julianday() to calculate the absolute difference in time.
            # This finds the 'nearest' scan regardless of if it was before or after.
            cursor.execute("""
                SELECT id, tag, timestamp,
                ABS(julianday(timestamp) - julianday(?)) as time_diff
                FROM rfid_event 
                WHERE claimed_by_session_id IS NULL 
                AND timestamp >= ? AND timestamp <= ?
                ORDER BY time_diff ASC LIMIT 1
            """, (start_timestamp, min_window, max_window))

            row = cursor.fetchone()

            if row:
                diff_sec = round(row['time_diff'] * 86400, 2)
                logging.info(f"RFID Match: {row['tag']} found for {session_id} (time difference: {diff_sec}s)")

                # Mark this tag as used so the other charging point doesn't steal it
                cursor.execute("""
                    UPDATE rfid_event 
                    SET claimed_by_session_id = ? 
                    WHERE id = ?
                """, (session_id, row['id']))

                return row['tag'], row['timestamp']
            
        # If we didn't find anything, wait 2 seconds before the next attempt, but only if we aren't on the last attempt.
        if attempt < 3:
            time.sleep(2)
            
    return None, None


def add_to_queue(config, charging_session_id: str, device_uid: str, payload: Dict[str, Any], session_type: str) -> None:
    """
    Adds a charging session event (start or end) to the SQLite queue.
    If an entry with the same charging_session_id and type already exists,
    its payload is updated, and its status is reset to 'pending' for re-transmission.

    Args:
        config: Dictionary containing configuration values.
        charging_session_id: The unique identifier for the charging session.
        device_uid: The unique identifier of the charging device.
        payload: A dictionary containing the full charging session data, which will be stored as a JSON string.
        session_type: The type of the session event, either 'start' or 'end'.
    Returns:
        None
    """
    
    # Convert payload dict to JSON string for storage
    payload_json = json.dumps(payload)
    current_time = datetime.now().isoformat()

    with get_db_connection(config) as conn:
        cursor = conn.cursor()

        # Check if a session with this charging_session_id and type already exists
        # This is crucial to avoid duplicates when retrying `save_to_queue` due to other failures
        cursor.execute("""
            SELECT id FROM charging_session
            WHERE charging_session_id = ? AND type = ?
        """, (charging_session_id, session_type))

        existing_entry = cursor.fetchone()

        if existing_entry:
            # If it exists, update it (e.g., if we are retrying an 'end' session that was already 'pending')
            # For simplicity, we'll just update the payload and reset status to pending for re-transmission
            cursor.execute("""
                UPDATE charging_session
                SET payload = ?, status = 'pending', attempts = 0, last_attempt_at = NULL, created_at = ?
                WHERE charging_session_id = ? AND type = ?
            """, (payload_json, current_time, charging_session_id, session_type))
            logging.info(f"Updated existing charging session in queue: ID: {charging_session_id}, Type: {session_type}")
        else:
            # Otherwise, insert a new entry
            cursor.execute("""
                INSERT INTO charging_session (charging_session_id, device_uid, payload, type, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (charging_session_id, device_uid, payload_json, session_type, current_time))
            logging.info(f"Added charging session to queue: ID: {charging_session_id}, Type: {session_type}")


def get_pending_queue_items(config) -> List[Dict]:
    """
    Retrieves a list of all charging session events from the queue that are
    either 'pending' or 'failed', regardless of the number of attempts.
    Items are ordered by their creation time to ensure they are processed in order.

    Args:
        config: Dictionary containing configuration values.
    Returns:
        A list of dictionaries, where each dictionary represents a queued item
        with its details (charging_session_id, device_uid, payload, type,
        attempts, last_attempt_at, and queue_db_id). Returns an empty list if no items.
    """

    with get_db_connection(config) as conn:
        cursor = conn.cursor()

        # Fetch items that are 'pending' or 'failed'. The attempt limit is removed.
        # The ORDER BY is important to process older messages first.
        cursor.execute("""
            SELECT charging_session_id, device_uid, payload, type, attempts, last_attempt_at, id
            FROM charging_session
            WHERE status IN ('pending', 'failed')
            ORDER BY created_at ASC
        """)

        rows = cursor.fetchall()

    items = []
    for row in rows:
        try:
            items.append({
                "queue_db_id": row['id'],
                "charging_session_id": row['charging_session_id'],
                "device_uid": row['device_uid'],
                "payload": json.loads(row['payload']), # Convert JSON string back to dict
                "type": row['type'],
                "attempts": row['attempts'],
                "last_attempt_at": row['last_attempt_at']
            })
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON payload for queue item: {row['id']}")

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

    current_time = datetime.now().isoformat()

    with get_db_connection(config) as conn:
        cursor = conn.cursor()

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


def get_active_session_from_queue(config, device_uid: str) -> Optional[Dict[str, Any]]:
    """
    Finds the most recent, unterminated charging session for a given device
    by looking for the latest 'start' event in the queue.

    Args:
        config: Dictionary containing configuration values.
        device_uid: The unique identifier of the charging device.
    Returns:
        A dictionary containing the 'charging_session_id' and the full 'payload'
        of the active session if one is found, otherwise None.
    """

    try:
        with get_db_connection(config) as conn:
            cursor = conn.cursor()

            # Find the latest "start" event for this device. We assume the latest one is the active one.
            # This is more robust than checking status, as an "end" event might not have been created yet.
            cursor.execute("""
                SELECT charging_session_id, payload
                FROM charging_session
                WHERE device_uid = ? AND type = 'start'
                ORDER BY created_at DESC
                LIMIT 1
            """, (device_uid,))

            row = cursor.fetchone()

            if row:
                return {
                    "charging_session_id": row['charging_session_id'],
                    "payload": json.loads(row['payload'])
                }
            
            # Return None if no row was found
            return None
        
    except Exception as e:
        logging.error(f"Could not read active session for device {device_uid} from queue: {e}")
        return None


#######################################################
############# END SQLITE QUEUE MANAGEMENT #############
#######################################################


#######################################################
############# SQLITE TELEMETRY MANAGEMENT #############
#######################################################


def update_controller_telemetry(config, device_uid: str, payload_json: str) -> None:
    """
    Persists the latest technical telemetry state for a charging controller into 
    the local SQLite database. Utilizes a flexible JSON payload column to ensure 
    data persistence remains compatible with future controller firmware updates 
    without requiring database schema migrations.

    Args:
        config: Dictionary containing configuration values, specifically file paths.
        device_uid: The unique identifier of the charging controller.
        payload_json: A stringified JSON object containing the unified telemetry data.
    Returns:
        None
    """

    current_time = datetime.now().isoformat()

    with get_db_connection(config) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO controller_telemetry (device_uid, payload, updated_at)
            VALUES (?, ?, ?)
        """, (device_uid, payload_json, current_time))


#######################################################
############# SQLITE TELEMETRY MANAGEMENT #############
#######################################################


#######################################################
############# GET LAST KNOWN DEVICE STATE #############
#######################################################


def get_last_known_controller_state(device_uid: str, config) -> Optional[str]:
    """
    Gets the last known charging state of a charging controller from the database.

    Args:
        device_uid: Charging controller ID
        config: Dictionary containing configuration values

    Returns:
        The last known charging state of the charging controller - 'connected' or 'disconnected' if successful, None if failed
    """

    try:
        with get_db_connection(config) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status FROM device_status WHERE device_uid = ?", (device_uid,))
            result = cursor.fetchone()

        if result:
            return result['status']
        
        return None # No entry found for this device UID
    
    except Exception as e:
        logging.error(f"Could not read last known state for {device_uid} from database: {e}")
        return None


###########################################################
############# END GET LAST KNOWN DEVICE STATE #############
###########################################################


#######################################################
############# SET LAST KNOWN DEVICE STATE #############
#######################################################


def set_last_known_state(device_uid: str, state: str, config) -> None:
    """
    Sets the last known charging state of a charging controller in the database.

    Args:
        device_uid: Charging controller ID
        state: The state that will be saved
        config: Dictionary containing configuration values

    Returns:
        None
    """
    
    if state not in ["connected", "disconnected"]:
        logging.warning(f"Incorrect device state provided for {device_uid}: {state}")
        return

    current_time = datetime.now().isoformat()

    try:
        with get_db_connection(config) as conn:
            cursor = conn.cursor()

            # Use INSERT OR REPLACE to create and optionally delete the existing record
            cursor.execute("""
                INSERT OR REPLACE INTO device_status (device_uid, status, updated_at)
                VALUES (?, ?, ?)
            """, (device_uid, state, current_time))
        
        logging.info(f"Setting device state in DB: deviceUid: {device_uid}, state: {state}")

    except Exception as e:
        logging.error(f"Could not set last known state for {device_uid} in database: {e}")


###########################################################
############# END SET LAST KNOWN DEVICE STATE #############
###########################################################