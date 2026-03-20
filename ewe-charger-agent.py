#######################################
# EWE Charger Agent
# VERSION 1.0.0 (Unified)
#
# Main service for EWE Mobility EV Chargers
# Handles: 
#   - Session Management (MQTT Events)
#   - Real-time Telemetry (MQTT Stream)
#   - Queue Management (SQLite)
#
# @ 2024 - 2026 EWE s.r.o.
# WWW: mobility.ewe.cz
#######################################


from typing import Dict, Any
from datetime import datetime

import re
import json
import time
import threading

# Global flag to signal the sender thread to stop
STOP_SENDER_THREAD = False

now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
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


##########################################
############# INITIALIZATION #############
##########################################

import paho.mqtt.client as mqtt
from utils import initialize_queue_db

# MQTT broker settings
mqtt_broker_address = config["Mqtt"]["Host"]
mqtt_broker_port = int(config["Mqtt"]["Port"])

# REST API settings
rest_api_host = config["RestApi"]["Host"]
rest_api_port = config["RestApi"]["Port"]

# EMM API settings
emm_api_host = config["EmmSettings"]["Host"]
emm_api_key = config["EmmSettings"]["ApiKey"]
emm_session_endpoint = config["EmmSettings"].get("SessionEndpoint", "/api/v2/public/charging-session")
emm_headers = {
    "Content-Type": "application/json",
    "Content-Encoding": "gzip",
    "Authorization": f"Bearer {emm_api_key}",
}

# Initialize the SQLite queue database
initialize_queue_db(config)

##############################################
############# END INITIALIZATION #############
##############################################


####################################################
############# ON VEHICLE STATUS CHANGE #############
####################################################

import uuid
from utils import (
    send_request,
    get_charging_point,
    get_last_known_controller_state,
    find_and_claim_rfid,
    get_active_session_from_queue,
    add_to_queue,
    set_last_known_state
)

# Function that is triggered on MQTT callback from the charger
def on_vehicle_status_changed(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
    """
    Callback function executed when an MQTT message related to vehicle status is received.
    Processes vehicle connection/disconnection events, retrieves relevant API data,
    and queues the data for reliable transmission to the EMM system.

    Args:
        client: The MQTT client instance.
        userdata: The private user data as set in Client() or userdata_set().
        message: An MQTTMessage object containing topic, payload, qos, retain, and mid.
                 The payload contains the IEC 61851 state of the charging controller.
    Returns:
        None
    """

    logging.info(f"Message received from topic {message.topic}")

    vehicle_state = message.payload.decode("utf-8")
    connected_vehicle_state = ["B1", "B2", "C1", "C2", "D1", "D2"]

    # Extract device_uid from the message topic
    pattern = r"/([^/]+)/"
    match = re.search(pattern, message.topic)
    device_uid = match.group(1) if match else "unknown_device"

    if device_uid == "unknown_device":
        logging.error(f"Could not extract device UID from topic: {message.topic}, skipping")
        return
    
    # Check if this is a critical state transition
    last_known_state = get_last_known_controller_state(device_uid, config)
    is_connected_event = vehicle_state in connected_vehicle_state

    # If the device is unknown, baseline it in the database
    if last_known_state is None:
        logging.info(f"First time seeing device {device_uid}. Initializing baseline state")
 
        if not is_connected_event:
            # If currently connected, assume it was disconnected so we start a session
            set_last_known_state(device_uid, "disconnected", config)
            return 
        else:
            # If currently disconnected, just save it and return
            last_known_state = "disconnected"

    # A session starts when the state becomes 'connected' AND the previous state was 'disconnected' or unknown.
    is_new_session_start = is_connected_event and last_known_state == "disconnected"
    
    # A session ends when the state is NOT 'connected' AND the previous state was 'connected'.
    is_session_end = not is_connected_event and last_known_state == "connected"

    # If this is just an intermediate state change (e.g., B1 -> B2), ignore it to save resources.
    if not is_new_session_start and not is_session_end:
        logging.debug(f"Ignoring intermediate state change '{vehicle_state}' for device {device_uid}")
        return

    # Get the starting energy data from the API
    energy_url = f"http://{rest_api_host}:{rest_api_port}/api/v1.0/charging-controllers/{device_uid}/data?param_list=energy"
    energy_response = send_request(url=energy_url, method="GET")
    
    if energy_response is None:
        logging.warning(f"Could not get energy data for {device_uid}, skipping MQTT message processing")
        return

    try:
        energy_data = energy_response.json()
    except json.JSONDecodeError:
        logging.error(f"Failed to parse energy data JSON for {device_uid}: {energy_response.text}")
        return

    # Get the charging point ID and name
    charging_point_url = f"http://{rest_api_host}:{rest_api_port}/api/v1.0/charging-points"
    charging_point_id, charging_point_name = get_charging_point(device_uid, charging_point_url)

    # New charging session started
    if is_new_session_start:
        print(f"[{now}] EV connected! deviceUid: {device_uid}")
        logging.info(f"EV connected to deviceUid: {device_uid}")

        last_known_state = get_last_known_controller_state(device_uid, config)

        charging_session_id = str(uuid.uuid4())

        # Look in our database for an RFID scanned just before this plug-in
        rfid_tag, rfid_timestamp = find_and_claim_rfid(config, charging_session_id, energy_data["energy"]["timestamp"])

        data_to_save = {
            "type": "start",
            "id": charging_session_id,
            "deviceUid": device_uid,
            "chargingPointName": charging_point_name,
            "rfidTag": rfid_tag,
            "rfidTimestamp": rfid_timestamp,
            "startRealPowerWh": energy_data["energy"]["energy_real_power"]["value"],
            "endRealPowerWh": None,
            "consumptionWh": None,
            "startTimestamp": energy_data["energy"]["timestamp"],
            "endTimestamp": None,
            "duration": None,
            "iec61851State": vehicle_state
        }

        # Add to SQLite queue for reliable transmission
        add_to_queue(config, charging_session_id, device_uid, data_to_save, "start")

        set_last_known_state(device_uid, "connected", config)
        logging.info(f"Charging session {charging_session_id} started and queued for device {device_uid}")

    # Charging session ended
    elif is_session_end:
        print(f"[{now}] EV disconnected! deviceUid: {device_uid}")
        logging.info(f"EV disconnected from deviceUid: {device_uid}")

        set_last_known_state(device_uid, "disconnected", config)

        # Find the active session from the queue to link the 'end' event.
        active_session = get_active_session_from_queue(config, device_uid)

        if not active_session:
            logging.warning(f"Received a disconnect event for {device_uid}, but no active session was found in the queue to end")
            return
            
        try:
            # Extract start-of-session data from the payload retrieved from the queue
            start_payload = active_session["payload"]
            charging_session_id = start_payload["id"]
            start_real_power = start_payload["startRealPowerWh"]

            start_timestamp = start_payload["startTimestamp"]
            end_timestamp = energy_data["energy"]["timestamp"]

            start_datetime = datetime.fromisoformat(start_timestamp)
            end_datetime = datetime.fromisoformat(end_timestamp)

            # Calculate the session duration and get the total number of seconds as a float, make sure it's not negative
            duration = max(0, (end_datetime - start_datetime).total_seconds())

            # Prefer RFID from current start event
            final_rfid_tag = start_payload.get("rfidTag") # Use .get for safety
            final_rfid_timestamp = start_payload.get("rfidTimestamp")

            # If we didn't have a tag at the start, check the buffer again
            # for any tag scanned DURING the session (Plug -> Scan scenario)
            if not final_rfid_tag:
                final_rfid_tag, final_rfid_timestamp = find_and_claim_rfid(
                    config, 
                    charging_session_id, 
                    start_timestamp
                )

            data_to_update = {
                "type": "end",
                "id": charging_session_id,
                "deviceUid": device_uid,
                "chargingPointName": start_payload.get("chargingPointName"),
                "rfidTag": final_rfid_tag,
                "rfidTimestamp": final_rfid_timestamp,
                "startRealPowerWh": start_real_power,
                "endRealPowerWh": energy_data["energy"]["energy_real_power"]["value"],
                "consumptionWh": energy_data["energy"]["energy_real_power"]["value"] - start_real_power,
                "startTimestamp": start_timestamp,
                "endTimestamp": energy_data["energy"]["timestamp"],
                "duration": duration,
                "iec61851State": vehicle_state
            }

            # Add/Update to SQLite queue for reliable transmission
            # Here, we pass the full, updated session data.
            add_to_queue(config, charging_session_id, device_uid, data_to_update, "end")

            logging.info(f"Charging session {charging_session_id} ended and queued for device {device_uid}")

        except (ValueError, KeyError) as e:
            logging.error(f"Error processing disconnected event for {device_uid}: {e}")

########################################################
############# END ON VEHICLE STATUS CHANGE #############
########################################################


################################################
############# ON TELEMETRY MESSAGE #############
################################################

def on_telemetry_message(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):
    logging.info(f"Message received from topic {message.topic}")

    # Extract device_uid from the message topic
    pattern = r"/([^/]+)/"
    match = re.search(pattern, message.topic)
    device_uid = match.group(1) if match else "unknown_device"

    if device_uid == "unknown_device":
        logging.error(f"Could not extract device UID from topic: {message.topic}, skipping")
        return
    
    try:
        # The energy payload is already a JSON string
        energy_data = json.loads(message.payload.decode("utf-8"))

    except Exception as e:
        logging.error(f"Error parsing energy JSON: {e}")

    print(energy_data)

####################################################
############# END ON TELEMETRY MESSAGE #############
####################################################


#########################################
############# ON RFID EVENT #############
#########################################

from utils import save_rfid_event

def on_rfid_message(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):
    """
    Handles incoming RFID tag and timestamp updates.

    Args:
        client: The MQTT client instance.
        userdata: The private user data as set in Client() or userdata_set().
        message: An MQTTMessage object containing topic, payload, qos, retain, and mid.
                 The payload contains the IEC 61851 state of the charging controller.
    Returns:
        None
    """

    logging.info(f"Message received from topic {message.topic}")

    try:
        # Parse the MQTT message that's in this format: {"tag": "XXXXX", "timestamp": "2026-03-13T08:36:47"}
        data = json.loads(message.payload.decode("utf-8"))

        tag = data.get("tag")
        timestamp = data.get("timestamp")

        if tag and timestamp:
            save_rfid_event(config, tag, timestamp)
            logging.info(f"Saved RFID scan to database: {tag} at {timestamp}")
        else:
            logging.warning(f"Received incomplete RFID data on {message.topic}: {data}")
    
    except json.JSONDecodeError:
        logging.error(f"Failed to parse RFID JSON payload: {message.payload}")
    except Exception as e:
        logging.error(f"Error processing RFID message: {e}")

#############################################
############# END ON RFID EVENT #############
#############################################


########################################################
############# BACKGROUND SENDER THREAD #################
########################################################

from utils import get_pending_queue_items, update_queue_item_status
import gzip

def send_queued_data_worker():
    global STOP_SENDER_THREAD
    logging.info("Sender thread started")

    base_sleep = int(config["AppSettings"].get("QueueCheckIntervalSeconds", 30))
    max_sleep = int(config["AppSettings"].get("MaxQueueCheckIntervalSeconds", 300))
    current_sleep = base_sleep

    while not STOP_SENDER_THREAD:
        try:
            pending_items = get_pending_queue_items(config)

            if not pending_items:
                # Queue is empty, sleep longer next time
                time.sleep(current_sleep)
                current_sleep = min(current_sleep * 2, max_sleep)

                continue

            # If we get here, work was found. Reset the sleep interval for the next idle cycle.
            current_sleep = base_sleep

            for item in pending_items:
                if STOP_SENDER_THREAD:
                    break

                queue_db_id = item["queue_db_id"]
                charging_session_id = item["charging_session_id"]
                device_uid = item["device_uid"]
                payload: Dict = item["payload"]
                session_type = item["type"]
                attempts = item["attempts"]

                # Add the charger's current time to the payload at the moment of sending
                item["payload"]["sentTimestamp"] = datetime.now().isoformat()

                logging.info(f"Attempting to send queued item (ID: {charging_session_id}, Type: {session_type}, Attempts: {attempts}) for device {device_uid}")

                target_url = f"{emm_api_host}{emm_session_endpoint}"

                # Convert payload dict to JSON string
                payload_json = json.dumps(payload)

                # Compress the JSON payload via gzip
                compressed_data = gzip.compress(payload_json.encode("utf-8"))

                # Send to EMM API
                emm_response = send_request(
                    url=target_url,
                    method="POST",
                    headers=emm_headers,
                    data=compressed_data,
                )

                if emm_response:
                    logging.info(f"Successfully sent queued item (ID: {charging_session_id}, Type: {session_type}) for device {device_uid} to EMM")
                    update_queue_item_status(config, queue_db_id, "sent")
                else:
                    logging.warning(f"Failed to send queued item (ID: {charging_session_id}, Type: {session_type}) for device {device_uid} to EMM")
                    update_queue_item_status(config, queue_db_id, "failed", increment_attempts=True)

                # Add a small delay between sending items to avoid hammering the API
                time.sleep(1)

            # After processing a batch, wait for the base interval before checking again.
            time.sleep(base_sleep)

        except Exception as e:
            logging.error(f"Error in sender thread: {e}", exc_info=True)

            # On error, wait for the base interval before retrying
            time.sleep(base_sleep)

    logging.info("Sender thread stopped")


########################################################
############# END BACKGROUND SENDER THREAD #############
########################################################


#########################################################
############# MQTT CLIENT CONFIGURATION #################
#########################################################

# the "+" sign is a wildcard for any UID of the controller
iec_61851_state_topic = "charging_controllers/+/data/iec_61851_state"
telemetry_topic = "charging_controllers/+/data/energy"
rfid_topic = "charging_controllers/+/data/rfid"

# Callback when the client connects to the broker
def on_connect(client: mqtt.Client, userdata: Any, flags, rc: int):
    """
    Callback function executed when the MQTT client connects to the broker.

    Args:
        client: The MQTT client instance.
        userdata: The private user data as set in Client() or userdata_set().
        flags: Response flags sent by the broker. (e.g., {'session present': 0} for clean session, 1 for unclean session).
      The value of rc determines success or not:
        0: Connection successful
        1: Connection refused - incorrect protocol version
        2: Connection refused - invalid client identifier
        3: Connection refused - server unavailable
        4: Connection refused - bad username or password
        5: Connection refused - not authorised
        6-255: Currently unused.

    Returns:
        None
    """

    # If the connection was successfull
    if rc == 0:
        logging.info("Connected to MQTT broker")

        client.subscribe(iec_61851_state_topic)
        #client.subscribe(telemetry_topic)
        client.subscribe(rfid_topic)

        logging.info(f"Subscribed to IEC 61851 state and RFID MQTT topics")

    else:
        logging.error(f"Failed to connect to MQTT broker with result code {rc}")


mqtt_client = mqtt.Client()

# Set up callbacks
mqtt_client.on_connect = on_connect
mqtt_client.message_callback_add(iec_61851_state_topic, on_vehicle_status_changed)
#mqtt_client.message_callback_add(telemetry_topic, on_telemetry_message)
mqtt_client.message_callback_add(rfid_topic, on_rfid_message)

# Connect to the broker
mqtt_client.connect(mqtt_broker_address, mqtt_broker_port, 60)

# Start the MQTT loop to handle incoming messages
mqtt_client.loop_start()

#############################################################
############# END MQTT CLIENT CONFIGURATION #################
############################################################


# Start the background sender thread
sender_thread = threading.Thread(target=send_queued_data_worker)

# Allow main program to exit even if thread is still running
sender_thread.daemon = True

sender_thread.start()
logging.info("Background sender thread initiated")

# Keep the script running
try:
    while True:
        time.sleep(1) # Main thread can do other light work or just sleep

except KeyboardInterrupt:
    logging.info("Script terminated by user")

    # Signal the sender thread to stop
    STOP_SENDER_THREAD = True

    # Wait for the sender thread to finish gracefully
    sender_thread.join(timeout=5)

    if sender_thread.is_alive():
        logging.warning("Sender thread did not stop gracefully, forcing exit")
    
    mqtt_client.disconnect()
    mqtt_client.loop_stop()

    logging.info("MQTT client disconnected and loop stopped")

except Exception as e:
    logging.critical(f"An unhandled error occurred in the main loop: {e}", exc_info=True)
    
    # Signal the sender thread to stop
    STOP_SENDER_THREAD = True

    # Wait for the sender thread to finish gracefully
    sender_thread.join(timeout=5)

    mqtt_client.disconnect()
    mqtt_client.loop_stop()