#######################################
# EWE Charger Agent
# VERSION 1.1.0
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


import re
import json
import time
import uuid
import gzip
import logging
import threading
import paho.mqtt.client as mqtt

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

from utils import (
    load_config,
    set_logging,
    send_request,
    initialize_queue_db,
    get_charging_point,
    get_last_known_controller_state,
    set_last_known_state,
    find_and_claim_rfid,
    get_active_session_from_queue,
    add_to_queue,
    get_pending_queue_items,
    update_queue_item_status,
    update_controller_telemetry,
    save_rfid_event
)


############################################
############# CONFIG & GLOBALS #############
############################################

config = load_config()
set_logging(config)

# Threading control
STOP_EVENT = threading.Event()
TELEMETRY_LOCK = threading.Lock()
telemetry_buffer: Dict[str, Any] = {}

# Thread pools
event_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="EV-Event")

# MQTT and API Settings
MQTT_HOST = config["Mqtt"]["Host"]
MQTT_PORT = int(config["Mqtt"]["Port"])

REST_API_HOST = config["RestApi"]["Host"]
REST_API_PORT = config["RestApi"]["Port"]

EMM_HOST = config["EmmSettings"]["Host"]
EMM_API_KEY = config["EmmSettings"]["ApiKey"]
EMM_SESSION_ENDPOINT = config["EmmSettings"].get("SessionEndpoint", "/api/v2/public/charging-session")
EMM_TELEMETRY_ENDPOINT = config["EmmSettings"].get("TelemetryEndpoint", "/api/v2/public/controller-telemetry")
EMM_HEADERS = {
    "Content-Type": "application/json",
    "Content-Encoding": "gzip",
    "Authorization": f"Bearer {EMM_API_KEY}",
}

# MQTT topics
# the "+" sign is a wildcard for any UID of the controller
TOPIC_IEC_61851_STATE = "charging_controllers/+/data/iec_61851_state"
TOPIC_ENERGY = "charging_controllers/+/data/energy"
TOPIC_RFID = "charging_controllers/+/data/rfid"

# Script start timestamp
NOW = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

################################################
############# END CONFIG & GLOBALS #############
################################################



######################################################
############# CONTROLLER TELEMETRY LOGIC #############
######################################################


def initialize_telemetry_metadata():
    """Fetches static data from the charger API once."""
    logging.info("Initializing controller metadata")

    url = f"http://{REST_API_HOST}:{REST_API_PORT}/api/v1.0/charging-controllers"
    response = send_request(url, "GET")

    if response:
        controllers = response.json()

        for device_uid, info in controllers.items():
            charging_point_url = f"http://{REST_API_HOST}:{REST_API_PORT}/api/v1.0/charging-points"
            charging_point_id, charging_point_name = get_charging_point(device_uid, charging_point_url)
            
            with TELEMETRY_LOCK:
                telemetry_buffer[device_uid] = {
                    "device_name": info["device_name"],
                    "device_type": info["device_type"],
                    "device_uid": info["device_uid"],
                    "firmware_version": info["firmware_version"],
                    "hardware_version": info["hardware_version"],
                    "parent_device_uid": info["parent_device_uid"],
                    "position": info["position"],
                    "charging_point_id": charging_point_id,
                    "charging_point_name": charging_point_name,
                    "energy": {} # Will be filled by MQTT
                }

        try:
            payload = {
                "type": "initial",
                "controllers": telemetry_buffer
            }
            payload_json = json.dumps(payload)

            compressed_data = gzip.compress(payload_json.encode("utf-8"))

            emm_response = send_request(
                url=f"{EMM_HOST}{EMM_TELEMETRY_ENDPOINT}",
                method="POST",
                headers=EMM_HEADERS,
                data=compressed_data
            )

            if not emm_response:
                logging.warning(f"Failed to send initial telemetry data for device to EMM")

        except Exception as e:
            logging.error(f"Error in telemetry initialization: {e}", exc_info=True)



def flatten_energy_data(raw_energy: Dict[str, Any]) -> Dict[str, Any]:
    """
    Strips 'name' and 'unit' from energy metrics, keeping only the raw values.
    Transforms {"u1": {"value": 240, "unit": "V", ...}} into {"u1": 240}.
    """

    cleaned = {}

    for key, val in raw_energy.items():
        # If the item is a dictionary containing a 'value' key, extract just the value
        if isinstance(val, dict) and "value" in val:
            cleaned[key] = val["value"]
        else:
            # Keep timestamp, meas_interval_sec, and energy_meter_info as they are
            cleaned[key] = val
            
    return cleaned


def on_telemetry_message(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):
    """
    Callback function executed when an MQTT telemetry message (energy JSON) is received.
    Parses the comprehensive energy metrics from the charger—including voltages, 
    currents, and power levels—and updates the in-memory buffer to keep the 
    real-time dashboard data current without disk I/O.

    Args:
        client: The MQTT client instance.
        userdata: The private user data as set in Client() or userdata_set().
        message: An MQTTMessage object containing topic and a JSON payload 
                 representing the current energy meter state.
    Returns:
        None
    """

    # Extract device_uid from the message topic
    pattern = r"/([^/]+)/"
    match = re.search(pattern, message.topic)
    device_uid = match.group(1) if match else "unknown_device"

    if device_uid == "unknown_device":
        logging.error(f"Could not extract device UID from topic: {message.topic}, skipping")
        return
    
    try:
        raw_data = json.loads(message.payload.decode("utf-8"))
        
        # Flatten the data before storing it
        energy_data = flatten_energy_data(raw_data)

        with TELEMETRY_LOCK:
            if device_uid in telemetry_buffer:
                # Update only the energy key, preserving static metadata
                telemetry_buffer[device_uid]["energy"] = energy_data
            else:
                # If metadata hasn't loaded yet, create a skeleton
                telemetry_buffer[device_uid] = {"energy": energy_data}

    except Exception as e:
        logging.error(f"Error parsing telemetry JSON: {e}")


def telemetry_heartbeat_worker():
    """
    Worker function executed in a background thread to manage telemetry delivery.
    Periodically aggregates technical data from the memory buffer with session 
    timers retrieved from the REST API, persists the unified state to the 
    local database, and transmits the pulse to the EMM system.

    Returns:
        None
    """
    
    while not STOP_EVENT.wait(timeout=10):
        # Create a local copy to minimize lock time
        with TELEMETRY_LOCK:
            current_snapshot = list(telemetry_buffer.items())

        # Build the batch of technical data
        batch_payload = {}

        for device_uid, cached_data in current_snapshot:
            try:
                # Fetch connected and charge time data from REST API
                timer_url = f"http://{REST_API_HOST}:{REST_API_PORT}/api/v1.0/charging-controllers/{device_uid}/data?param_list=iec_61851_state,connected_time_sec,charge_time_sec"
                timer_response = send_request(timer_url, "GET")

                if timer_response is None:
                    continue

                api_data = timer_response.json()
                connected_state = get_last_known_controller_state(device_uid, config)

                pulse = {
                    "device_uid": device_uid,
                    "connected_state": connected_state,
                    "iec_61851_state": api_data["iec_61851_state"],
                    "connected_time_sec": api_data["connected_time_sec"],
                    "charge_time_sec": api_data["charge_time_sec"],
                    "energy": cached_data.get("energy", {})
                }

                # Update SQLite per controller (important for local persistence)
                update_controller_telemetry(config, device_uid, json.dumps(pulse))

                batch_payload[device_uid] = pulse
                
            except Exception as e:
                logging.error(f"Error gathering telemetry for {device_uid}: {e}", exc_info=True)

        # Send the entire batch if we have data
        if batch_payload:
            try:
                payload = {
                    "type": "pulse",
                    "controllers": batch_payload
                }

                payload_json = json.dumps(payload)
                compressed_data = gzip.compress(payload_json.encode("utf-8"))

                emm_response = send_request(
                    url=f"{EMM_HOST}{EMM_TELEMETRY_ENDPOINT}",
                    method="POST",
                    headers=EMM_HEADERS,
                    data=compressed_data,
                )
                
                if not emm_response:
                    logging.warning("Failed to send telemetry batch to EMM")

            except Exception as e:
                logging.error(f"Error in telemetry heartbeat thread: {e}", exc_info=True)


##########################################################
############# END CONTROLLER TELEMETRY LOGIC #############
##########################################################



##################################################
############# CHARGING SESSION LOGIC #############
##################################################


def handle_vehicle_event_logic(vehicle_state: str, topic: str) -> None:
    """
    Perfoms the heavy lifting operations of vehicle status change - REST API, DB operations, RFID pairing.
    This functions runs in its own thread seperate from the MQTT loop.

    Args:
        vehicle_state: The IEC 61851 state received in the MQTT payload.
        topic: The MQTT message topic.

    Returns:
        None
    """

    connected_vehicle_states = ["B1", "B2", "C1", "C2", "D1", "D2"]

    # Extract device_uid from the message topic
    pattern = r"/([^/]+)/"
    match = re.search(pattern, topic)
    device_uid = match.group(1) if match else "unknown_device"

    if device_uid == "unknown_device":
        logging.error(f"Could not extract device UID from topic: {topic}, skipping")
        return
    
    # Check if this is a critical state transition
    last_known_state = get_last_known_controller_state(device_uid, config)
    is_connected_event = vehicle_state in connected_vehicle_states

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
    energy_url = f"http://{REST_API_HOST}:{REST_API_PORT}/api/v1.0/charging-controllers/{device_uid}/data?param_list=energy"
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
    charging_point_url = f"http://{REST_API_HOST}:{REST_API_PORT}/api/v1.0/charging-points"
    charging_point_id, charging_point_name = get_charging_point(device_uid, charging_point_url)

    # New charging session started
    if is_new_session_start:
        print(f"[{NOW}] EV connected! deviceUid: {device_uid}")
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
        print(f"[{NOW}] EV disconnected! deviceUid: {device_uid}")
        logging.info(f"EV disconnected from deviceUid: {device_uid}")

        # Find the active session from the queue to link the 'end' event.
        active_session = get_active_session_from_queue(config, device_uid)

        if not active_session:
            logging.warning(f"Received a disconnect event for {device_uid}, but no active session was found in the queue to end")
            set_last_known_state(device_uid, "disconnected", config)
            
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

            # Calculate the session duration and consumption, make sure it's not negative
            duration = max(0, (end_datetime - start_datetime).total_seconds())
            consumption = max(0, energy_data["energy"]["energy_real_power"]["value"] - start_real_power)

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
                "consumptionWh": consumption,
                "startTimestamp": start_timestamp,
                "endTimestamp": energy_data["energy"]["timestamp"],
                "duration": duration,
                "iec61851State": vehicle_state
            }

            # Add/Update to SQLite queue for reliable transmission
            # Here, we pass the full, updated session data.
            add_to_queue(config, charging_session_id, device_uid, data_to_update, "end")
            set_last_known_state(device_uid, "disconnected", config)

            logging.info(f"Charging session {charging_session_id} ended and queued for device {device_uid}")

        except (ValueError, KeyError) as e:
            logging.error(f"Error processing disconnected event for {device_uid}: {e}")


def on_vehicle_status_changed(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
    """
    Callback function executed when an MQTT message related to vehicle status is received.
    Extracts raw data and hands it to the ThreadPoolExecutor.
    Returns immediately to keep the MQTT loop responsive.

    Args:
        client: The MQTT client instance.
        userdata: The private user data as set in Client() or userdata_set().
        message: An MQTTMessage object containing topic, payload, qos, retain, and mid.
                 The payload contains the IEC 61851 state of the charging controller.
    Returns:
        None
    """

    try:
        vehicle_state = message.payload.decode("utf-8")
        logging.info(f"Message received from topic {message.topic}: {vehicle_state}")

        topic = message.topic
        
        # Offload the slow logic to the background
        event_executor.submit(handle_vehicle_event_logic, vehicle_state, topic)
        
    except Exception as e:
        logging.error(f"Error submitting vehicle event to executor: {e}")


def send_queued_data_worker():
    base_sleep = int(config["AppSettings"].get("QueueCheckIntervalSeconds", 30))
    max_sleep = int(config["AppSettings"].get("MaxQueueCheckIntervalSeconds", 300))
    current_sleep = base_sleep

    while not STOP_EVENT.is_set():
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
                if STOP_EVENT.is_set():
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

                target_url = f"{EMM_HOST}{EMM_SESSION_ENDPOINT}"

                payload_json = json.dumps(payload)
                compressed_data = gzip.compress(payload_json.encode("utf-8"))

                # Send to EMM API
                emm_response = send_request(
                    url=target_url,
                    method="POST",
                    headers=EMM_HEADERS,
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
            logging.error(f"Error in session sender thread: {e}", exc_info=True)

            # On error, wait for the base interval before retrying
            time.sleep(base_sleep)

    logging.info("Sender thread stopped")


#########################################################
############# CHARGING SESSION LOGIC CHANGE #############
#########################################################



#########################################
############# ON RFID EVENT #############
#########################################


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

        else:
            logging.warning(f"Received incomplete RFID data on {message.topic}: {data}")
    
    except json.JSONDecodeError:
        logging.error(f"Failed to parse RFID JSON payload: {message.payload}")

    except Exception as e:
        logging.error(f"Error processing RFID message: {e}")


#############################################
############# END ON RFID EVENT #############
#############################################



##############################################
############# MAIN EXECUTION #################
##############################################


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

        client.subscribe([(TOPIC_IEC_61851_STATE, 0), (TOPIC_ENERGY, 0), (TOPIC_RFID, 0)])

        logging.info(f"Subscribed to MQTT topics")

    else:
        logging.error(f"Failed to connect to MQTT broker with result code {rc}")


if __name__ == "__main__":
    print(f"[{NOW}] Script started")

    initialize_queue_db(config)
    initialize_telemetry_metadata()

    # MQTT client
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.message_callback_add(TOPIC_IEC_61851_STATE, on_vehicle_status_changed)
    mqtt_client.message_callback_add(TOPIC_ENERGY, on_telemetry_message)
    mqtt_client.message_callback_add(TOPIC_RFID, on_rfid_message)

    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    mqtt_client.loop_start()

    # Background daemons
    threading.Thread(target=send_queued_data_worker, daemon=True).start()
    threading.Thread(target=telemetry_heartbeat_worker, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logging.info("Script terminated by user")
    
        STOP_EVENT.set()
        event_executor.shutdown(wait=True)
        
        mqtt_client.disconnect()
        mqtt_client.loop_stop()
    
        logging.info("MQTT client disconnected and loop stopped")
    
    except Exception as e:
        logging.critical(f"An unhandled error occurred in the main loop: {e}", exc_info=True)
        
        STOP_EVENT.set()
        event_executor.shutdown(wait=True)
    
        mqtt_client.disconnect()
        mqtt_client.loop_stop()