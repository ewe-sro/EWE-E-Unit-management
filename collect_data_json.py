#######################################
# VERSION 0.3
# DATE 28/02/2025
#
# @ 2024 - 2025 EWE s.r.o.
# WWW: mobility.ewe.cz
#######################################


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


# REST API settings
api_host = config["RestApi"]["Host"]
api_port = config["RestApi"]["Port"]


###################################################
############# COLLECT CONTROLLER DATA #############
###################################################

from typing import Dict
from utils import send_request, get_charging_point

def collect_controller_data():
    # Create a dictionary object for output data
    output_data = {}

    # Get the chargers' controllers from the internal API
    controllers_response = send_request(
        url=f"http://{api_host}:{api_port}/api/v1.0/charging-controllers",
        method="GET"
    )

    # Check if the controllers request was successful, if not exit the function
    if controllers_response is None:
        return None
    
    # Get the controllers from the JSON of the response
    controllers: Dict[str, Dict[str, str]] = controllers_response.json()

    # Loop over the controller data
    for controller in controllers:

        # Get the charging point name from API
        charging_point_url = f"http://{api_host}:{api_port}/api/v1.0/charging-points"
        
        # Set the variables for charging point data
        charging_point_id, charging_point_name = get_charging_point(controller, charging_point_url)

        # Add the controller data to output dictionary
        output_data[controller] = {
            "device_name": controllers[controller]["device_name"],
            "controller_uid": controllers[controller]["device_uid"],
            "firmware_version": controllers[controller]["firmware_version"],
            "hardware_version": controllers[controller]["hardware_version"],
            "parent_device_uid": controllers[controller]["parent_device_uid"],
            "position": controllers[controller]["position"],
            "charging_point_id": charging_point_id,
            "charging_point_name": charging_point_name,
        }

        ###################
        ### ENERGY DATA ###
        ###################

        # Get the data the internal API
        energy_response = send_request(
            url=f"http://{api_host}:{api_port}/api/v1.0/charging-controllers/{controller}/data?param_list=energy",
            method="GET"
        )

        # Check if the request was successful, if not exit the function
        if energy_response is None:
            return None
        
        # Get the data from the JSON of the response
        energy = energy_response.json()


        ######################
        ### CHARGING STATE ###
        ######################

        # Vehicle connected states
        connected_vehicle_state = ["B1", "B2", "C1", "C2", "D1", "D2"]

        # Get the connected state the internal API
        connected_state_response = send_request(
            url=f"http://{api_host}:{api_port}/api/v1.0/charging-controllers/{controller}/data?param_list=iec_61851_state",
            method="GET"
        )
        
        # Check if the request was successful, if not exit the function
        if connected_state_response is None:
            return None
        
        # Get the data from the JSON of the response
        connected_state_data = connected_state_response.json()

        # Check if vehicle is connected to the charger
        if connected_state_data["iec_61851_state"] in connected_vehicle_state:
            connected_state = "connected"

        else:
            connected_state = "disconnected"


        ######################
        ### CONNECTED TIME ###
        ######################

        # Get the connected time the internal API
        connected_time_response = send_request(
            url=f"http://{api_host}:{api_port}/api/v1.0/charging-controllers/{controller}/data?param_list=connected_time_sec",
            method="GET"
        )

        # Check if the request was successful, if not exit the function
        if connected_time_response is None:
            return None
        
        # Get the data from the JSON of the response
        connected_time = connected_time_response.json()


        ###################
        ### CHARGE TIME ###
        ###################

        # Get the connected time the internal API
        charge_time_response = send_request(
            url=f"http://{api_host}:{api_port}/api/v1.0/charging-controllers/{controller}/data?param_list=charge_time_sec",
            method="GET"
        )

        # Check if the request was successful, if not exit the function
        if charge_time_response is None:
            return None
        
        # Get the data from the JSON of the response
        charge_time = charge_time_response.json()


        # Add the charging data output dictionary
        output_data[controller]["charging_data"] = energy["energy"]
        output_data[controller]["charging_data"]["iec_61851_state"] = connected_state_data["iec_61851_state"]
        output_data[controller]["charging_data"]["connected_state"] = connected_state
        output_data[controller]["charging_data"]["connected_time_sec"] = connected_time["connected_time_sec"]
        output_data[controller]["charging_data"]["charge_time_sec"] = charge_time["charge_time_sec"]

    return output_data


#######################################################
############# END COLLECT CONTROLLER DATA #############
#######################################################


############################################################
############# COLLECT CONTROLLER DATA FROM API #############
############################################################

import os
import json
import threading

# EMM API settings
emm_api_host = config["EmmSettings"]["Host"]
emm_api_key = config["EmmSettings"]["ApiKey"]
emm_headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {emm_api_key}",
}

def controller_data_to_json():
    # Run this function every 5 seconds, start the timer
    threading.Timer(5, controller_data_to_json).start()

    output_data = collect_controller_data()

    # Set the data folder and file name
    data_folder_path = config["AppSettings"]["FileFolder"]

    # Check if folder for data DOESN'T exists
    if not os.path.isdir(data_folder_path):
        # Create the data folder
        os.makedirs(data_folder_path)

    json_file_name = data_folder_path + "controller_data.json"

    # Save the collected data to JSON file
    with open(json_file_name, "w", encoding="utf-8") as json_file:
        json.dump(output_data, json_file)

    # If EMM API is configured also save to EMM web app
    if emm_api_host != "" and emm_api_key != "":
        send_request(
            url=f"{emm_api_host}/api/public/controller-data",
            method="POST",
            headers=emm_headers,
            data=json.dumps(output_data)
        )


################################################################
############# END COLLECT CONTROLLER DATA FROM API #############
################################################################


controller_data_to_json()