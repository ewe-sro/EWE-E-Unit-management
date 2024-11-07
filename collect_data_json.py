#######################################
# VERSION 0.2
# DATE 07/11/2024
#
# @ 2024 EWE s.r.o.
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


############################################
############# CHARGER API CALL #############
############################################

import requests

# REST API settings
api_address = config["RestApi"]["Host"]
api_port = config["RestApi"]["Port"]


def call_charger_api(api_call):
    # Make a REST API call to get the current energy data
    api_url = f"http://{api_address}:{api_port}/api/v1.0/{api_call}"

    try:
        response = requests.get(api_url)

        # If the API response is successful
        if response.status_code == 200:
            # Parse the response JSON data
            return response.json()

        else:
            logging.error(f"COLLECT DATA: API call failed, URL: {api_url}")

            return False

    except:
        logging.error(f"COLLECT DATA: API call failed, URL: {api_url}")

        return False


################################################
############# END CHARGER API CALL #############
################################################


###################################################
############# COLLECT CONTROLLER DATA #############
###################################################


def collect_controller_data():
    # Create a dictionary object for output data
    output_data = {}

    # Get the controller data from API
    controller_url = "charging-controllers"
    controller_data = call_charger_api(controller_url)

    # Check if API call was successful
    if controller_data != False:
        # Loop over the controller data
        for controller in controller_data:

            # Get the charging point name from API
            charging_point_url = "charging-points"
            charging_point_data = call_charger_api(charging_point_url)
            charging_point_name = ""

            # Loop over the charging point data and find the correspoding controller
            for point in charging_point_data["charging_points"]:
                # If device_uid is the same as the current controller_uid
                if (
                    charging_point_data["charging_points"][point][
                        "charging_controller_device_uid"
                    ]
                    == controller
                ):
                    charging_point_id = charging_point_data["charging_points"][point][
                        "id"
                    ]
                    charging_point_name = charging_point_data["charging_points"][point][
                        "charging_point_name"
                    ]

            # Add the controller data to output dictionary
            output_data[controller] = {
                "device_name": controller_data[controller]["device_name"],
                "controller_uid": controller_data[controller]["device_uid"],
                "firmware_version": controller_data[controller]["firmware_version"],
                "hardware_version": controller_data[controller]["hardware_version"],
                "parent_device_uid": controller_data[controller]["parent_device_uid"],
                "position": controller_data[controller]["position"],
                "charging_point_id": charging_point_id,
                "charging_point_name": charging_point_name,
            }

            ###################
            ### ENERGY DATA ###
            ###################

            # Get the energy data from API
            energy_url = f"charging-controllers/{controller}/data?param_list=energy"
            energy_data = call_charger_api(energy_url)

            ######################
            ### CHARGING STATE ###
            ######################

            # Vehicle connected states
            connected_vehicle_state = ["B1", "B2", "C1", "C2", "D1", "D2"]

            # Get the connected state from API
            connected_state_url = (
                f"charging-controllers/{controller}/data?param_list=iec_61851_state"
            )
            connected_state_data = call_charger_api(connected_state_url)

            # Check if vehicle is connected to the charger
            if connected_state_data["iec_61851_state"] in connected_vehicle_state:
                connected_state = "connected"
            else:
                connected_state = "disconnected"

            ######################
            ### CONNECTED TIME ###
            ######################

            # Get the connected state from API
            connected_time_url = (
                f"charging-controllers/{controller}/data?param_list=connected_time_sec"
            )
            connected_time_data = call_charger_api(connected_time_url)

            ###################
            ### CHARGE TIME ###
            ###################

            # Get the connected state from API
            charge_time_url = (
                f"charging-controllers/{controller}/data?param_list=charge_time_sec"
            )
            charge_time_data = call_charger_api(charge_time_url)

            # Add the charging data output dictionary
            output_data[controller]["charging_data"] = energy_data["energy"]
            output_data[controller]["charging_data"]["iec_61851_state"] = (
                connected_state_data["iec_61851_state"]
            )
            output_data[controller]["charging_data"][
                "connected_state"
            ] = connected_state
            output_data[controller]["charging_data"]["connected_time_sec"] = (
                connected_time_data["connected_time_sec"]
            )
            output_data[controller]["charging_data"]["charge_time_sec"] = (
                charge_time_data["charge_time_sec"]
            )

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

from utils import save_to_emm


def controller_data_to_json():
    config = load_config()

    emm_api_host = config["EmmSettings"]["Host"]
    emm_api_key = config["EmmSettings"]["ApiKey"]
    emm_api_url = f"{emm_api_host}/api/public/controller-data"

    # Set threading timer
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
    with open(json_file_name, "w") as json_file:
        json.dump(output_data, json_file)

    # If EMM API is configured also save to EMM web app
    if emm_api_host != "" and emm_api_key != "" and emm_api_url != "":
        save_to_emm(output_data, emm_api_url, emm_api_key)


################################################################
############# END COLLECT CONTROLLER DATA FROM API #############
################################################################

controller_data_to_json()
