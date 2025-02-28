#######################################
# VERSION 0.2
# DATE 28/02/2025
#
# @ 2024 - 2025 EWE s.r.o.
# WWW: mobility.ewe.cz
#######################################


#######################################
############# LOAD CONFIG #############
#######################################

from utils import load_config

config = (
    load_config()  # Loads config from /data/user-app/charging_data/charging_data.conf
)

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

# EMM API settings
emm_api_host = config["EmmSettings"]["Host"]
emm_api_key = config["EmmSettings"]["ApiKey"]
emm_headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {emm_api_key}",
}


##############################################
############# APPLY EMM SETTINGS #############
##############################################

from typing import Dict
from utils import send_request


def apply_emm_settings() -> None:
    """
    Get charging controller settings from EMM API endpoint and apply them via the internal API.
    """
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

    # Call the EMM API and get the settings data
    controller_settings_response = send_request(
        url=f"{emm_api_host}/api/public/controller-settings",
        method="GET",
        headers=emm_headers
    )

    # If we couldn't get the controller settings from EMM exit the function
    if controller_settings_response is None:
        return None
    
    # Get the controller settings from the JSON of the response
    controller_settings: Dict[str, Dict[str, str]] = controller_settings_response.json()

    # Loop over the controller settings we got from EMM
    for controller in controller_settings:
        # Check if the controlled ID supplied by EMM is correct
        # and belongs to this charger if not skip this controller
        if controller not in controllers.keys():
            # If 'success' was returned by the API, it means that the charger was found
            # but no settings is saved in the EMM database => don't log the error
            if controller != "success":
                logging.error(f"Controller ID supplied by EMM not found, ID: {controller}")

            continue

        # Get the charging point id from the API response
        charging_point_id = controller_settings[controller]["chargingPointId"]

        # Get the settings data for later use in the controller API request
        settings_data = {
            "charging_point_name": controller_settings[controller]["settings"]["chargingPointName"],
            "location": controller_settings[controller]["settings"]["location"],
            "minimum_charge_current": controller_settings[controller]["settings"]["minimumChargeCurrent"],
            "maximum_charge_current": controller_settings[controller]["settings"]["maximumChargeCurrent"],
            "fallback_charge_current": controller_settings[controller]["settings"]["fallbackChargeCurrent"],
        }
        # Filter values which are None from the settings dictionary
        settings_data = {k: v for k, v in settings_data.items() if v is not None}

        # If we got no settings data from EMM, end the function
        if len(settings_data) == 0:
            return

        # Controller API URL for changing the charging point config
        api_url = f"http://{api_host}:{api_port}/api/v1.0/charging-points/{charging_point_id}/config"

        # Change the controller settings via the internal API
        config_response = send_request(url=api_url, method="PUT", json=settings_data)

        if config_response is None:
            return

        # Call the EMM API with POST request to let the app know
        # that the EMM settings were applied
        send_request(
            url=f"{emm_api_host}/api/public/controller-settings",
            method="POST",
            headers=emm_headers
        )


##################################################
############# END APPLY EMM SETTINGS #############
##################################################


########################################################
############# SEND CURRENT SETTINGS TO EMM #############
########################################################

from utils import get_charging_point


def sync_emm_settings() -> None:
    """
    Get the current charging point settings from the internal API and send them to EMM API endpoint.
    """
    # Get the chargers' controllers from the internal API
    controllers_response = send_request(
        url=f"http://{api_host}:{api_port}/api/v1.0/charging-controllers",
        method="GET",
        headers=emm_headers,
    )

    # Check if any controllers were found, if not exit the function
    if controllers_response is None:
        return None
    
    # Get the controllers from the JSON of the response
    controllers: Dict[str, Dict[str, str]] = controllers_response.json()

    # Loop over the controller settings we got from EMM
    for controller in controllers:
        api_url = f"http://{api_host}:{api_port}/api/v1.0/charging-points"

        # Get the controller's charging point and id
        charging_point_id, charging_point_name = get_charging_point(controller, api_url)

        # If we were unable to get charging point data log the error and end the script
        if charging_point_id is None or charging_point_name is None:
            logging.error(f"Controller ID supplied by EMM not found, ID: {controller}")
            return

        # Get the charging point config
        settings_data_response = send_request(
            url=f"http://{api_host}:{api_port}/api/v1.0/charging-points/{charging_point_id}/config",
            method="GET",
        )

        if settings_data_response is None:
            return None
        
        # Get the controllers from the JSON of the response
        settings_data: Dict[str, Dict[str, str]] = settings_data_response.json()

        # Send a PUT request to EMM to save the current settings
        send_request(
            url=f"{emm_api_host}/api/public/controller-settings/{controller}",
            method="PUT",
            headers=emm_headers,
            json=settings_data,
        )


############################################################
############# END SEND CURRENT SETTINGS TO EMM #############
############################################################


######################################################
############# SYNC THE SETTINGS WITH EMM #############
######################################################

import threading


def sync_settings_periodically():
    # Run this function every 30 seconds, start the timer
    threading.Timer(30, sync_settings_periodically).start()

    apply_emm_settings()
    sync_emm_settings()


##########################################################
############# END SYNC THE SETTINGS WITH EMM #############
##########################################################

sync_settings_periodically()