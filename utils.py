from datetime import datetime
import os

now = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

#######################################
############# LOAD CONFIG #############
#######################################

import configparser

config_path = "/data/user-app/charging_data/charging_data.conf"

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
    


########################################################
############# SQLALCHEMY DATABASE SETTINGS #############
########################################################
#        
#import sqlalchemy as db
#from models import Base
#
#def set_db(config):
#    try:
#        database_folder = config["DatabaseSettings"]["DatabaseFolder"]
#        database_name = config["DatabaseSettings"]["DatabaseName"]
#
#        engine = db.create_engine(f"sqlite:///{database_folder}{database_name}")
#
#        # Create the database schema
#        Base.metadata.create_all(engine)
#
#        logging.info(f"Successfully connected to database: sqlite:///{database_folder}{database_name}")
#
#        return engine
#    
#    except Exception as e:
#        logging.error("Error connecting to database: ", e)
#
#    except:
#        logging.error(f"Error connecting to database: sqlite:///{database_folder}{database_name}")
#        logging.error("Terminating the script")
#        exit()
#
#
############################################################
############# END SQLALCHEMY DATABASE SETTINGS #############
############################################################



###################################################
############# INSERT DATA TO DATABASE #############
###################################################
#
#from sqlalchemy.orm import sessionmaker
#
#def insert_data(data, engine):
#    # Create a session
#    Session = sessionmaker(bind=engine)
#    session = Session()
#
#    try:
#        # Check if data is a list and the data to session
#        if type(data) is list:
#            session.add_all(data)
#        else:
#            session.add(data)
#
#        # Commit the session to save changes to the database
#        session.commit()
#
#        logging.info("Record successfully added to database")
#
#    except Exception as e:
#        # Rollback the session in case of an error
#        session.rollback()
#        logging.error("Error inserting data to database: ", e)
#
#    finally:
#        # Close the session
#        session.close()
#
#######################################################
############# END INSERT DATA TO DATABASE #############
#######################################################
        


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

    # Loop over the data to find correct row for edit
    for row in current_data:
        # If the deviceUid is the same and id is higher that currently highest known id
        if row["deviceUid"] == device_uid and int(row["id"]) > highest_id:
            edit_row = row
            highest_id = int(row["id"])
            start_real_power = int(row["startRealPowerWh"])
            start_timestamp = row["startTimestamp"]
            end_timestamp = row["endTimestamp"]

    return current_data, edit_row, start_real_power, start_timestamp, end_timestamp

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
        # If id is higher than currently highest known id
        if int(row["id"]) > highest_id:
            highest_id = int(row["id"])

    return highest_id

############################################################
############# END GET HIGHEST ID FROM CSV DATA #############
############################################################