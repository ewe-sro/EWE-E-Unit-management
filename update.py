#######################################
# VERSION 0.1
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


# EMM API settings
emm_api_host = config["EmmSettings"]["Host"]
emm_api_key = config["EmmSettings"]["ApiKey"]
emm_headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {emm_api_key}",
}


###################################################
############# SAVE FILE TO FYLESYSTEM #############
###################################################

import os

# The directory in which the updated script files will be saved
software_directory = "/data/user-app/"


def save_file(path: str, content: bytes):
    """
    Save file to the filesystem. Also checks if the provided file differs from the current file if it exists.

    Args:
        path: The full filesystem path, where the updated file should be saved
        content: The content of the updated file
    Returns:
        True if successfully processed, None if failed
    """

    # The file on the controller needs to save inside '/data/user-app'
    # which is the dedicated directory for saving custom software
    # see CHARX SEC-XXXX manual - 2.5.2 Directory structure and accessing the file system
    if not software_directory in path:
        logging.error(
            f"The file needs to be save inside {software_directory}, path: {path}"
        )

        return None

    # If the file doesn't exists, create it
    if not os.path.exists(path):
        # Write the file to the filesystem
        with open(path, "wb") as file:  # Binary mode
            file.write(content)

        logging.info(f"New script file was saved to the filesystem, path: {path}")

        return True

    with open(path, "rb") as file:
        file_content = file.read()

    # If the contents of the existing file and the new file are different, write the new file
    if content != file_content:
        with open(path, "wb") as file:  # Binary mode
            file.write(content)

        logging.info(f"Script file was updated, path: {path}")

        return True


#######################################################
############# END SAVE FILE TO FYLESYSTEM #############
#######################################################


######################################################
############# TERMINATE A SCRIPT PROCESS #############
######################################################

import psutil
import subprocess

# The path to python executable
python_path = "/usr/bin/python3"


def terminate_script_process(path: str):
    """
    Terminate a script process.

    Args:
        path: The full filesystem path of the script that should be terminated
    Returns:
        None
    """
    process_name = f"{python_path} {path}"

    for proc in psutil.process_iter(attrs=["pid", "cmdline"]):
        try:
            cmdline = " ".join(proc.info["cmdline"])  # Join cmdline args into a string
            if (
                process_name in cmdline
            ):  # Check if the script name appears in the command line
                proc.terminate()  # Stop the process gracefully

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass  # Process may have ended before we accessed it


##########################################################
############# END TERMINATE A SCRIPT PROCESS #############
##########################################################


############################################
############# START THE SCRIPT #############
############################################


def start_script_process(path: str):
    """
    Start a script in a new process.

    Args:
        path: The full filesystem path of the script that should be started automatically
    Returns:
        None
    """

    # Start a process as a new independent instance
    subprocess.Popen(
        [python_path, path],
        start_new_session=True,  # Equivalent to nohup: make the process immune to hangups
        close_fds=True,  # Close parent file descriptors in child process
        stdout=open("/dev/null", "w"),  # Prevents terminal-related failures
        stderr=open("/dev/null", "w"),
    )


################################################
############# END START THE SCRIPT #############
################################################


##########################################################
############# START THE SCRIPT AUTOMATICALLY #############
##########################################################

# The user-application-start file, that is used for declaring
# which scripts should be started automatically on charger startup
user_application_start = "/data/user-app/user-application-start"


def start_script_automatically(path: str):
    """
    Edit the user-application-start file so the script is started automatically on charger startup.
    Also checks if the file is not started automatically already.

    Args:
        path: The full filesystem path of the script that should be started automatically
    Returns:
        True if successfully processed, None if failed
    """

    # If the user-application-start file doesn't exists,
    # log the error and exit the function
    if not os.path.exists(user_application_start):
        logging.error(
            f"user-application-start file not found, path: {user_application_start}"
        )

        return None

    # Open the user-application-start file in read/write mode
    with open(user_application_start, "r+") as file:
        content = file.read()

        # If the file is not started automatically already
        # add it to the user-application-start file
        if path not in content:
            # Move to end of file before writing
            file.seek(0, 2)  # 2 means seek relative to end of file

            # If file is not empty (has content), write a new line before writing the path
            if content:
                file.write("\n")

            file.write(f"{python_path} {path} &")

        return True


##############################################################
############# END START THE SCRIPT AUTOMATICALLY #############
##############################################################


#################################################
############# SETUP UPDATE CRON JOB #############
#################################################

import re


def is_valid_cron(cron_expression: str) -> bool:
    """Validates a cron expression format using regex."""

    # Regex pattern for each field (minute, hour, day, month, weekday)
    cron_pattern = (
        r"^([0-5]?\d|\*|(\d+[-/]\d+)|(\d+(,\d+)*))\s"  # Minutes (0-59)
        r"([0-2]?\d|\*|(\d+[-/]\d+)|(\d+(,\d+)*))\s"  # Hours (0-23)
        r"([1-9]|[12]\d|3[01]|\*|(\d+[-/]\d+)|(\d+(,\d+)*))\s"  # Day of Month (1-31)
        r"([1-9]|1[0-2]|\*|(\d+[-/]\d+)|(\d+(,\d+)*))\s"  # Month (1-12)
        r"([0-6]|\*|(\d+[-/]\d+)|(\d+(,\d+)*))$"  # Day of Week (0-6, Sunday=0)
    )

    return bool(re.match(cron_pattern, cron_expression.strip()))


def setup_cron_job(cron_expression: str, path: str):
    """
    Checks if a cron job exists with the given command
    and create it if not found using native Linux commands.

    Args:
        cron_expression: Cron expression for scheduling the cron job
        path: The full filesystem path of the script that should be started with cron job
    Returns:
        True if successfully processed, None if failed
    """

    if not is_valid_cron(cron_expression):
        # If invalid cron expression was provided, log an erorr and exit the function
        logging.error(f"Invalid cron expression provided: {cron_expression}")

        return None

    # The cron job row to be added
    cron_job = f"{cron_expression} {python_path} {path}"

    # Get the current crontab
    try:
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True, check=True
        )
        cron_jobs = result.stdout
    except subprocess.CalledProcessError:
        cron_jobs = ""  # If crontab is empty, treat it as an empty string

    # Check if the job already exists
    if cron_job not in cron_jobs:
        # Append the new job - handle empty crontab case
        if cron_jobs:
            new_cron_jobs = cron_jobs + f"\n{cron_job}\n"
        else:
            new_cron_jobs = f"{cron_job}\n"

        try:
            # Write the updated crontab
            subprocess.run(["crontab", "-"], input=new_cron_jobs, text=True, check=True)

            logging.info(f"Cron job was created: {cron_job}")
            print(f"Cron job was created: {cron_job}")

            return True

        except subprocess.CalledProcessError as err:
            # If updating the crontab failed log the error and exit the function
            logging.error(f"Failed to update crontab: {err}")

            return None

    return True  # Cronjob already exists


#####################################################
############# END SETUP UPDATE CRON JOB #############
#####################################################


###############################################
############# UPDATE SCRIPT FILES #############
###############################################

from utils import send_request
from typing import Dict, Union


def update_scripts():
    """
    Gets a list of Python scripts from EMM API endpoint and either updates existing file or saves the file.
    Also terminates any existing processes for the file and starts a new process.
    """
    print("Getting a list of files to update")

    # Call the EMM API and get the script file names and where they should be saved
    scripts_response = send_request(
        url=f"{emm_api_host}/api/public/script", method="GET", headers=emm_headers
    )

    if scripts_response is None:
        return None

    # Get the JSON data we got from the EMM APi
    scripts: Dict[str, Dict[str, Union[str, bool]]] = scripts_response.json()

    # Loop over the script names we got from the API
    for script_name in scripts:
        print(f"Updating script file: {script_name}")

        # Call the EMM API and get the script file
        file_response = send_request(
            url=f"{emm_api_host}/api/public/script/{script_name}",
            method="GET",
            headers=emm_headers,
        )

        # If the API request wasn't successful go to the next file
        if file_response is None:
            continue

        # Full path to the file
        file_path = f'{scripts[script_name]["directory"]}/{script_name}'

        # Get the file content in binary data
        file_content = file_response.content

        # Save the file to the filesystem (also check if the file is changed)
        file_saved = save_file(file_path, file_content)

        # If the file couldn't be saved go to the next file
        if file_saved is None:
            continue

        # Setup a cron job if necessary
        if scripts[script_name]["cron"]["enabled"] is True:
            setup_cron_job(scripts[script_name]["cron"]["expression"], file_path)

        # If the file shouldn't be started automatically, go to the next file
        if scripts[script_name]["persistent"] is False:
            continue

        # Configure the file to be started automatically
        start_script_automatically(file_path)

        # Terminate the script process with the same name, if it exists
        terminate_script_process(file_path)

        # Start the script process for the newly update file
        start_script_process(file_path)


###################################################
############# END UPDATE SCRIPT FILES #############
###################################################

update_scripts()
