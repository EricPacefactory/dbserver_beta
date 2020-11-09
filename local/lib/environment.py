#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 15:57:04 2020

@author: eo
"""


# ---------------------------------------------------------------------------------------------------------------------
#%% Add local path

import os
import sys

def find_path_to_local(target_folder = "local"):
    
    # Skip path finding if we successfully import the dummy file
    try:
        from local.dummy import dummy_func; dummy_func(); return
    except ImportError:
        print("", "Couldn't find local directory!", "Searching for path...", sep="\n")
    
    # Figure out where this file is located so we can work backwards to find the target folder
    file_directory = os.path.dirname(os.path.abspath(__file__))
    path_check = []
    
    # Check parent directories to see if we hit the main project directory containing the target folder
    prev_working_path = working_path = file_directory
    while True:
        
        # If we find the target folder in the given directory, add it to the python path (if it's not already there)
        if target_folder in os.listdir(working_path):
            if working_path not in sys.path:
                tilde_swarm = "~"*(4 + len(working_path))
                print("\n{}\nPython path updated:\n  {}\n{}".format(tilde_swarm, working_path, tilde_swarm))
                sys.path.append(working_path)
            break
        
        # Stop if we hit the filesystem root directory (parent directory isn't changing)
        prev_working_path, working_path = working_path, os.path.dirname(working_path)
        path_check.append(prev_working_path)
        if prev_working_path == working_path:
            print("\nTried paths:", *path_check, "", sep="\n  ")
            raise ImportError("Can't find '{}' directory!".format(target_folder))
            
find_path_to_local()

# ---------------------------------------------------------------------------------------------------------------------
#%% Imports


# ---------------------------------------------------------------------------------------------------------------------
#%% Pathing functions

# .....................................................................................................................

def get_env_code_folder():
    return os.environ.get("DBSERVER_CODE_FOLDER_PATH", None)

# .....................................................................................................................

def get_env_data_folder():
    return os.environ.get("DBSERVER_DATA_FOLDER_PATH", None)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% MongoDB functions

# .....................................................................................................................

def get_mongo_protocol():
    return os.environ.get("MONGO_PROTOCOL", "mongodb")

# .....................................................................................................................

def get_mongo_host():
    return os.environ.get("MONGO_HOST", "localhost")

# .....................................................................................................................
    
def get_mongo_port():
    return int(os.environ.get("MONGO_PORT", 27017))

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% DB server functions

# .....................................................................................................................

def get_dbserver_protocol():
    return os.environ.get("DBSERVER_PROTOCOL", "http")

# .....................................................................................................................

def get_dbserver_host():
    return os.environ.get("DBSERVER_HOST", "0.0.0.0")

# .....................................................................................................................

def get_dbserver_port():
    return int(os.environ.get("DBSERVER_PORT", 8050))

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Autodelete functions

# .....................................................................................................................

def get_env_autodelete_on_startup():
    return bool(int(os.environ.get("DELETE_ON_STARTUP", 0)))

# .....................................................................................................................

def get_env_hour_to_run():
    return int(os.environ.get("HOUR_TO_RUN", 1))

# .....................................................................................................................

def get_env_upper_max_disk_usage_pct():
    return int(os.environ.get("UPPER_DISK_USAGE_PCT", 85))

# .....................................................................................................................

def get_default_max_disk_usage_pct():
    return int(os.environ.get("MAX_DISK_USAGE_PCT", 50))

# .....................................................................................................................

def get_default_days_to_keep():
    return float(os.environ.get("DAYS_TO_KEEP", 10))

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Control functions

# .....................................................................................................................

def get_debugmode():
    return bool(int(os.environ.get("DEBUG_MODE", 1)))

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    # Print out environment variables for quick checks
    print("")
    print("DEBUG_MODE:", get_debugmode())
    print("")
    print("DBSERVER_CODE_FOLDER_PATH:", get_env_code_folder())
    print("DBSERVER_DATA_FOLDER_PATH:", get_env_data_folder())
    print("")
    print("MONGO_PROTOCOL:", get_mongo_protocol())
    print("MONGO_HOST:", get_mongo_host())
    print("MONGO_PORT:", get_mongo_port())
    print("")
    print("DBSERVER_PROTOCOL:", get_dbserver_protocol())
    print("DBSERVER_HOST:", get_dbserver_host())
    print("DBSERVER_PORT:", get_dbserver_port())
    print("")


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


