#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  9 13:17:22 2020

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

from local.lib.mongo_helpers import connect_to_mongo, post_one_to_mongo
from local.lib.timekeeper_utils import time_to_epoch_ms, get_utc_datetime, datetime_to_epoch_ms

from starlette.responses import UJSONResponse
from starlette.routing import Route

from pymongo import ASCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

# .....................................................................................................................

def log_to_server(camera_select, log_type, log_data_dict):
    
    # Get timestamp to use as a log id
    log_dt = get_utc_datetime()
    log_timestamp = datetime_to_epoch_ms(log_dt)
    get_utc_datetime, datetime_to_epoch_ms
    
    # Build log entry & send to mongo
    log_entry = {"_id": log_timestamp, "log_type": log_type, "log_data": log_data_dict}
    collection_name = get_log_collection_name(log_type)
    post_success, mongo_response = post_one_to_mongo(mclient, camera_select, collection_name, log_entry)
    
    return post_success, mongo_response

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create logging routes

# .....................................................................................................................

def logs_get_all_types(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    db_ref = mclient[camera_select]
    all_collection_names = db_ref.list_collection_names()
    log_collections_iter = (each_name for each_name in all_collection_names if each_name.startswith(LOG_PREFIX))
    
    # Remove the prefix from each of the logging collections to get the type name to return
    type_idx = len(LOG_PREFIX)
    log_types_list = [each_name[type_idx:] for each_name in log_collections_iter]
    
    return UJSONResponse(log_types_list)
    
# .....................................................................................................................

def logs_since_target_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    log_type = request.path_params["log_type"]
    target_time = request.path_params["target_time"]
    target_time = int(target_time) if target_time.isnumeric() else target_time
    target_ems = time_to_epoch_ms(target_time)
    
    # Build query
    target_field = "_id"
    query_dict = {target_field: {"$gte": target_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_logging_collection(camera_select, log_type)
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, ASCENDING)
    
    return UJSONResponse(list(query_result))

# .....................................................................................................................

def logs_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    log_type = request.path_params["log_type"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert epoch inputs to integers, if needed
    start_time = int(start_time) if start_time.isnumeric() else start_time
    end_time = int(end_time) if end_time.isnumeric() else end_time
    
    # Convert times to epoch values for db lookup
    start_ems = time_to_epoch_ms(start_time)
    end_ems = time_to_epoch_ms(end_time)
    
    # Build query
    target_field = "_id"
    query_dict = {target_field: {"$gte": start_ems, "$lt": end_ems}}
    projection_dict = {}
    
    # Request data from the db
    collection_ref = get_logging_collection(camera_select, log_type)
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, ASCENDING)
    
    return UJSONResponse(list(query_result))

# .....................................................................................................................

def logs_count_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    log_type = request.path_params["log_type"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert epoch inputs to integers, if needed
    start_time = int(start_time) if start_time.isnumeric() else start_time
    end_time = int(end_time) if end_time.isnumeric() else end_time
    
    # Convert times to epoch values for db lookup
    start_ems = time_to_epoch_ms(start_time)
    end_ems = time_to_epoch_ms(end_time)
    
    # Build query
    target_field = "_id"
    query_dict = {target_field: {"$gte": start_ems, "$lt": end_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_logging_collection(camera_select, log_type)
    query_result = collection_ref.count_documents(query_dict, projection_dict)
    
    # Convert to dictionary with count
    return_result = {"count": int(query_result)}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def get_log_collection_name(log_type):
    return "".join([LOG_PREFIX, log_type])

# .....................................................................................................................

def get_logging_collection(camera_select, log_type):
    collection_name = get_log_collection_name(log_type)
    return mclient[camera_select][collection_name]

# .....................................................................................................................

def build_logging_routes():
    
    # Bundle all camera info routes
    log_url = lambda log_route: "".join(["/{camera_select:str}/logs", log_route])
    logging_routes = \
    [
     Route(log_url("/get-all-log-types"), logs_get_all_types),
     Route(log_url("/{log_type:str}/since/{target_time}"), logs_since_target_time),
     Route(log_url("/{log_type:str}/by-time-range/{start_time}/{end_time}"), logs_by_time_range),
     Route(log_url("/{log_type:str}/count/by-time-range/{start_time}/{end_time}"), logs_count_by_time_range)
    ]
    
    return logging_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Set shared log prefix indicator
LOG_PREFIX = "logs-"

# Connection to mongoDB
mclient = connect_to_mongo()


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap

