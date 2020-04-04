#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 15:03:58 2020

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

from local.lib.mongo_helpers import connect_to_mongo
from local.lib.timekeeper_utils import time_to_epoch_ms
from local.lib.response_helpers import first_of_query, bad_request_response, no_data_response

from starlette.responses import UJSONResponse
from starlette.routing import Route

from pymongo import ASCENDING, DESCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Create object routes

# .....................................................................................................................

def objects_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Build query
    target_field = "_id"
    query_dict = {}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, DESCENDING).limit(1)
    
    # Pull out a single entry (there should only be one)
    return_result = first_of_query(query_result)
    if return_result is None:
        error_message = "No object metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_ids_at_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_time = int(target_time) if target_time.isnumeric() else target_time
    target_ems = time_to_epoch_ms(target_time)
    
    # Build query
    target_field = "_id"
    query_dict = {"first_epoch_ms": {"$lte": target_ems}, "final_epoch_ms": {"$gte": target_ems}}
    projection_dict = {}
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict)
    
    # Convert to list of ids only
    return_result = [each_entry[target_field] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_ids_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
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
    query_dict = {"first_epoch_ms": {"$lt": end_ems}, "final_epoch_ms": {"$gt": start_ems}}
    projection_dict = {}
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, ASCENDING)
    
    # Pull out the epoch values into a list, instead of returning a list of dictionaries
    return_result = [each_entry[target_field] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    object_full_id = int(request.path_params["object_full_id"])
    
    # Build query
    query_dict = {"_id": object_full_id}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find_one(query_dict, projection_dict)
    
    # Deal with missing data
    if not query_result:
        error_message = "No object with id {}".format(object_full_id)
        return bad_request_response(error_message)
    
    return UJSONResponse(query_result)

# .....................................................................................................................

def objects_get_many_metadata_at_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_time = int(target_time) if target_time.isnumeric() else target_time
    target_ems = time_to_epoch_ms(target_time)
    
    # Build query
    query_dict = {"first_epoch_ms": {"$lte": target_ems}, "final_epoch_ms": {"$gte": target_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict)
    
    # Convert to dictionary, with object ids as keys
    filter_key = "full_id"
    return_result = {each_result[filter_key]: each_result for each_result in query_result}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_many_metadata_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert epoch inputs to integers, if needed
    start_time = int(start_time) if start_time.isnumeric() else start_time
    end_time = int(end_time) if end_time.isnumeric() else end_time
    
    # Convert times to epoch values for db lookup
    start_ems = time_to_epoch_ms(start_time)
    end_ems = time_to_epoch_ms(end_time)
    
    # Build query
    query_dict = {"first_epoch_ms": {"$lt": end_ems}, "final_epoch_ms": {"$gt": start_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict)
    
    # Convert to dictionary, with object ids as keys
    filter_key = "full_id"
    return_result = {each_result[filter_key]: each_result for each_result in query_result}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_object_routes():
    
    # Bundle all object routes
    obj_url = lambda obj_route: "".join(["/{camera_select:str}/objects", obj_route])
    object_routes = \
    [
     Route(obj_url("/get-newest-metadata"), objects_get_newest_metadata),
     Route(obj_url("/get-ids-list/by-time-target/{target_time}"), objects_get_ids_at_time),
     Route(obj_url("/get-ids-list/by-time-range/{start_time}/{end_time}"), objects_get_ids_by_time_range),
     Route(obj_url("/get-one-metadata/by-id/{object_full_id:int}"), objects_get_one_metadata_by_id),
     Route(obj_url("/get-many-metadata/by-time-target/{target_time}"), objects_get_many_metadata_at_time),
     Route(obj_url("/get-many-metadata/by-time-range/{start_time}/{end_time}"), objects_get_many_metadata_by_time_range),
    ]
    
    return object_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Connection to mongoDB
mclient = connect_to_mongo()


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


