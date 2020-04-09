#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 15:21:42 2020

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
from local.lib.timekeeper_utils import time_to_epoch_ms,get_deletion_by_days_to_keep_timing
from local.lib.response_helpers import first_of_query, no_data_response

from starlette.responses import UJSONResponse
from starlette.routing import Route

from pymongo import DESCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Create camera info routes

# .....................................................................................................................

def caminfo_get_all_info(request):
    
    '''
    Returns all camera info for a given camera. This will include entries from every time the camera is reset.
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Build query
    query_dict = {}
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_camera_info_collection(camera_select)
    query_result = collection_ref.find(query_dict, projection_dict)
    
    return UJSONResponse(list(query_result))

# .....................................................................................................................

def caminfo_get_newest_info(request):
    
    '''
    Returns the newest camera info entry for a specific camera.
    Note that this may not be the best thing to do if working with a specific alarm time!
    A better option is the 'relative' info entry, which will take into account camera reset events
    (and possible reconfiguration associated with those events)
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Build query
    target_field = "_id"
    query_dict = {}
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_camera_info_collection(camera_select)
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, DESCENDING).limit(1)
    
    # Pull out only the newest entry & handle missing data
    return_result = first_of_query(query_result)
    if return_result is None:
        error_message = "No camera info for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def caminfo_get_relative_info(request):
    
    ''' 
    Returns the camera info that was most applicable to the given target time
    For example, 
      If a camera started on 2020-01-01, ran until 2020-01-05, then was restarted so the snapshots could be resized.
      The camera would dump a camera info entry on 2020-01-01 and again on 2020-01-05 when it restarts.
      However, if something about the camera was changed in that time period (e.g. snapshot frame sizing),
      then alarms between 2020-01-01 and 2020-01-05 would need to reference 
      the 2020-01-01 camera info to get the correct information.
      Given an input time between 2020-01-01 to 2020-01-05, this function will return the info from 2020-01-01
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_time = int(target_time) if target_time.isnumeric() else target_time
    target_ems = time_to_epoch_ms(target_time)
    
    # Build query
    target_field = "_id"
    query_dict = {target_field: {"$lte": target_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_camera_info_collection(camera_select)
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, DESCENDING).limit(1)
    
    # Try to get the newest data from the given list (which may be empty!)
    return_result = first_of_query(query_result, return_if_missing = None)
    empty_query = (return_result is None)
    if empty_query:
        error_message = "No camera info before time {}".format(target_ems)
        return no_data_response(error_message)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def caminfo_get_many_info(request):
    
    '''
    Returns a list of camera info entries, given an input start and end time range.
    The returned values from this route act similar to the 'relative info' route, in that only camera info
    which was relative to the provided time range will be returned.
    This includes all camera info generated during the given time period + the closest camera info
    generate before the given start time (if applicable).
    See 'caminfo_get_relative_info' route for more information.
    '''
    
    # Initialize return value
    many_caminfo_list = []
    
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
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Find first camera info that occurred before the provided range (since it is relevant to the range)
    
    # Build query to get earliest camera info first (i.e. the closest info before the given start time)
    target_field = "_id"
    query_dict = {target_field: {"$lte": start_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_camera_info_collection(camera_select)
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, DESCENDING).limit(1)
    
    # Try to get the newest data from the given list (which may be empty!)
    caminfo_before_start_time = first_of_query(query_result, return_if_missing = None)
    have_early_caminfo = (caminfo_before_start_time is not None)
    if have_early_caminfo:
        many_caminfo_list.append(caminfo_before_start_time)
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Find camera info within the provided range, and return it along with the first info before the range
    
    # Build query for camera info that was generated during the given time period
    target_field = "_id"
    query_dict = {target_field: {"$gt": start_ems, "$lt": end_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_camera_info_collection(camera_select)
    query_result = collection_ref.find(query_dict, projection_dict)
    
    # Add entries that occured during the time range to the list
    caminfo_in_range = list(query_result)
    have_caminfo_in_range = (len(caminfo_in_range) > 0)
    if have_caminfo_in_range:
        many_caminfo_list += caminfo_in_range
    
    # Handle missing data case
    no_caminfo_for_range = (len(many_caminfo_list) == 0)
    if no_caminfo_for_range:
        error_message = "No camera info for provided time range! Camera likely started later!"
        return no_data_response(error_message)
    
    return UJSONResponse(many_caminfo_list)

# .....................................................................................................................

def caminfo_delete_by_days_to_keep(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    days_to_keep = request.path_params["days_to_keep"]
    
    # Get timing needed to handle deletions
    oldest_allowed_dt, oldest_allowed_ems, deletion_datetime_str = get_deletion_by_days_to_keep_timing(days_to_keep)
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Find oldest camera info that we need to keep (would have occurred before deletion target time)
    
    # Find the first camera info before the target deletion time (which we'll need to keep)
    target_field = "_id"
    query_dict = {target_field: {"$lte": oldest_allowed_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_camera_info_collection(camera_select)
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, DESCENDING).limit(1)
    
    # Try to get the newest data from the given list (which may be empty!)
    return_result = first_of_query(query_result, return_if_missing = None)
    empty_query = (return_result is None)
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Build & execute the deletion command
    
    # Build new deletion target time, if possible, which deletes all camera info before the oldest one we need to keep
    oldest_camera_info_ems = oldest_allowed_ems
    if not empty_query:
        oldest_camera_info_ems = return_result["start_epoch_ms"] - 1000
    
    # Build filter
    target_field = "_id"
    filter_dict = {target_field: {"$lt": oldest_camera_info_ems}}
    
    # Send deletion command to the db
    collection_ref = get_camera_info_collection(camera_select)
    delete_response = collection_ref.delete_many(filter_dict)
    
    # Build output to provide feedback about deletion
    return_result = {"deletion_datetime": deletion_datetime_str,
                     "deletion_epoch_ms": oldest_allowed_ems,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def get_camera_info_collection(camera_select):
    return mclient[camera_select]["camerainfo"]

# .....................................................................................................................

def build_camerainfo_routes():
    
    # Bundle all camera info routes
    caminfo_url = lambda caminfo_route: "".join(["/{camera_select:str}/camerainfo", caminfo_route])
    camerainfo_routes = \
    [
     Route(caminfo_url("/get-all-camera-info"), caminfo_get_all_info),
     Route(caminfo_url("/get-newest-camera-info"), caminfo_get_newest_info),
     Route(caminfo_url("/get-relative-camera-info/{target_time}"), caminfo_get_relative_info),
     Route(caminfo_url("/get-many-camera-info/{start_time}/{end_time}"), caminfo_get_many_info),
     Route(caminfo_url("/delete/by-time/{days_to_keep:int}"), caminfo_delete_by_days_to_keep)
    ]
    
    return camerainfo_routes

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


