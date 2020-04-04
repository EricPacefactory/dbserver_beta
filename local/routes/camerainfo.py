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
from local.lib.timekeeper_utils import time_to_epoch_ms
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
    collection_ref = mclient[camera_select]["camerainfo"]
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
    collection_ref = mclient[camera_select]["camerainfo"]
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
    collection_ref = mclient[camera_select]["camerainfo"]
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, DESCENDING).limit(1)
    
    # Try to get the newest data, from the given list (which may be empty!)
    return_result = first_of_query(query_result, return_if_missing = None)
    empty_query = (return_result is None)
    if empty_query:
        error_message = "No camera info before time {}".format(target_ems)
        return no_data_response(error_message)
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_camerainfo_routes():
    
    # Bundle all camera info routes
    caminfo_url = lambda caminfo_route: "".join(["/{camera_select:str}/camerainfo", caminfo_route])
    camerainfo_routes = \
    [
     Route(caminfo_url("/get-all-camera-info"), caminfo_get_all_info),
     Route(caminfo_url("/get-newest-camera-info"), caminfo_get_newest_info),
     Route(caminfo_url("/get-relative-camera-info/{target_time}"), caminfo_get_relative_info),
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


