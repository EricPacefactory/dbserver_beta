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

from local.lib.mongo_helpers import MCLIENT

from local.lib.query_helpers import url_time_to_epoch_ms, start_end_times_to_epoch_ms
from local.lib.query_helpers import get_newest_metadata, get_oldest_metadata, get_many_metadata_in_time_range
from local.lib.query_helpers import get_closest_metadata_before_target_ems, get_closest_metadata_after_target_ems
from local.lib.query_helpers import get_epoch_ms_list_in_time_range, get_count_in_time_range

from local.lib.response_helpers import no_data_response

from starlette.responses import JSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create camera info routes

# .....................................................................................................................

def caminfo_get_oldest_metadata(request):
    
    '''
    Returns oldest camera info entry. Should be an indication of when the camera first turned on,
    though keep in mind, older entries may have been deleted as part of storage cleanup
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_camera_info_collection(camera_select)
    no_oldest_metadata, metadata_dict = get_oldest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_oldest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return JSONResponse(metadata_dict)

# .....................................................................................................................

def caminfo_get_newest_metadata(request):
    
    '''
    Returns the newest camera info entry for a specific camera.
    Note that this may not be the best thing to do if working with a specific alarm time!
    A better option is the 'active' info entry, which will take into account camera reset events
    (and possible reconfiguration associated with those events)
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_camera_info_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return JSONResponse(metadata_dict)

# .....................................................................................................................

def caminfo_get_active_metadata(request):
    
    '''
    Returns the camera info that was 'active' given the target time (i.e. the 'newest' info at the given time)
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Find the active metadata entry
    collection_ref = get_camera_info_collection(camera_select)
    no_older_entry, entry_dict = get_closest_metadata_before_target_ems(collection_ref, target_ems, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_older_entry:
        error_message = "No metadata before time {}".format(target_ems)
        return no_data_response(error_message)
    
    return JSONResponse(entry_dict)

# .....................................................................................................................

def caminfo_get_many_metadata(request):
    
    '''
    Returns a list of camera info entries, given an input start and end time range.
    The returned values from this route act similar to the 'active' info route, in that only camera info
    which was active in the provided time range will be returned.
    This includes all camera info generated during the given time period + the closest camera info
    generated before the given start time (if applicable).
    See 'caminfo_get_active_info' route for more information.
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get reference to collection to use for queries
    collection_ref = get_camera_info_collection(camera_select)
    
    # Get 'active' entry along with range entries
    no_older_entry, active_entry = get_closest_metadata_before_target_ems(collection_ref, start_ems, EPOCH_MS_FIELD)
    range_query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Build output
    return_result = [] if no_older_entry else [active_entry]
    return_result += list(range_query_result)
    
    return JSONResponse(return_result)

# .....................................................................................................................

def caminfo_get_ems_list_by_time_range(request):
    
    '''
    Route which takes in a time range and returns a list of camera start times within the range
    If start times exists before or after the given range, then the given start/end times
    will be included in the list as well
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get reference to collection to use for queries
    collection_ref = get_camera_info_collection(camera_select)
    
    # Get start times within the given time range
    in_range_start_ems_list = get_epoch_ms_list_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Get start times just before/after the given range, if available
    no_prev_entry, prev_entry = get_closest_metadata_before_target_ems(collection_ref, start_ems, EPOCH_MS_FIELD)
    no_next_entry, next_entry = get_closest_metadata_after_target_ems(collection_ref, end_ems, EPOCH_MS_FIELD)
    
    # Build a list of all start ems values within the given time range
    output_start_ems_list = []
    
    # If a camera info entry exists prior to the given range, begin the output list with the given start time itself
    prev_entry_exists = (not no_prev_entry)
    if prev_entry_exists:
        output_start_ems_list += [start_ems]
    
    # Add all in-range start times
    output_start_ems_list += in_range_start_ems_list
    
    # If a camera info entry exists after the given range, end the output list with the given end time instead
    next_entry_exists = (not no_next_entry)
    if next_entry_exists:
        output_start_ems_list += [end_ems]
    
    return JSONResponse(output_start_ems_list)

# .....................................................................................................................

def caminfo_count_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get reference to collection to use for queries
    collection_ref = get_camera_info_collection(camera_select)
    
    # Get 'active' entry, since it should be included in count
    no_older_entry, _ = get_closest_metadata_before_target_ems(collection_ref, start_ems, EPOCH_MS_FIELD)
    add_one_to_count = (not no_older_entry)
    
    # Get count over range of time
    range_query_result = get_count_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Tally up total
    total_count = int(range_query_result) + int(add_one_to_count)
    
    # Build output
    return_result = {"count": total_count}
    
    return JSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def get_camera_info_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_camerainfo_routes():
    
    # Bundle all camera info routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    camerainfo_routes = \
    [
     Route(url("get-oldest-metadata"),
           caminfo_get_oldest_metadata),
     
     Route(url("get-newest-metadata"),
           caminfo_get_newest_metadata),
     
     Route(url("get-active-metadata", "by-time-target", "{target_time}"),
           caminfo_get_active_metadata),
     
     Route(url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
           caminfo_get_many_metadata),
     
     Route(url("get-ems-list", "by-time-range", "{start_time}", "{end_time}"),
           caminfo_get_ems_list_by_time_range),
     
     Route(url("count", "by-time-range", "{start_time}", "{end_time}"),
           caminfo_count_by_time_range)
    ]
    
    return camerainfo_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variable used to indicate timing field
EPOCH_MS_FIELD = "_id"

# Set name of collection, which determines url routing + storage on mongoDB
COLLECTION_NAME = "camerainfo"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


