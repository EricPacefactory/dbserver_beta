#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 15:42:58 2020

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

from local.lib.query_helpers import url_time_to_epoch_ms, start_end_times_to_epoch_ms
from local.lib.query_helpers import get_newest_metadata, get_oldest_metadata
from local.lib.query_helpers import get_closest_metadata_before_target_ems, get_many_metadata_in_time_range
from local.lib.query_helpers import get_count_in_time_range

from local.lib.response_helpers import no_data_response

from starlette.responses import UJSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create config info routes

# .....................................................................................................................

def cfginfo_get_oldest_metadata(request):
    
    '''
    Returns oldest config info entry. Should be an indication of when the camera first turned on,
    though keep in mind, older entries may have been deleted as part of storage cleanup
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_config_info_collection(camera_select)
    no_oldest_metadata, metadata_dict = get_oldest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_oldest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................

def cfginfo_get_newest_metadata(request):
    
    '''
    Returns the newest config info entry for a specific camera.
    Note that this may not be the best thing to do if working with a specific alarm time!
    A better option is the 'active' info entry, which will take into account camera reset events
    (and possible reconfiguration associated with those events)
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_config_info_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................

def cfginfo_get_active_metadata(request):
    
    '''
    Returns the config info that was 'active' given the target time (i.e. the 'newest' info at the given time)
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Find the active metadata entry
    collection_ref = get_config_info_collection(camera_select)
    no_older_entry, entry_dict = get_closest_metadata_before_target_ems(collection_ref, target_ems, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_older_entry:
        error_message = "No metadata before time {}".format(target_ems)
        return no_data_response(error_message)
    
    return UJSONResponse(entry_dict)

# .....................................................................................................................

def cfginfo_get_many_metadata(request):
    
    '''
    Returns a list of config info entries, given an input start and end time range.
    The returned values from this route act similar to the 'active' info route, in that only config info
    which was active in the provided time range will be returned.
    This includes all config info generated during the given time period + the closest config info
    generated before the given start time (if applicable).
    See 'cfginfo_get_active_info' route for more information.
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get reference to collection to use for queries
    collection_ref = get_config_info_collection(camera_select)
    
    # Get 'active' entry along with range entries
    no_older_entry, active_entry = get_closest_metadata_before_target_ems(collection_ref, start_ems, EPOCH_MS_FIELD)
    range_query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Build output
    return_result = [] if no_older_entry else [active_entry]
    return_result += list(range_query_result)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def cfginfo_count_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get reference to collection to use for queries
    collection_ref = get_config_info_collection(camera_select)
    
    # Get 'active' entry, since it should be included in count
    no_older_entry, _ = get_closest_metadata_before_target_ems(collection_ref, start_ems, EPOCH_MS_FIELD)
    add_one_to_count = (not no_older_entry)
    
    # Get count over range of time
    range_query_result = get_count_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Tally up total
    total_count = int(range_query_result) + int(add_one_to_count)
    
    # Build output
    return_result = {"count": total_count}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def get_config_info_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_configinfo_routes():
    
    # Bundle all config info routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    configinfo_routes = \
    [
     Route(url("get-oldest-metadata"),
           cfginfo_get_oldest_metadata),
     
     Route(url("get-newest-metadata"),
           cfginfo_get_newest_metadata),
     
     Route(url("get-active-metadata", "by-time-target", "{target_time}"),
           cfginfo_get_active_metadata),
     
     Route(url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
           cfginfo_get_many_metadata),
     
     Route(url("count", "by-time-range", "{start_time}", "{end_time}"),
           cfginfo_count_by_time_range)
    ]
    
    return configinfo_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variable used to indicate timing field
EPOCH_MS_FIELD = "_id"

# Connection to mongoDB
MCLIENT = connect_to_mongo()
COLLECTION_NAME = "configinfo"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


