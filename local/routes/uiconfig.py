#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 15:21:08 2020

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

from json import JSONDecodeError
from time import perf_counter

from local.lib.timekeeper_utils import get_local_datetime, datetime_to_epoch_ms

from local.lib.mongo_helpers import MCLIENT, post_one_to_mongo

from local.lib.query_helpers import start_end_times_to_epoch_ms, get_count_in_time_range
from local.lib.query_helpers import get_newest_metadata, get_oldest_metadata, get_all_ids, get_one_metadata
from local.lib.query_helpers import get_many_metadata_in_time_range, get_epoch_ms_list_in_time_range

from local.lib.response_helpers import post_success_response, bad_request_response
from local.lib.response_helpers import not_allowed_response, no_data_response

from starlette.responses import UJSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

# .....................................................................................................................

def get_ems_range_query_filter(start_ems, end_ems):
    return {EPOCH_MS_FIELD: {"$gte": start_ems, "$lt": end_ems}}

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create uiconfig routes

# .....................................................................................................................

def uiconfig_info(request):
    
    ''' Helper route, used to document uiconfig usage & behaviour '''
    
    # Build a message meant to help document this set of routes
    msg_list = ["The 'uiconfig' storage is intended for holding configuration data for UI processing",
                "- Entries will be given timestamps automatically when created",
                "- When updating entries the target ems must already exist, otherwise it will NOT be created!",
                "- Entries are not auto-deleted! They will persist until manual deletion"]
    
    info_dict = {"info": msg_list}
    
    return UJSONResponse(info_dict)

# .....................................................................................................................

def uiconfig_CREATE_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    return

async def uiconfig_create_new_entry(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get current epoch ms for storage
    get_local_datetime, datetime_to_epoch_ms
    current_dt = get_local_datetime()
    current_epoch_ms = datetime_to_epoch_ms(current_dt)
    
    # Get post data & add entry id parameter
    post_data_json = await request.json()
    
    # Create 'default' entry
    default_config_dict = {EPOCH_MS_FIELD: current_epoch_ms}
    
    # Overwrite defaults with posted data
    config_data_json = {**default_config_dict, **post_data_json}
    
    # Send metadata to mongo
    post_success, mongo_response = post_one_to_mongo(MCLIENT, camera_select, COLLECTION_NAME, config_data_json)
    
    # Return an error response if there was a problem posting
    if not post_success:
        additional_response_dict = {"mongo_response": mongo_response}
        error_message = "Error posting config data"
        return not_allowed_response(error_message, additional_response_dict)
    
    return post_success_response()

# .....................................................................................................................
    
def uiconfig_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_uiconfig_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................
    
def uiconfig_get_oldest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_uiconfig_collection(camera_select)
    no_oldest_metadata, metadata_dict = get_oldest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_oldest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................

def uiconfig_get_one_metadata_by_ems(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Get data from db
    collection_ref = get_uiconfig_collection(camera_select)
    query_result = get_one_metadata(collection_ref, EPOCH_MS_FIELD, target_ems)

    # Deal with missing data
    if not query_result:
        error_message = "No metadata at {}".format(target_ems)
        return bad_request_response(error_message)
    
    return UJSONResponse(query_result)

# .....................................................................................................................

def uiconfig_get_many_metadata_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get data from db
    collection_ref = get_uiconfig_collection(camera_select)
    query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    return UJSONResponse(list(query_result))

# .....................................................................................................................

def uiconfig_get_all_ems_list(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_uiconfig_collection(camera_select)
    query_result = get_all_ids(collection_ref)
    
    # Pull out the entry IDs into a list, instead of returning a list of dictionaries
    return_result = [each_entry[EPOCH_MS_FIELD] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def uiconfig_get_ems_list_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get data from db
    collection_ref = get_uiconfig_collection(camera_select)
    epoch_ms_list = get_epoch_ms_list_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    return UJSONResponse(epoch_ms_list)

# .....................................................................................................................

def uiconfig_count_by_time_range(request):
    
     # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)

    # Request data from the db
    collection_ref = get_uiconfig_collection(camera_select)
    query_result = get_count_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Convert to dictionary with count
    return_result = {"count": int(query_result)}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def uiconfig_UPDATE_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    return

async def uiconfig_update_one_metadata_by_ems(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    try:
        update_data_dict = await request.json()
        
    except JSONDecodeError:
        # Handle json conversion errors
        error_message = "Couldn't decode POST body data!"
        return bad_request_response(error_message)
    
    # Handle missing incoming data
    if not update_data_dict:
        error_message = "Update cancelled! No POST body data!"
        return bad_request_response(error_message)
    
    # Bail on updates including entry ID keys
    if EPOCH_MS_FIELD in update_data_dict.keys():
        error_message = "Cannot modify entry time! Delete old entry & create a new one if needed"
        return not_allowed_response(error_message)
    
    # Build commands for query
    filter_dict = {EPOCH_MS_FIELD: target_ems}
    update_data_dict = {"$set": update_data_dict}
    
    # Send update command to the db
    collection_ref = get_uiconfig_collection(camera_select)
    update_response = collection_ref.update_one(filter_dict, update_data_dict, upsert = False)
    
    return UJSONResponse(update_response)

# .....................................................................................................................

def uiconfig_delete_one_metadata_by_ems(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Start timing for feedback
    t_start = perf_counter()
    
    # Send deletion command to the db
    filter_dict = {EPOCH_MS_FIELD: target_ems}
    collection_ref = get_uiconfig_collection(camera_select)
    delete_response = collection_ref.delete_one(filter_dict)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    return_result = {"time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def uiconfig_delete_many_metadata_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Start timing for feedback
    t_start = perf_counter()
    
    # Send deletion command to the db
    filter_dict = get_ems_range_query_filter(start_ems, end_ems)
    collection_ref = get_uiconfig_collection(camera_select)
    delete_response = collection_ref.delete_many(filter_dict)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    return_result = {"time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def get_uiconfig_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_uiconfig_routes():
    
    # Bundle all uiconfig routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    uiconfig_routes = \
    [
     Route("/{}/info".format(COLLECTION_NAME), uiconfig_info),
     
     Route(url("create-new-metadata"),
               uiconfig_create_new_entry,
               methods = ["POST"]),
     
     Route(url("update-one-metadata", "by-ems", "{epoch_ms:int}"),
               uiconfig_update_one_metadata_by_ems,
               methods = ["POST"]),
     
     Route(url("get-newest-metadata"),
               uiconfig_get_newest_metadata),
     
     Route(url("get-oldest-metadata"),
               uiconfig_get_oldest_metadata),
     
     Route(url("get-one-metadata", "by-ems", "{epoch_ms:int}"),
               uiconfig_get_one_metadata_by_ems),
     
     Route(url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
               uiconfig_get_many_metadata_by_time_range),
     
     Route(url("get-all-ems-list"),
               uiconfig_get_all_ems_list),
     
     Route(url("get-ems-list", "by-time-range", "{start_time}", "{end_time}"),
               uiconfig_get_ems_list_by_time_range),
     
     Route(url("count", "by-time-range", "{start_time}", "{end_time}"),
               uiconfig_count_by_time_range),
     
     Route(url("delete-one-metadata", "by-ems", "{epoch_ms:int}"),
               uiconfig_delete_one_metadata_by_ems),
     
     Route(url("delete-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
               uiconfig_delete_many_metadata_by_time_range)
    ]
    
    return uiconfig_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variables used to indicate config entry fields
EPOCH_MS_FIELD = "_id"

# Set name of collection, which determines url routing + storage on mongoDB
COLLECTION_NAME = "uiconfig"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


