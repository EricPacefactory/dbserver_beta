#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 16:04:15 2020

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

from time import perf_counter

from local.lib.mongo_helpers import connect_to_mongo, check_collection_indexing, set_collection_indexing

from local.lib.query_helpers import start_end_times_to_epoch_ms
from local.lib.query_helpers import get_all_ids, get_one_metadata, get_newest_metadata, get_oldest_metadata

from local.lib.response_helpers import bad_request_response, no_data_response

from starlette.responses import UJSONResponse
from starlette.routing import Route

from pymongo import ASCENDING, DESCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Create station-specific query helpers

# .....................................................................................................................

def get_target_time_query_filter(target_ems):
    return {FIRST_EPOCH_MS_FIELD: {"$lte": target_ems}, FINAL_EPOCH_MS_FIELD: {"$gte": target_ems}}

# .....................................................................................................................

def get_time_range_query_filter(start_ems, end_ems):
    return {FIRST_EPOCH_MS_FIELD: {"$lt": end_ems}, FINAL_EPOCH_MS_FIELD: {"$gt": start_ems}}


# .....................................................................................................................

def find_by_target_time(collection_ref, target_ems, *, return_ids_only, ascending_order = True):
    
    # Build query
    filter_dict = get_target_time_query_filter(target_ems)
    projection_dict = {} if return_ids_only else None
    
    # Request data from the db
    sort_order = ASCENDING if ascending_order else DESCENDING
    query_result = collection_ref.find(filter_dict, projection_dict).sort(STN_ID_FIELD, sort_order)
    
    return query_result

# .....................................................................................................................

def find_by_time_range(collection_ref, start_ems, end_ems, *, return_ids_only, ascending_order = True):
    
    # Build query
    filter_dict = get_time_range_query_filter(start_ems, end_ems)
    projection_dict = {} if return_ids_only else None
    
    # Request data from the db
    sort_order = ASCENDING if ascending_order else DESCENDING
    query_result = collection_ref.find(filter_dict, projection_dict).sort(STN_ID_FIELD, sort_order)
    
    return query_result

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Create station routes

# .....................................................................................................................

def stations_get_oldest_metadata(request):
    
    '''
    Returns oldest station data entry. Can be used along with the 'get newest' route to figure
    out the bounding times for station data
    (though interpretation is slightly more complex due to station data representing intervals of time...)
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_station_collection(camera_select)
    no_oldest_metadata, metadata_dict = get_oldest_metadata(collection_ref, FIRST_EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_oldest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................

def stations_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_station_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, FINAL_EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................

def stations_get_all_ids(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Request data from the db
    collection_ref = get_station_collection(camera_select)
    query_result = get_all_ids(collection_ref)
    
    # Pull out the ID into a list, instead of returning a list of dictionaries
    return_result = [each_entry[STN_ID_FIELD] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def stations_get_ids_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_station_collection(camera_select)
    query_result = find_by_time_range(collection_ref, start_ems, end_ems, return_ids_only = True)
    
    # Pull out the epoch values into a list, instead of returning a list of dictionaries
    return_result = [each_entry[STN_ID_FIELD] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def stations_get_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    station_full_id = int(request.path_params["station_data_id"])
    
    # Request data from the db
    collection_ref = get_station_collection(camera_select)
    query_result = get_one_metadata(collection_ref, STN_ID_FIELD, station_full_id)
    
    # Deal with missing data
    if not query_result:
        error_message = "No station with id {}".format(station_full_id)
        return bad_request_response(error_message)
    
    return UJSONResponse(query_result)

# .....................................................................................................................

def stations_get_many_metadata_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_station_collection(camera_select)
    query_result = find_by_time_range(collection_ref, start_ems, end_ems, return_ids_only = False)
    
    # Convert to dictionary, with station ids as keys
    return_result = {each_result[STN_ID_FIELD]: each_result for each_result in query_result}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def stations_count_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Build query
    query_dict = get_time_range_query_filter(start_ems, end_ems)
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_station_collection(camera_select)
    query_result = collection_ref.count_documents(query_dict, projection_dict)
    
    # Convert to dictionary with count
    return_result = {"count": int(query_result)}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def stations_set_indexing(request):
    
    ''' 
    Hacky function... Used to manually set station timing indexes for a specified camera.
    Ideally this is handled automatically during posting, but this route can be used to manually set/check indexing
    '''
    
    # Get selected camera & corresponding collection
    camera_select = request.path_params["camera_select"]
    collection_ref = get_station_collection(camera_select)
    
    # Start timing
    t_start = perf_counter()
    
    # First check if the index is already set
    indexes_already_set = check_collection_indexing(collection_ref, KEYS_TO_INDEX)
    if indexes_already_set:
        return_result = {"already_set": True, "indexes": KEYS_TO_INDEX}
        return UJSONResponse(return_result)
    
    # Set indexes on target fields if we haven't already
    mongo_response_list = set_collection_indexing(collection_ref, KEYS_TO_INDEX)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build response for debugging
    return_result = {"indexes_now_set": True,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response_list": mongo_response_list}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def get_station_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_station_routes():
    
    # Bundle all station routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    station_routes = \
    [
     Route(url("get-oldest-metadata"),
               stations_get_oldest_metadata),
     
     Route(url("get-newest-metadata"),
               stations_get_newest_metadata),
     
     Route(url("get-all-ids-list"),
               stations_get_all_ids),
     
     Route(url("get-ids-list", "by-time-range", "{start_time}", "{end_time}"),
               stations_get_ids_by_time_range),
     
     Route(url("get-one-metadata", "by-id", "{station_data_id:int}"),
               stations_get_one_metadata_by_id),
     
     Route(url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
               stations_get_many_metadata_by_time_range),
     
     Route(url("count", "by-time-range", "{start_time}", "{end_time}"),
               stations_count_by_time_range),
     
     Route(url("set-indexing"),
               stations_set_indexing)
    ]
    
    return station_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variables used to indicate timing fields
STN_ID_FIELD = "_id"
FIRST_EPOCH_MS_FIELD = "first_epoch_ms"
FINAL_EPOCH_MS_FIELD = "final_epoch_ms"

# Hard-code the list of keys that need indexing
KEYS_TO_INDEX = [FIRST_EPOCH_MS_FIELD, FINAL_EPOCH_MS_FIELD]

# Connection to mongoDB
MCLIENT = connect_to_mongo()
COLLECTION_NAME = "stations"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


