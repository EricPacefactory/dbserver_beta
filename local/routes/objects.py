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

from time import perf_counter

from local.lib.mongo_helpers import connect_to_mongo

from local.lib.query_helpers import url_time_to_epoch_ms, start_end_times_to_epoch_ms
from local.lib.query_helpers import get_all_ids, get_one_metadata, get_newest_metadata

from local.lib.response_helpers import bad_request_response, no_data_response

from starlette.responses import UJSONResponse
from starlette.routing import Route

from pymongo import ASCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Create indexing functions

# .....................................................................................................................

def check_collection_indexing(collection_ref):
    
    ''' Helper function which can be used to check if object indexing is already set '''
    
    current_index_info_dict = collection_ref.index_information()
    first_is_set = any(FIRST_EPOCH_MS_FIELD in each_key for each_key in current_index_info_dict.keys())
    final_is_set = any(FINAL_EPOCH_MS_FIELD in each_key for each_key in current_index_info_dict.keys())
    indexes_already_set = (first_is_set and final_is_set)
    
    return indexes_already_set

# .....................................................................................................................

def set_collection_indexing(collection_ref):
    
    ''' Helper function which can be used to set indexing on object collection '''
    
    first_resp = collection_ref.create_index(FIRST_EPOCH_MS_FIELD)
    final_resp = collection_ref.create_index(FINAL_EPOCH_MS_FIELD)
    
    mongo_response_list = [first_resp, final_resp]
    
    return mongo_response_list

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create object-specific query helpers

# .....................................................................................................................

def get_target_time_query_filter(target_ems):
    return {FIRST_EPOCH_MS_FIELD: {"$lte": target_ems}, FINAL_EPOCH_MS_FIELD: {"$gte": target_ems}}

# .....................................................................................................................

def get_time_range_query_filter(start_ems, end_ems):
    return {FIRST_EPOCH_MS_FIELD: {"$lt": end_ems}, FINAL_EPOCH_MS_FIELD: {"$gt": start_ems}}


# .....................................................................................................................

def find_by_target_time(collection_ref, target_ems, *, return_ids_only):
    
    # Build query
    filter_dict = get_target_time_query_filter(target_ems)
    projection_dict = {} if return_ids_only else None
    
    # Request data from the db
    query_result = collection_ref.find(filter_dict, projection_dict).sort(OBJ_ID_FIELD, ASCENDING)
    
    return query_result

# .....................................................................................................................

def find_by_time_range(collection_ref, start_ems, end_ems, *, return_ids_only):
    
    # Build query
    filter_dict = get_time_range_query_filter(start_ems, end_ems)
    projection_dict = {} if return_ids_only else None
    
    # Request data from the db
    query_result = collection_ref.find(filter_dict, projection_dict).sort(OBJ_ID_FIELD, ASCENDING)
    
    return query_result

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Create object routes

# .....................................................................................................................

def objects_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_object_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, FINAL_EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................

def objects_get_all_ids(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = get_all_ids(collection_ref)
    
    # Pull out the ID into a list, instead of returning a list of dictionaries
    return_result = [each_entry[OBJ_ID_FIELD] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_ids_at_target_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = find_by_target_time(collection_ref, target_ems, return_ids_only = True)
    
    # Convert to list of ids only
    return_result = [each_entry[OBJ_ID_FIELD] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_ids_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = find_by_time_range(collection_ref, start_ems, end_ems, return_ids_only = True)
    
    # Pull out the epoch values into a list, instead of returning a list of dictionaries
    return_result = [each_entry[OBJ_ID_FIELD] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    object_full_id = int(request.path_params["object_full_id"])
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = get_one_metadata(collection_ref, OBJ_ID_FIELD, object_full_id)
    
    # Deal with missing data
    if not query_result:
        error_message = "No object with id {}".format(object_full_id)
        return bad_request_response(error_message)
    
    return UJSONResponse(query_result)

# .....................................................................................................................

def objects_get_many_metadata_at_target_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = find_by_target_time(collection_ref, target_ems, return_ids_only = False)
    
    # Convert to dictionary, with object ids as keys
    return_result = {each_result[OBJ_ID_FIELD]: each_result for each_result in query_result}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_many_metadata_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = find_by_time_range(collection_ref, start_ems, end_ems, return_ids_only = False)
    
    # Convert to dictionary, with object ids as keys
    return_result = {each_result[OBJ_ID_FIELD]: each_result for each_result in query_result}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_count_at_target_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Build query
    query_dict = get_target_time_query_filter(target_ems)
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = collection_ref.count_documents(query_dict, projection_dict)
    
    # Convert to dictionary with count
    return_result = {"count": int(query_result)}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_count_by_time_range(request):
    
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
    collection_ref = get_object_collection(camera_select)
    query_result = collection_ref.count_documents(query_dict, projection_dict)
    
    # Convert to dictionary with count
    return_result = {"count": int(query_result)}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_set_indexing(request):
    
    ''' 
    Hacky function... Used to manually set object timing indexes for a specified camera. 
    Should eventually be done automatically (during data posting or by periodic background task?)
    '''
    
    # Get selected camera & corresponding collection
    camera_select = request.path_params["camera_select"]
    collection_ref = get_object_collection(camera_select)
    
    # Start timing
    t_start = perf_counter()
    
    # First check if the index is already set
    indexes_already_set = check_collection_indexing(collection_ref)
    if indexes_already_set:
        return_result = {"already_set": True}
        return UJSONResponse(return_result)
    
    # Set indexes on target fields if we haven't already
    mongo_response_list = set_collection_indexing(collection_ref)
    
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

def get_object_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_object_routes():
    
    # Bundle all object routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    object_routes = \
    [
     Route(url("get-newest-metadata"),
               objects_get_newest_metadata),
     
     Route(url("get-all-ids-list"),
               objects_get_all_ids),
     
     Route(url("get-ids-list", "by-time-target", "{target_time}"),
               objects_get_ids_at_target_time),
     
     Route(url("get-ids-list", "by-time-range", "{start_time}", "{end_time}"),
               objects_get_ids_by_time_range),
     
     Route(url("get-one-metadata", "by-id", "{object_full_id:int}"),
               objects_get_one_metadata_by_id),
     
     Route(url("get-many-metadata", "by-time-target", "{target_time}"),
               objects_get_many_metadata_at_target_time),
     
     Route(url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
               objects_get_many_metadata_by_time_range),
     
     Route(url("count", "by-time-target", "{target_time}"),
               objects_count_at_target_time),
     
     Route(url("count", "by-time-range", "{start_time}", "{end_time}"),
               objects_count_by_time_range),
     
     Route(url("set-indexing"),
               objects_set_indexing)
    ]
    
    return object_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variables used to indicate timing field
OBJ_ID_FIELD = "_id"
FIRST_EPOCH_MS_FIELD = "first_epoch_ms"
FINAL_EPOCH_MS_FIELD = "final_epoch_ms"

# Connection to mongoDB
MCLIENT = connect_to_mongo()
COLLECTION_NAME = "objects"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


