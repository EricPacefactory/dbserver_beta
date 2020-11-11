#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 16:46:55 2020

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

from local.lib.mongo_helpers import MCLIENT, post_one_to_mongo

from local.lib.query_helpers import start_end_times_to_epoch_ms
from local.lib.query_helpers import get_all_ids, get_one_metadata, get_newest_metadata

from local.lib.response_helpers import bad_request_response, no_data_response
from local.lib.response_helpers import post_success_response, not_allowed_response

from local.routes.objects import OBJ_ID_FIELD, FIRST_EPOCH_MS_FIELD, FINAL_EPOCH_MS_FIELD
from local.routes.objects import check_collection_indexing, set_collection_indexing
from local.routes.objects import get_object_collection

from starlette.responses import JSONResponse
from starlette.routing import Route

from pymongo import ASCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Create object-specific query helpers

# .....................................................................................................................

def get_target_time_query_filter(target_ems):
    return {FIRST_EPOCH_MS_FIELD: {"$lte": target_ems}, FINAL_EPOCH_MS_FIELD: {"$gte": target_ems}}

# .....................................................................................................................

def get_time_range_query_filter(start_ems, end_ems):
    return {FIRST_EPOCH_MS_FIELD: {"$lt": end_ems}, FINAL_EPOCH_MS_FIELD: {"$gt": start_ems}}

# .....................................................................................................................

def find_by_time_range(collection_ref, start_ems, end_ems, *, return_ids_only):
    
    # Build query
    filter_dict = get_time_range_query_filter(start_ems, end_ems)
    projection_dict = {} if return_ids_only else None
    
    # Request data from the db
    query_result = collection_ref.find(filter_dict, projection_dict).sort(FAVE_ID_FIELD, ASCENDING)
    
    return query_result

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create create/delete routes

# .....................................................................................................................

def fave_add_entry(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    object_full_id = int(request.path_params["object_full_id"])
    
    # Get existing object data, if possible
    obj_collection_ref = get_object_collection(camera_select)
    query_result = get_one_metadata(obj_collection_ref, OBJ_ID_FIELD, object_full_id)
    
    # Deal with missing data
    if not query_result:
        error_message = "No object with id {}".format(object_full_id)
        return bad_request_response(error_message)
    
    # Add data to the favorites collection
    data_to_post = dict(query_result)
    post_success, mongo_response = post_one_to_mongo(MCLIENT, camera_select, COLLECTION_NAME, data_to_post)
    
    # Return an error response if there was a problem posting
    # Hard-coded: assuming the issue is with duplicate entries
    if not post_success:
        additional_response_dict = {"mongo_response": mongo_response}
        error_message = "Error posting {} metadata. Entry likely exists already!".format(COLLECTION_NAME)
        return not_allowed_response(error_message, additional_response_dict)
    
    # If we succeed, make sure to check/set time indexing, in case it hasn't already been set
    fave_collection_ref = get_favorite_collection(camera_select)
    fave_index_already_set = check_collection_indexing(fave_collection_ref, KEYS_TO_INDEX)
    if not fave_index_already_set:
        set_collection_indexing(fave_collection_ref, KEYS_TO_INDEX)
    
    return post_success_response()

# .....................................................................................................................

def faves_remove_entry(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    object_full_id = int(request.path_params["object_full_id"])
    
    # Build query
    query_dict = {FAVE_ID_FIELD: object_full_id}
    
    # Request document deletion
    collection_ref = get_favorite_collection(camera_select)
    pymongo_delete_result = collection_ref.delete_one(query_dict)
    
    # Give a different response if no data was deleted
    deleted_count = pymongo_delete_result.deleted_count
    nothing_deleted = (deleted_count == 0)
    if nothing_deleted:
        error_message = "No favorite with ID: {}".format(object_full_id)
        return no_data_response(error_message)
    
    # Convert to dictionary with count
    return_result = {"success": True, "deleted": deleted_count}
    
    return JSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create favorites routes

# .....................................................................................................................

def faves_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_favorite_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, FINAL_EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return JSONResponse(metadata_dict)

# .....................................................................................................................

def faves_get_all_ids(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Request data from the db
    collection_ref = get_favorite_collection(camera_select)
    query_result = get_all_ids(collection_ref)
    
    # Pull out the epoch values into a list, instead of returning a list of dictionaries
    return_result = [each_entry[FAVE_ID_FIELD] for each_entry in query_result]
    
    return JSONResponse(return_result)

# .....................................................................................................................

def faves_get_ids_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_favorite_collection(camera_select)
    query_result = find_by_time_range(collection_ref, start_ems, end_ems, return_ids_only = True)
    
    # Pull out the epoch values into a list, instead of returning a list of dictionaries
    return_result = [each_entry[FAVE_ID_FIELD] for each_entry in query_result]
    
    return JSONResponse(return_result)

# .....................................................................................................................

def faves_get_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    object_full_id = int(request.path_params["object_full_id"])
    
    # Request data from the db
    collection_ref = get_favorite_collection(camera_select)
    query_result = get_one_metadata(collection_ref, FAVE_ID_FIELD, object_full_id)
    
    # Deal with missing data
    if not query_result:
        error_message = "No object with id {}".format(object_full_id)
        return bad_request_response(error_message)
    
    return JSONResponse(query_result)

# .....................................................................................................................

def faves_count_by_time_range(request):
    
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
    collection_ref = get_favorite_collection(camera_select)
    query_result = collection_ref.count_documents(query_dict, projection_dict)
    
    # Convert to dictionary with count
    return_result = {"count": int(query_result)}
    
    return JSONResponse(return_result)

# .....................................................................................................................

def faves_set_indexing(request):
    
    '''
    Route mainly for debugging. Indexing should already be done automatically when faves are added!
    Can be used to forcefully set indexing on favorites, or otherwise check that the indexing is set
    '''
    
    # Get selected camera & corresponding collection
    camera_select = request.path_params["camera_select"]
    collection_ref = get_favorite_collection(camera_select)
    
    # Start timing
    t_start = perf_counter()
    
    # First check if the index is already set
    indexes_already_set = check_collection_indexing(collection_ref, KEYS_TO_INDEX)
    if indexes_already_set:
        return_result = {"already_set": True, "indexes": KEYS_TO_INDEX}
        return JSONResponse(return_result)
    
    # Set indexes on target fields if we haven't already
    mongo_response_list = set_collection_indexing(collection_ref, KEYS_TO_INDEX)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build response for debugging
    return_result = {"indexes_now_set": True,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response_list": mongo_response_list}
    
    return JSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def get_favorite_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_favorite_routes():
    
    # Bundle all object routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    favorite_routes = \
    [
     Route(url("add", "{object_full_id:int}"),
               fave_add_entry),
     
     Route(url("remove", "{object_full_id:int}"),
               faves_remove_entry),
     
     Route(url("get-all-ids-list"),
               faves_get_all_ids),
     
     Route(url("get-newest-metadata"),
               faves_get_newest_metadata),
     
     Route(url("get-ids-list", "by-time-range", "{start_time}", "{end_time}"),
               faves_get_ids_by_time_range),
     
     Route(url("get-one-metadata", "by-id", "{object_full_id:int}"),
               faves_get_one_metadata_by_id),
     
     Route(url("count", "by-time-range", "{start_time}", "{end_time}"),
               faves_count_by_time_range),
     
     Route(url("set-indexing"),
               faves_set_indexing)
    ]
    
    return favorite_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variables used to indicate timing field
FAVE_ID_FIELD = OBJ_ID_FIELD

# Hard-code the list of keys that need indexing
KEYS_TO_INDEX = [FIRST_EPOCH_MS_FIELD, FINAL_EPOCH_MS_FIELD]

# Set name of collection, which determines url routing + storage on mongoDB
COLLECTION_NAME = "favorites"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


