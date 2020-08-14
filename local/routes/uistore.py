#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 09:55:17 2020

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

from local.lib.mongo_helpers import connect_to_mongo, post_one_to_mongo

from local.lib.query_helpers import get_one_metadata, get_all_ids

from local.lib.response_helpers import post_success_response, bad_request_response, not_allowed_response

from starlette.responses import UJSONResponse
from starlette.routing import Route

from pymongo import ASCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

# .....................................................................................................................

async def retrieve_post_data(request):
    ''' Helper function used to avoid setting route functions to async '''
    return await request.json()

# .....................................................................................................................

def get_id_range_query_filter(start_id_range, end_id_range):
    return {ENTRY_ID_FIELD: {"$gte": start_id_range, "$lt": end_id_range}}

# .....................................................................................................................

def find_by_id_range(collection_ref, start_id, end_id, *, return_ids_only):
    
    # Build query
    filter_dict = get_id_range_query_filter(start_id, end_id)
    projection_dict = {} if return_ids_only else None
    
    # Request data from the db
    query_result = collection_ref.find(filter_dict, projection_dict).sort(ENTRY_ID_FIELD, ASCENDING)
    
    return query_result

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create logging routes

# .....................................................................................................................

async def uistore_create_new_entry(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    entry_id = request.path_params["entry_id"]
    
    # Get post data & add _id parameter
    post_data_json = await request.json()
    post_data_json.update({ENTRY_ID_FIELD: entry_id})
    
    # Send metadata to mongo
    post_success, mongo_response = post_one_to_mongo(MCLIENT, camera_select, COLLECTION_NAME, post_data_json)
    
    # Return an error response if there was a problem posting
    if not post_success:
        additional_response_dict = {"mongo_response": mongo_response}
        error_message = "Error posting data for entry ID: {}".format(entry_id)
        return not_allowed_response(error_message, additional_response_dict)
    
    return post_success_response()

# .....................................................................................................................

def uistore_get_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    entry_id = request.path_params["entry_id"]
    
    # Get data from db
    collection_ref = get_uistore_collection(camera_select)
    query_result = get_one_metadata(collection_ref, ENTRY_ID_FIELD, entry_id)

    # Deal with missing data
    if not query_result:
        error_message = "No metadata for id {}".format(entry_id)
        return bad_request_response(error_message)
    
    return UJSONResponse(query_result)

# .....................................................................................................................

def uistore_get_many_metadata_by_id_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_id_range = request.path_params["start_id"]
    end_id_range = request.path_params["end_id"]
    
    # Request data from the db
    collection_ref = get_uistore_collection(camera_select)
    query_result = find_by_id_range(collection_ref, start_id_range, end_id_range, return_ids_only = False)
    
    # Convert to dictionary, with entry ids as keys
    return_result = {each_result[ENTRY_ID_FIELD]: each_result for each_result in query_result}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def uistore_get_all_ids_list(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_uistore_collection(camera_select)
    query_result = get_all_ids(collection_ref)
    
    # Pull out the entry IDs into a list, instead of returning a list of dictionaries
    return_result = [each_entry[ENTRY_ID_FIELD] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def uistore_get_ids_by_id_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_id_range = request.path_params["start_id"]
    end_id_range = request.path_params["end_id"]
    
    # Request data from the db
    collection_ref = get_uistore_collection(camera_select)
    query_result = find_by_id_range(collection_ref, start_id_range, end_id_range, return_ids_only = True)
    
    # Pull out the entry IDs into a list, instead of returning a list of dictionaries
    return_result = [each_entry[ENTRY_ID_FIELD] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def uistore_count_by_id_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_id_range = request.path_params["start_id"]
    end_id_range = request.path_params["end_id"]
    
    # Build query
    query_dict = get_id_range_query_filter(start_id_range, end_id_range)
    projection_dict = None
    
    # Request data from the db
    collection_ref = get_uistore_collection(camera_select)
    query_result = collection_ref.count_documents(query_dict, projection_dict)
    
    # Convert to dictionary with count
    return_result = {"count": int(query_result)}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

async def uistore_update_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    entry_id = request.path_params["entry_id"]
    
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
    
    # Bail on updates including _id keys
    if ENTRY_ID_FIELD in update_data_dict.keys():
        error_message = "Cannot modify entry ID! Delete old entry & create a new one if needed"
        return not_allowed_response(error_message)
    
    # Build commands for query
    filter_dict = {ENTRY_ID_FIELD: entry_id}
    update_data_dict = {"$set": update_data_dict}
    
    # Send update command to the db
    collection_ref = get_uistore_collection(camera_select)
    update_response = collection_ref.update_one(filter_dict, update_data_dict, upsert = True)
    
    return UJSONResponse(update_response)

# .....................................................................................................................

def uistore_delete_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    entry_id = request.path_params["entry_id"]
    
    # Start timing for feedback
    t_start = perf_counter()
    
    # Send deletion command to the db
    filter_dict = {ENTRY_ID_FIELD: entry_id}
    collection_ref = get_uistore_collection(camera_select)
    delete_response = collection_ref.delete_one(filter_dict)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    return_result = {"time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def uistore_delete_many_metadata_by_id_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_id_range = request.path_params["start_id"]
    end_id_range = request.path_params["end_id"]
    
    # Start timing for feedback
    t_start = perf_counter()
    
    # Send deletion command to the db
    filter_dict = get_id_range_query_filter(start_id_range, end_id_range)
    collection_ref = get_uistore_collection(camera_select)
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

def get_uistore_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_uistore_routes():
    
    # Bundle all ui storage routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    uistore_routes = \
    [
     Route(url("create-new-metadata", "{entry_id:int}"),
               uistore_create_new_entry,
               methods = ["POST"]),
     
     Route(url("get-one-metadata", "by-id", "{entry_id:int}"),
               uistore_get_one_metadata_by_id),
     
     Route(url("get-many-metadata", "by-id-range", "{start_id:int}", "{end_id:int}"),
               uistore_get_many_metadata_by_id_range),
     
     Route(url("get-all-ids-list"),
               uistore_get_all_ids_list),
     
     Route(url("get-ids-list", "by-id-range", "{start_id:int}", "{end_id:int}"),
               uistore_get_ids_by_id_range),
     
     Route(url("count", "by-id-range", "{start_id:int}", "{end_id:int}"),
               uistore_count_by_id_range),
     
     Route(url("update-one-metadata", "by-id", "{entry_id:int}"),
               uistore_update_one_metadata_by_id,
               methods = ["POST"]),
     
     Route(url("delete-one-metadata", "by-id", "{entry_id:int}"),
               uistore_delete_one_metadata_by_id),
     
     Route(url("delete-many-metadata", "by-id-range", "{start_id:int}", "{end_id:int}"),
               uistore_delete_many_metadata_by_id_range)
    ]
    
    return uistore_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variable used to indicate entry (id) access field
ENTRY_ID_FIELD = "_id"

# Connection to mongoDB
MCLIENT = connect_to_mongo()
COLLECTION_NAME = "uistore"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


