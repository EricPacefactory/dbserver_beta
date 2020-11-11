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

from local.lib.mongo_helpers import MCLIENT, post_one_to_mongo, get_collection_names_list, get_camera_names_list
from local.lib.mongo_helpers import check_collection_indexing, set_collection_indexing

from local.lib.query_helpers import get_one_metadata, get_all_ids, get_newest_metadata

from local.lib.response_helpers import post_success_response, bad_request_response
from local.lib.response_helpers import not_allowed_response, no_data_response

from starlette.responses import JSONResponse
from starlette.routing import Route

from pymongo import ASCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

# .....................................................................................................................

def get_end_time_range_query_filter(low_end_ems, high_end_ems):
    return {FINAL_EPOCH_MS_FIELD: {"$gte": low_end_ems, "$lt": high_end_ems}}

# .....................................................................................................................

def find_by_end_time_range(collection_ref, low_end_ems, high_end_ems, *, return_ids_only):
    
    # Build query
    filter_dict = get_end_time_range_query_filter(low_end_ems, high_end_ems)
    projection_dict = {} if return_ids_only else None
    
    # Request data from the db
    query_result = collection_ref.find(filter_dict, projection_dict).sort(FINAL_EPOCH_MS_FIELD, ASCENDING)
    
    return query_result

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create uistore routes

# .....................................................................................................................

def uistore_info(request):
    
    ''' Helper route, used to document uistore usage & behaviour '''
    
    # Build a message meant to help document this set of routes
    msg_list = ["The 'uistore' storage is intended for holding data generated from the web UI",
                "- Data is grouped by 'store types', this can be used to separate different categories of data",
                "- Individual entries are stored using string IDs",
                "- Range based routes assume an end-time key ({}) is present".format(FINAL_EPOCH_MS_FIELD),
                "- Nothing else is assumed about the content of uistore data",
                "- Entries do not need to be consistently formatted",
                "- When updating entries, if the target ID doesn't already exist, it will be created!",
                "- Entries are not auto-deleted! They will persist until manual deletion"]
    
    info_dict = {"info": msg_list,
                 "indexes": KEYS_TO_INDEX}
    
    return JSONResponse(info_dict)

# .....................................................................................................................

def uistore_get_all_store_types(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Extract only the uistore related collection names
    all_collection_names_list = get_collection_names_list(MCLIENT, camera_select)
    
    # Iterate over all collections, grab only uistore entries and remove the uistore prefix
    remove_prefix_idx = len(COLLECTION_NAME_PREFIX)
    uistore_types_list = []
    for each_collection_name in all_collection_names_list:
        
        # Skip over collection names not related to uistore
        if not each_collection_name.startswith(COLLECTION_NAME_PREFIX):
            continue
        
        # Remove the name prefix for readability
        store_type_only = each_collection_name[remove_prefix_idx:]
        uistore_types_list.append(store_type_only)
    
    return JSONResponse(uistore_types_list)

# .....................................................................................................................

def uistore_all_cameras_get_all_store_types(request):
    
    ''' Same as 'uistore_get_all_store_types' but with a loop over all cameras '''
    
    # Run id list retrieval for all known cameras
    aggregate_results_dict = {}
    camera_names_list = get_camera_names_list(MCLIENT)
    for each_camera_name in camera_names_list:
        
        # Extract only the uistore related collection names
        all_collection_names_list = get_collection_names_list(MCLIENT, each_camera_name)
        
        # Iterate over all collections, grab only uistore entries and remove the uistore prefix
        remove_prefix_idx = len(COLLECTION_NAME_PREFIX)
        one_camera_uistore_types_list = []
        for each_collection_name in all_collection_names_list:
            
            # Skip over collection names not related to uistore
            if not each_collection_name.startswith(COLLECTION_NAME_PREFIX):
                continue
            
            # Remove the name prefix for readability
            store_type_only = each_collection_name[remove_prefix_idx:]
            one_camera_uistore_types_list.append(store_type_only)
        
        # Store final result for each camera
        aggregate_results_dict[each_camera_name] = one_camera_uistore_types_list
    
    return JSONResponse(aggregate_results_dict)

# .....................................................................................................................

def uistore_CREATE_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    return

async def uistore_create_new_entry(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    
    # Get post data
    post_data_json = await request.json()
    
    # Error out if the post data does not contain an id field
    if ENTRY_ID_FIELD not in post_data_json.keys():
        error_message = "Cannot post data without a '{}' key!".format(ENTRY_ID_FIELD)
        return not_allowed_response(error_message)
    
    # Send metadata to mongo
    collection_name = get_uistore_collection_name(store_type)
    post_success, mongo_response = post_one_to_mongo(MCLIENT, camera_select, collection_name, post_data_json)
    
    # Return an error response if there was a problem posting
    if not post_success:
        entry_id = post_data_json[ENTRY_ID_FIELD]
        additional_response_dict = {"mongo_response": mongo_response}
        error_message = "Error posting data for entry ID: {}".format(entry_id)
        return not_allowed_response(error_message, additional_response_dict)
    
    # If we get this far, make sure to apply indexing if needed
    collection_ref = get_uistore_collection(camera_select, store_type)
    indexes_already_set = check_collection_indexing(collection_ref, KEYS_TO_INDEX)
    if not indexes_already_set:
        set_collection_indexing(collection_ref, KEYS_TO_INDEX)
    
    return post_success_response()

# .....................................................................................................................

def uistore_UPDATE_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    return

async def uistore_update_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
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
    
    # Bail on updates including entry ID keys
    if ENTRY_ID_FIELD in update_data_dict.keys():
        error_message = "Cannot modify entry ID! Delete old entry & create a new one if needed"
        return not_allowed_response(error_message)
    
    # Build commands for query
    filter_dict = {ENTRY_ID_FIELD: entry_id}
    update_data_dict = {"$set": update_data_dict}
    
    # Send update command to the db
    collection_ref = get_uistore_collection(camera_select, store_type)
    update_response = collection_ref.update_one(filter_dict, update_data_dict, upsert = True)
    
    return JSONResponse(update_response)

# .....................................................................................................................

def uistore_get_example_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    
    # Get data from db
    collection_ref = get_uistore_collection(camera_select, store_type)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, ENTRY_ID_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return JSONResponse(metadata_dict)

# .....................................................................................................................

def uistore_get_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    entry_id = request.path_params["entry_id"]
    
    # Get data from db
    collection_ref = get_uistore_collection(camera_select, store_type)
    query_result = get_one_metadata(collection_ref, ENTRY_ID_FIELD, entry_id)

    # Deal with missing data
    if not query_result:
        error_message = "No metadata for id {}".format(entry_id)
        return bad_request_response(error_message)
    
    return JSONResponse(query_result)

# .....................................................................................................................

def uistore_get_many_metadata_by_end_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    low_end_ems = request.path_params["low_end_ems"]
    high_end_ems = request.path_params["high_end_ems"]
    
    # Request data from the db
    collection_ref = get_uistore_collection(camera_select, store_type)
    query_result = find_by_end_time_range(collection_ref, low_end_ems, high_end_ems, return_ids_only = False)
    
    # Convert to dictionary, with entry ids as keys
    return_result = {each_result[ENTRY_ID_FIELD]: each_result for each_result in query_result}
    
    return JSONResponse(return_result)

# .....................................................................................................................

def uistore_all_cameras_get_many_metadata_by_end_time_range(request):
    
    ''' Same as 'uistore_get_many_metadata_by_end_time_range' but with a loop over all cameras '''
    
    # Get information from route url
    store_type = request.path_params["store_type"]
    low_end_ems = request.path_params["low_end_ems"]
    high_end_ems = request.path_params["high_end_ems"]
    
    # Run metadata retrieval for all known cameras
    aggregate_results_dict = {}
    camera_names_list = get_camera_names_list(MCLIENT)
    for each_camera_name in camera_names_list:
            
        # Request data from the db
        collection_ref = get_uistore_collection(each_camera_name, store_type)
        query_result = find_by_end_time_range(collection_ref, low_end_ems, high_end_ems, return_ids_only = False)
        
        # Convert to dictionary, with entry ids as keys and store result for each camera
        one_camera_result = {each_result[ENTRY_ID_FIELD]: each_result for each_result in query_result}
        aggregate_results_dict[each_camera_name] = one_camera_result
    
    return JSONResponse(aggregate_results_dict)

# .....................................................................................................................

def uistore_get_all_ids_list(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    
    # Get data from db
    collection_ref = get_uistore_collection(camera_select, store_type)
    query_result = get_all_ids(collection_ref)
    
    # Pull out the entry IDs into a list, instead of returning a list of dictionaries
    return_result = [each_entry[ENTRY_ID_FIELD] for each_entry in query_result]
    
    return JSONResponse(return_result)

# .....................................................................................................................

def uistore_get_ids_list_by_end_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    low_end_ems = request.path_params["low_end_ems"]
    high_end_ems = request.path_params["high_end_ems"]
    
    # Request data from the db
    collection_ref = get_uistore_collection(camera_select, store_type)
    query_result = find_by_end_time_range(collection_ref, low_end_ems, high_end_ems, return_ids_only = True)
    
    # Pull out the entry IDs into a list, instead of returning a list of dictionaries
    return_result = [each_entry[ENTRY_ID_FIELD] for each_entry in query_result]
    
    return JSONResponse(return_result)

# .....................................................................................................................

def uistore_all_cameras_get_ids_list_by_end_time_range(request):
    
    ''' Same as 'uistore_get_ids_list_by_end_time_range' but with a loop over all cameras '''
    
    # Get information from route url
    store_type = request.path_params["store_type"]
    low_end_ems = request.path_params["low_end_ems"]
    high_end_ems = request.path_params["high_end_ems"]
    
    # Run id list retrieval for all known cameras
    aggregate_results_dict = {}
    camera_names_list = get_camera_names_list(MCLIENT)
    for each_camera_name in camera_names_list:
        
        # Request data from the db
        collection_ref = get_uistore_collection(each_camera_name, store_type)
        query_result = find_by_end_time_range(collection_ref, low_end_ems, high_end_ems, return_ids_only = True)
        
        # Pull out the entry IDs into a list, instead of returning a list of dictionaries & store for each camera
        one_camera_result = [each_entry[ENTRY_ID_FIELD] for each_entry in query_result]
        aggregate_results_dict[each_camera_name] = one_camera_result
        
        print(each_camera_name, one_camera_result)
    
    return JSONResponse(aggregate_results_dict)

# .....................................................................................................................

def uistore_delete_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    entry_id = request.path_params["entry_id"]
    
    # Start timing for feedback
    t_start = perf_counter()
    
    # Send deletion command to the db
    filter_dict = {ENTRY_ID_FIELD: entry_id}
    collection_ref = get_uistore_collection(camera_select, store_type)
    delete_response = collection_ref.delete_one(filter_dict)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    return_result = {"time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return JSONResponse(return_result)

# .....................................................................................................................

def uistore_delete_many_metadata_by_end_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    low_end_ems = request.path_params["low_end_ems"]
    high_end_ems = request.path_params["high_end_ems"]
    
    # Start timing for feedback
    t_start = perf_counter()
    
    # Send deletion command to the db
    filter_dict = get_end_time_range_query_filter(low_end_ems, high_end_ems)
    collection_ref = get_uistore_collection(camera_select, store_type)
    delete_response = collection_ref.delete_many(filter_dict)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    return_result = {"time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return JSONResponse(return_result)

# .....................................................................................................................

def uistore_set_indexing(request):
    
    '''
    Hacky function... Used to manually set uistore indexes for a specified camera.
    Ideally this is handled automatically during posting, but this route can be used to manually set/check indexing
    '''
    
    # Get selected camera & corresponding collection
    camera_select = request.path_params["camera_select"]
    store_type = request.path_params["store_type"]
    collection_ref = get_uistore_collection(camera_select, store_type)
    
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

def get_uistore_collection_name(store_type):
    return "".join([COLLECTION_NAME_PREFIX, store_type])

# .....................................................................................................................

def get_uistore_collection(camera_select, store_type):
    collection_name = get_uistore_collection_name(store_type)
    return MCLIENT[camera_select][collection_name]

# .....................................................................................................................

def build_uistore_routes():
    
    # Build function for creating single-camera routes
    one_camera_component = "/{camera_select:str}"
    url = lambda *url_components: "/".join([one_camera_component, COLLECTION_BASE_NAME, *url_components])
    
    # Build function for creating 'all cameras' routes
    all_cameras_component = "/all-cameras-{}".format(COLLECTION_BASE_NAME)
    all_cameras_url = lambda *url_components: "/".join([all_cameras_component, *url_components])
    
    # Bundle all ui storage routes
    uistore_routes = \
    [
     Route("/{}/info".format(COLLECTION_BASE_NAME), uistore_info),
     
     Route(url("get-all-store-types"),
               uistore_get_all_store_types),
     
     Route(all_cameras_url("get-all-store-types"),
                           uistore_all_cameras_get_all_store_types),
     
     Route(url("{store_type:str}", "create-new-metadata"),
               uistore_create_new_entry,
               methods = ["POST"]),
     
     Route(url("{store_type:str}","update-one-metadata", "by-id", "{entry_id:str}"),
               uistore_update_one_metadata_by_id,
               methods = ["POST"]),
     
     Route(url("{store_type:str}","get-example-metadata"),
               uistore_get_example_metadata),
     
     Route(url("{store_type:str}","get-one-metadata", "by-id", "{entry_id:str}"),
               uistore_get_one_metadata_by_id),
     
     Route(url("{store_type:str}","get-many-metadata", "by-end-time-range",
               "{low_end_ems:int}", "{high_end_ems:int}"),
               uistore_get_many_metadata_by_end_time_range),
     
     Route(all_cameras_url("{store_type:str}","get-many-metadata", "by-end-time-range",
                           "{low_end_ems:int}", "{high_end_ems:int}"),
                           uistore_all_cameras_get_many_metadata_by_end_time_range),
     
     Route(url("{store_type:str}", "get-all-ids-list"),
               uistore_get_all_ids_list),
     
     Route(url("{store_type:str}", "get-ids-list", "by-end-time-range",
               "{low_end_ems:int}", "{high_end_ems:int}"),
               uistore_get_ids_list_by_end_time_range),
     
     Route(all_cameras_url("{store_type:str}","get-ids-list", "by-end-time-range",
                           "{low_end_ems:int}", "{high_end_ems:int}"),
                           uistore_all_cameras_get_ids_list_by_end_time_range),
     
     Route(url("{store_type:str}", "delete-one-metadata", "by-id", "{entry_id:str}"),
               uistore_delete_one_metadata_by_id),
     
     Route(url("{store_type:str}", "delete-many-metadata", "by-end-time-range",
               "{low_end_ems:int}", "{high_end_ems:int}"),
               uistore_delete_many_metadata_by_end_time_range),
     
     Route(url("{store_type:str}", "set-indexing"),
               uistore_set_indexing)
    ]
    
    return uistore_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variable used to indicate entry (id) access field
ENTRY_ID_FIELD = "_id"

# Hard-code the list of keys that need indexing
FINAL_EPOCH_MS_FIELD = "end"
KEYS_TO_INDEX = [FINAL_EPOCH_MS_FIELD]

# Set name of collection, which determines url routing + storage on mongoDB
COLLECTION_BASE_NAME = "uistore"

# Set shared uistore prefix indicator
COLLECTION_NAME_PREFIX = "{}-".format(COLLECTION_BASE_NAME)


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


