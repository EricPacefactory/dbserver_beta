#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 10:27:52 2020

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

from local.lib.timekeeper_utils import get_local_datetime, datetime_to_epoch_ms

from local.lib.mongo_helpers import MCLIENT, post_one_to_mongo

from local.lib.query_helpers import start_end_times_to_epoch_ms
from local.lib.query_helpers import get_all_ids, get_one_metadata, get_newest_metadata, get_oldest_metadata
from local.lib.query_helpers import get_epoch_ms_list_in_time_range

from local.lib.response_helpers import bad_request_response, no_data_response
from local.lib.response_helpers import post_success_response, not_allowed_response

from starlette.responses import JSONResponse
from starlette.routing import Route

from local.routes.objects import get_object_collection, get_start_end_bounding_ems


# ---------------------------------------------------------------------------------------------------------------------
#%% Create object routes

# .....................................................................................................................

def svolabels_CREATE_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    return

async def svolabels_create_new_entry(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get post data
    post_data_json = await request.json()
    
    # Make sure the posted data has the required keys
    required_keys_list = ["object_labels"]
    has_required_keys = all([each_key in post_data_json.keys() for each_key in required_keys_list])
    if not has_required_keys:
        additional_response_dict = {"required_keys": required_keys_list}
        error_message = "Error posting data, missing required keys"
        return not_allowed_response(error_message, additional_response_dict)
    
    # Get current timing to set ID
    current_local_dt = get_local_datetime()
    current_ems = datetime_to_epoch_ms(current_local_dt)
    post_data_json["_id"] = current_ems
    
    # Figure out the bounding start/end times for the given objects, if not already provided
    has_bounding_start_ems = (BOUNDING_START_EMS_FIELD in post_data_json)
    has_bounding_end_ems = (BOUNDING_END_EMS_FIELD in post_data_json)
    has_bounding_times = (has_bounding_start_ems and has_bounding_end_ems)
    if not has_bounding_times:
        try:
            obj_collection_ref = get_object_collection(camera_select)
            obj_ids_list = [int(each_id_str) for each_id_str in post_data_json["object_labels"].keys()]
            bounding_start_ems, bounding_end_ems = get_start_end_bounding_ems(obj_collection_ref, obj_ids_list)
        
        except Exception as err:
            # In case we run into an error, just record None for start/end times
            bounding_start_ems = None
            bounding_end_ems = None
            print("",
                  "ERROR ({})".format(COLLECTION_NAME),
                  "Failed to find bounding start/end times",
                  "", str(err), sep = "\n")
        
        # Add bounding times to the post data
        post_data_json[BOUNDING_START_EMS_FIELD] = bounding_start_ems
        post_data_json[BOUNDING_END_EMS_FIELD] = bounding_end_ems
    
    # Send metadata to mongo
    post_success, mongo_response = post_one_to_mongo(MCLIENT, camera_select, COLLECTION_NAME, post_data_json)
    
    # Return an error response if there was a problem posting
    if not post_success:
        additional_response_dict = {"mongo_response": mongo_response, "posted_data": post_data_json}
        error_message = "Error posting data (mongo)"
        return not_allowed_response(error_message, additional_response_dict)
    
    # Include the auto-generated id in the response
    additional_response_dict = {"_id": current_ems}
    return post_success_response(additional_response_dict = additional_response_dict)

# .....................................................................................................................

def svolabels_delete_one_metadata_by_ems(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    epoch_ms = request.path_params["epoch_ms"]
    
    # Start timing for feedback
    t_start = perf_counter()
    
    # Send deletion command to the db
    filter_dict = {EPOCH_MS_FIELD: epoch_ms}
    collection_ref = get_svolabel_collection(camera_select)
    pymongo_DeleteResult = collection_ref.delete_one(filter_dict)
    
    # Get the number of deleted documents from the response (if possible!)
    num_deleted = pymongo_DeleteResult.deleted_count
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    return_result = {"num_deleted": num_deleted,"time_taken_ms": time_taken_ms}
    
    return JSONResponse(return_result)

# .....................................................................................................................

def svolabels_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_svolabel_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return JSONResponse(metadata_dict)

# .....................................................................................................................

def svolabels_get_oldest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_svolabel_collection(camera_select)
    no_oldest_metadata, metadata_dict = get_oldest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_oldest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return JSONResponse(metadata_dict)

# .....................................................................................................................

def svolabels_get_all_ems_list(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_svolabel_collection(camera_select)
    all_epoch_ms_list = list(get_all_ids(collection_ref, EPOCH_MS_FIELD))
    
    return JSONResponse(all_epoch_ms_list)

# .....................................................................................................................

def svolabels_get_ems_list_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get data from db
    collection_ref = get_svolabel_collection(camera_select)
    epoch_ms_list = get_epoch_ms_list_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    return JSONResponse(epoch_ms_list)

# .....................................................................................................................

def svolabels_get_one_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Get data from db
    collection_ref = get_svolabel_collection(camera_select)
    query_result = get_one_metadata(collection_ref, EPOCH_MS_FIELD, target_ems)

    # Deal with missing data
    if not query_result:
        error_message = "No metadata at {}".format(target_ems)
        return bad_request_response(error_message)
    
    return JSONResponse(query_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def get_svolabel_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_svolabel_routes():
    
    # Bundle all object routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    svolabel_routes = \
    [
     Route(url("create-new-metadata"),
               svolabels_create_new_entry, methods = ["POST"]),
     
     Route(url("get-newest-metadata"),
               svolabels_get_newest_metadata),
     
     Route(url("get-oldest-metadata"),
               svolabels_get_oldest_metadata),
     
     Route(url("get-all-ems-list"),
               svolabels_get_all_ems_list),
     
     Route(url("get-ems-list", "by-time-range", "{start_time}", "{end_time}"),
               svolabels_get_ems_list_by_time_range),
     
     Route(url("get-one-metadata", "by-ems", "{epoch_ms:int}"),
               svolabels_get_one_metadata),
     
     Route(url("delete-one-metadata", "by-ems", "{epoch_ms:int}"),
               svolabels_delete_one_metadata_by_ems)
    ]
    
    return svolabel_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variables used to indicate timing fields
EPOCH_MS_FIELD = "_id"
BOUNDING_START_EMS_FIELD = "first_epoch_ms"
BOUNDING_END_EMS_FIELD = "final_epoch_ms"

# Set name of collection, which determines url routing + storage on mongoDB
COLLECTION_NAME = "svolabels"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


