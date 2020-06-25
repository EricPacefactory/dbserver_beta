#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  9 14:38:00 2020

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

import base64

from local.lib.mongo_helpers import connect_to_mongo

from local.lib.query_helpers import start_end_times_to_epoch_ms
from local.lib.query_helpers import get_one_metadata, get_oldest_metadata, get_newest_metadata
from local.lib.query_helpers import get_closest_metadata_before_target_ems, get_many_metadata_in_time_range
from local.lib.query_helpers import get_epoch_ms_list_in_time_range, get_count_in_time_range

from local.lib.response_helpers import no_data_response, bad_request_response

from local.lib.image_pathing import build_base_image_pathing, build_background_image_pathing

from starlette.responses import FileResponse, UJSONResponse, PlainTextResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create image routes

# .....................................................................................................................

def bg_get_newest_image(request):
        
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_background_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No image data for {}".format(camera_select)
        return no_data_response(error_message)
    
    # Build pathing to the file
    newest_ems = metadata_dict[EPOCH_MS_FIELD]
    image_load_path = build_background_image_pathing(IMAGE_FOLDER, camera_select, newest_ems)
    if not os.path.exists(image_load_path):
        error_message = "No image at {}".format(newest_ems)
        return no_data_response(error_message)
    
    return FileResponse(image_load_path)

# .....................................................................................................................

def bg_get_one_image(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Build pathing to the file
    image_load_path = build_background_image_pathing(IMAGE_FOLDER, camera_select, target_ems)
    if not os.path.exists(image_load_path):
        error_message = "No image at {}".format(target_ems)
        return bad_request_response(error_message)
    
    return FileResponse(image_load_path)

# .....................................................................................................................

def bg_get_active_image(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Find the active metadata entry, so we can grab the corresponding image path
    collection_ref = get_background_collection(camera_select)
    no_older_entry, entry_dict = get_closest_metadata_before_target_ems(collection_ref, target_ems, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_older_entry:
        error_message = "No metadata before time {}".format(target_ems)
        return bad_request_response(error_message)
    
    # Build pathing to the file & handle missing file data
    active_ems = entry_dict[EPOCH_MS_FIELD]
    image_load_path = build_background_image_pathing(IMAGE_FOLDER, camera_select, active_ems)
    if not os.path.exists(image_load_path):
        error_message = "No image at {}".format(active_ems)
        return bad_request_response(error_message)
    
    return FileResponse(image_load_path)

# .....................................................................................................................

def bg_get_one_b64_jpg(request):
        
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Build pathing to the file
    image_load_path = build_background_image_pathing(IMAGE_FOLDER, camera_select, target_ems)
    if not os.path.exists(image_load_path):
        error_message = "No image at {}".format(target_ems)
        return bad_request_response(error_message)
    
    # Load the image file and convert to base64
    with open(image_load_path, "rb") as image_data:
        b64_image = base64.b64encode(image_data.read())
    
    return PlainTextResponse(b64_image)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create metadata routes

# .....................................................................................................................
    
def bg_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_background_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................

def bg_get_bounding_times(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Request data from the db
    collection_ref = get_background_collection(camera_select)
    no_oldest_metadata, oldest_metadata_dict = get_oldest_metadata(collection_ref, EPOCH_MS_FIELD)
    no_newest_metadata, newest_metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Get results, if possible
    if no_oldest_metadata or no_newest_metadata:
        error_message = "No bounding times for {}".format(camera_select)
        return no_data_response(error_message)
    
    # Pull out only the timing info from the min/max entries to minimize the data being sent
    return_result = {"min_epoch_ms": oldest_metadata_dict["epoch_ms"],
                     "max_epoch_ms": newest_metadata_dict["epoch_ms"],
                     "min_datetime_isoformat": oldest_metadata_dict["datetime_isoformat"],
                     "max_datetime_isoformat": newest_metadata_dict["datetime_isoformat"]}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def bg_get_epochs_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get reference to collection to use for queries
    collection_ref = get_background_collection(camera_select)
    
    # Get 'active' entry along with range entries
    no_older_entry, active_entry = get_closest_metadata_before_target_ems(collection_ref, start_ems, EPOCH_MS_FIELD)
    range_epoch_ms_list = get_epoch_ms_list_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Build output
    epoch_ms_list = [] if no_older_entry else [active_entry[EPOCH_MS_FIELD]]
    epoch_ms_list += range_epoch_ms_list
    
    return UJSONResponse(epoch_ms_list)

# .....................................................................................................................

def bg_get_active_metadata(request):
    
    ''' 
    Returns the background metadata that was relevant at the given time (i.e. the background in use in realtime)
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Find the active metadata entry, so we can grab the corresponding image path
    collection_ref = get_background_collection(camera_select)
    no_older_entry, entry_dict = get_closest_metadata_before_target_ems(collection_ref, target_ems, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_older_entry:
        error_message = "No metadata before time {}".format(target_ems)
        return no_data_response(error_message)
    
    return UJSONResponse(entry_dict)

# .....................................................................................................................

def bg_get_one_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Get data from db
    collection_ref = get_background_collection(camera_select)
    query_result = get_one_metadata(collection_ref, EPOCH_MS_FIELD, target_ems)

    # Deal with missing data
    if not query_result:
        error_message = "No metadata at {}".format(target_ems)
        return bad_request_response(error_message)
    
    return UJSONResponse(query_result)

# .....................................................................................................................

def bg_get_many_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get reference to collection to use for queries
    collection_ref = get_background_collection(camera_select)
    
    # Get 'active' entry along with range entries
    no_older_entry, active_entry = get_closest_metadata_before_target_ems(collection_ref, start_ems, EPOCH_MS_FIELD)
    range_query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Build output
    return_result = [] if no_older_entry else [active_entry]
    return_result += list(range_query_result)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def bg_count_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get reference to collection to use for queries
    collection_ref = get_background_collection(camera_select)
    
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
#%% Define build functions

# .....................................................................................................................

def get_background_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_background_routes():
    
    # Bundle all background routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    background_routes = \
    [
     Route(url("get-bounding-times"),
               bg_get_bounding_times),
     
     Route(url("get-ems-list", "by-time-range", "{start_time}", "{end_time}"),
               bg_get_epochs_by_time_range),
     
     Route(url("get-newest-metadata"),
               bg_get_newest_metadata),
     
     Route(url("get-one-metadata", "by-ems", "{epoch_ms:int}"),
               bg_get_one_metadata),
     
     Route(url("get-active-metadata", "by-ems", "{epoch_ms:int}"),
               bg_get_active_metadata),
     
     Route(url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
               bg_get_many_metadata),
     
     Route(url("get-newest-image"),
               bg_get_newest_image),
     
     Route(url("get-one-image", "by-ems", "{epoch_ms:int}"),
               bg_get_one_image),
     
     Route(url("get-active-image", "by-ems", "{epoch_ms:int}"),
               bg_get_active_image),
     
     Route(url("get-one-b64-jpg", "by-ems", "{epoch_ms:int}"),
               bg_get_one_b64_jpg),
     
     Route(url("count", "by-time-range", "{start_time}", "{end_time}"),
               bg_count_by_time_range)
    ]
    
    return background_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Establish (global!) variable used to access the persistent image folder
IMAGE_FOLDER = build_base_image_pathing()

# Hard-code (global!) variable used to indicate timing field
EPOCH_MS_FIELD = "_id"

# Connection to mongoDB
MCLIENT = connect_to_mongo()
COLLECTION_NAME = "backgrounds"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


