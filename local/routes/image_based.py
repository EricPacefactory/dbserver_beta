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

import base64

from shutil import rmtree

from local.lib.mongo_helpers import connect_to_mongo
from local.lib.timekeeper_utils import time_to_epoch_ms
from local.lib.timekeeper_utils import get_deletion_by_days_to_keep_timing
from local.lib.response_helpers import first_of_query, no_data_response, bad_request_response
from local.lib.image_pathing import build_base_image_pathing, build_image_pathing, get_old_image_folders_list

from starlette.responses import FileResponse, UJSONResponse, PlainTextResponse
from starlette.routing import Route

from pymongo import ASCENDING, DESCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Create image routes

# .....................................................................................................................

def get_newest_image(data_category):
    
    def inner_get_newest_image(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        
        # Build query
        target_field = "_id"
        query_dict = {}
        projection_dict = {}
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]    
        query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, DESCENDING).limit(1)
        
        # Pull out a single entry (there should only be one), if possible
        return_result = first_of_query(query_result)
        if return_result is None:
            error_message = "No image data for {}".format(camera_select)
            return no_data_response(error_message)
        
        # Build pathing to the file
        newest_id = return_result[target_field]
        image_load_path = build_image_pathing(IMAGE_FOLDER, camera_select, data_category, newest_id)
        if not os.path.exists(image_load_path):
            error_message = "No image at {}".format(newest_id)
            return no_data_response(error_message)
        
        return FileResponse(image_load_path)
    
    return inner_get_newest_image

# .....................................................................................................................

def get_one_image(data_category):
    
    def inner_get_one_image(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_ems = request.path_params["epoch_ms"]
        
        # Build pathing to the file
        image_load_path = build_image_pathing(IMAGE_FOLDER, camera_select, data_category, target_ems)
        if not os.path.exists(image_load_path):
            error_message = "No image at {}".format(target_ems)
            return bad_request_response(error_message)
        
        return FileResponse(image_load_path)
    
    return inner_get_one_image

# .....................................................................................................................

def get_one_b64_jpg(data_category):
    
    def inner_get_one_b64_jpg(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_ems = request.path_params["epoch_ms"]
        
        # Build pathing to the file
        image_load_path = build_image_pathing(IMAGE_FOLDER, camera_select, data_category, target_ems)
        if not os.path.exists(image_load_path):
            error_message = "No image at {}".format(target_ems)
            return bad_request_response(error_message)
        
        # Load the image file and convert to base64
        with open(image_load_path, "rb") as image_data:
            b64_image = base64.b64encode(image_data.read())
        
        return PlainTextResponse(b64_image)
    
    return inner_get_one_b64_jpg

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Create metadata routes

# .....................................................................................................................

def get_newest_metadata(data_category):
    
    def inner_get_newest_metadata(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        
        # Build query
        target_field = "_id"
        query_dict = {}
        projection_dict = None
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]    
        query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, DESCENDING).limit(1)
        
        # Pull out a single entry (there should only be one)
        return_result = first_of_query(query_result)
        if return_result is None:
            error_message = "No metadata for {}".format(camera_select)
            return no_data_response(error_message)
        
        return UJSONResponse(return_result)
    
    return inner_get_newest_metadata

# .....................................................................................................................

def get_bounding_times(data_category):
    
    def inner_get_bounding_times(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        
        # Request data from the db
        target_field = "_id"
        collection_ref = mclient[camera_select][data_category]
        min_query = collection_ref.find().sort(target_field, ASCENDING).limit(1)
        max_query = collection_ref.find().sort(target_field, DESCENDING).limit(1)
        
        # Get results, if possible
        min_result = first_of_query(min_query)
        max_result = first_of_query(max_query)
        if (min_result is None) or (max_result is None):
            error_message = "No bounding times for {}".format(camera_select)
            return no_data_response(error_message)
        
        # Pull out only the timing info from the min/max entries to minimize the data being sent
        return_result = {"min_epoch_ms": min_result["epoch_ms"],
                         "max_epoch_ms": max_result["epoch_ms"],
                         "min_datetime_isoformat": min_result["datetime_isoformat"],
                         "max_datetime_isoformat": max_result["datetime_isoformat"]}
        
        return UJSONResponse(return_result)
    
    return inner_get_bounding_times

# .....................................................................................................................

def get_closest_epoch_by_time(data_category):
    
    def inner_get_closest_epoch_by_time(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_time = request.path_params["target_time"]
        target_time = int(target_time) if target_time.isnumeric() else target_time
        target_ems = time_to_epoch_ms(target_time)
        
        # Build query components for the upper/lower bounding entries
        target_field = "_id"
        lower_search_query = {target_field: {"$lte": target_ems}}
        upper_search_query = {target_field: {"$gte": target_ems}}
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        lower_query_result = collection_ref.find(lower_search_query).sort(target_field, DESCENDING).limit(1)
        upper_query_result = collection_ref.find(upper_search_query).sort(target_field, ASCENDING).limit(1)
        
        # Handle missing query values
        upper_result = first_of_query(upper_query_result)
        lower_result = first_of_query(lower_query_result)
        if (upper_result is None) and (lower_result is None):
            error_message = "No data for {}".format(camera_select)
            return no_data_response(error_message)
        
        # Pull out upper/lower bound epoch_ms values (if possible)
        upper_ems = None if (upper_result is None) else upper_result[target_field]
        lower_ems = None if (lower_result is None) else lower_result[target_field]
        
        # Determine the closest epoch value while handling missing values
        closest_ems = None
        if lower_ems is None:
            closest_ems = upper_ems
        elif upper_ems is None:
            closest_ems = lower_ems
        else:
            lower_diff = (target_ems - lower_ems)
            upper_diff = (upper_ems - target_ems)
            closest_ems = lower_ems if (lower_diff < upper_diff) else upper_ems
        
        # Bundle outputs
        return_result = {"upper_bound_epoch_ms": upper_ems,
                         "lower_bound_epoch_ms": lower_ems,
                         "closest_epoch_ms": closest_ems}
        
        return UJSONResponse(return_result)
    
    return inner_get_closest_epoch_by_time

# .....................................................................................................................

def get_epochs_by_time_range(data_category):
    
    def inner_get_epochs_by_time_range(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        start_time = request.path_params["start_time"]
        end_time = request.path_params["end_time"]
        
        # Convert epoch inputs to integers, if needed
        start_time = int(start_time) if start_time.isnumeric() else start_time
        end_time = int(end_time) if end_time.isnumeric() else end_time
        
        # Convert times to epoch values for db lookup
        start_ems = time_to_epoch_ms(start_time)
        end_ems = time_to_epoch_ms(end_time)
        
        # Build query
        target_field = "_id"
        query_dict = {target_field: {"$gte": start_ems, "$lt": end_ems}}
        projection_dict = {}
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, ASCENDING)
        
        # Pull out the epoch values into a list, instead of returning a list of dictionaries
        return_result = [each_entry[target_field] for each_entry in query_result]
        
        return UJSONResponse(return_result)
    
    return inner_get_epochs_by_time_range

# .....................................................................................................................

def get_closest_metadata_by_time(data_category):
    
    def inner_get_closest_metadata_by_time(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_time = request.path_params["target_time"]
        target_time = int(target_time) if target_time.isnumeric() else target_time
        target_ems = time_to_epoch_ms(target_time)
        
        # Build aggregate query
        time_absdiff_cmd = {"$abs": {"$subtract": [target_ems, "$_id"]}}
        projection_cmd = {"$project": {"doc": "$$ROOT",  "time_absdiff": time_absdiff_cmd}}
        sort_cmd = {"$sort": {"time_absdiff": 1}}
        limit_cmd = {"$limit": 1}
        query_cmd_list = [projection_cmd, sort_cmd, limit_cmd]
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        query_result = collection_ref.aggregate(query_cmd_list)
        
        # Deal with missing data
        first_entry = first_of_query(query_result)
        if first_entry is None:
            error_message = "No closest metadata for {}".format(target_ems)
            return no_data_response(error_message)
        
        # If we get here we probably have data so return the document data except for the id
        return_result = first_entry["doc"]
        
        return UJSONResponse(return_result)

    return inner_get_closest_metadata_by_time

# .....................................................................................................................

def get_one_metadata(data_category):

    def inner_get_one_metadata(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_ems = request.path_params["epoch_ms"]
        
        # Build query
        query_dict = {"_id": target_ems}
        projection_dict = None
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        query_result = collection_ref.find_one(query_dict, projection_dict)
        
        # Deal with missing data
        if not query_result:
            error_message = "No metadata at {}".format(target_ems)
            return bad_request_response(error_message)
        
        return UJSONResponse(query_result)
    
    return inner_get_one_metadata

# .....................................................................................................................

def get_many_metadata(data_category):
    
    def inner_get_many_metadata(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        start_time = request.path_params["start_time"]
        end_time = request.path_params["end_time"]
        
        # Convert epoch inputs to integers, if needed
        start_time = int(start_time) if start_time.isnumeric() else start_time
        end_time = int(end_time) if end_time.isnumeric() else end_time
        
        # Convert times to epoch values for db lookup
        start_ems = time_to_epoch_ms(start_time)
        end_ems = time_to_epoch_ms(end_time)
        
        # Build query
        target_field = "_id"
        query_dict = {target_field: {"$gte": start_ems, "$lt": end_ems}}
        projection_dict = None
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, ASCENDING)
        
        return UJSONResponse(list(query_result))
    
    return inner_get_many_metadata

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Shared routes (image + metadata)

# .....................................................................................................................

def delete_by_days_to_keep(data_category):
    
    def inner_delete_by_days_to_keep(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        days_to_keep = request.path_params["days_to_keep"]
        
        # Get timing needed to handle deletions
        oldest_allowed_dt, oldest_allowed_ems, deletion_datetime_str = \
        get_deletion_by_days_to_keep_timing(days_to_keep)
        
        # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
        # Delete metadata entries
        
        # Build filter
        filter_dict = {"_id": {"$lt": oldest_allowed_ems}}
        
        # Send deletion command to the db
        collection_ref = mclient[camera_select][data_category]
        delete_response = collection_ref.delete_many(filter_dict)
        
        # Build output to provide feedback about deletion
        return_result = {"deletion_datetime": deletion_datetime_str,
                         "deletion_epoch_ms": oldest_allowed_ems,
                         "mongo_response": delete_response}
        
        # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
        # Delete image files (after metadata, so metadata reference doesn't exist anymore)
        
        # Get list of all folder paths that hold data older than the allowable time
        old_image_folder_paths = \
        get_old_image_folders_list(IMAGE_FOLDER, camera_select, data_category, oldest_allowed_ems)
        
        # Delete all the image data!
        for each_folder_path in old_image_folder_paths:
            rmtree(each_folder_path, ignore_errors = True)
        
        return UJSONResponse(return_result)
    
    return inner_delete_by_days_to_keep

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Define build functions

# .....................................................................................................................

def build_snapshot_routes():
    
    # Bundle all snapshot routes
    snap_category = "snapshots"
    snap_url = lambda snap_route: "".join(["/{camera_select:str}/snapshots", snap_route])
    snapshot_routes = \
    [
     Route(snap_url("/get-newest-metadata"), get_newest_metadata(snap_category)),
     Route(snap_url("/get-newest-image"), get_newest_image(snap_category)),
     Route(snap_url("/get-bounding-times"), get_bounding_times(snap_category)),
     Route(snap_url("/get-closest-ems/by-time-target/{target_time}"), get_closest_epoch_by_time(snap_category)),
     Route(snap_url("/get-ems-list/by-time-range/{start_time}/{end_time}"), get_epochs_by_time_range(snap_category)),
     Route(snap_url("/get-closest-metadata/by-time-target/{target_time}"), get_closest_metadata_by_time(snap_category)),
     Route(snap_url("/get-one-metadata/by-ems/{epoch_ms:int}"), get_one_metadata(snap_category)),
     Route(snap_url("/get-many-metadata/by-time-range/{start_time}/{end_time}"), get_many_metadata(snap_category)),
     Route(snap_url("/get-one-image/by-ems/{epoch_ms:int}"), get_one_image(snap_category)),
     Route(snap_url("/get-one-b64-jpg/by-ems/{epoch_ms:int}"), get_one_b64_jpg(snap_category)),
     Route(snap_url("/delete/by-cutoff/{days_to_keep:int}"), delete_by_days_to_keep(snap_category))
    ]
    
    return snapshot_routes

# .....................................................................................................................

def build_background_routes():
    
    # Bundle all background routes
    bg_category = "backgrounds"
    bg_url = lambda bg_route: "".join(["/{camera_select:str}/backgrounds", bg_route])
    background_routes = \
    [
     Route(bg_url("/get-newest-metadata"), get_newest_metadata(bg_category)),
     Route(bg_url("/get-newest-image"), get_newest_image(bg_category)),
     Route(bg_url("/get-bounding-times"), get_bounding_times(bg_category)),
     Route(bg_url("/get-closest-ems/by-time-target/{target_time}"), get_closest_epoch_by_time(bg_category)),
     Route(bg_url("/get-ems-list/by-time-range/{start_time}/{end_time}"), get_epochs_by_time_range(bg_category)),
     Route(bg_url("/get-closest-metadata/by-time-target/{target_time}"), get_closest_metadata_by_time(bg_category)),
     Route(bg_url("/get-one-metadata/by-ems/{epoch_ms:int}"), get_one_metadata(bg_category)),
     Route(bg_url("/get-many-metadata/by-time-range/{start_time}/{end_time}"), get_many_metadata(bg_category)),
     Route(bg_url("/get-one-image/by-ems/{epoch_ms:int}"), get_one_image(bg_category)),
     Route(bg_url("/get-one-b64-jpg/by-ems/{epoch_ms:int}"), get_one_b64_jpg(bg_category)),
     Route(bg_url("/delete/by-cutoff/{days_to_keep:int}"), delete_by_days_to_keep(bg_category))
    ]
    
    return background_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Establish (global!) variable used to access the persistent image folder
IMAGE_FOLDER = build_base_image_pathing()

# Connection to mongoDB
mclient = connect_to_mongo()


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


