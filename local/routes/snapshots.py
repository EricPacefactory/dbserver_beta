#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  9 14:36:17 2020

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

from local.lib.query_helpers import url_time_to_epoch_ms, start_end_times_to_epoch_ms
from local.lib.query_helpers import get_one_metadata, get_oldest_metadata, get_newest_metadata
from local.lib.query_helpers import get_many_metadata_in_time_range
from local.lib.query_helpers import get_epoch_ms_list_in_time_range, get_count_in_time_range

from local.lib.response_helpers import no_data_response, bad_request_response
from local.lib.query_helpers import first_of_query
from local.lib.image_pathing import build_base_image_pathing, build_snapshot_image_pathing

from starlette.responses import FileResponse, UJSONResponse, PlainTextResponse
from starlette.routing import Route

from pymongo import ASCENDING, DESCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Create image routes

# .....................................................................................................................

def snap_get_newest_image(request):
        
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_snapshot_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No image data for {}".format(camera_select)
        return no_data_response(error_message)
    
    # Build pathing to the file
    newest_ems = metadata_dict[EPOCH_MS_FIELD]
    image_load_path = build_snapshot_image_pathing(IMAGE_FOLDER, camera_select, newest_ems)
    if not os.path.exists(image_load_path):
        error_message = "No image at {}".format(newest_ems)
        return no_data_response(error_message)
    
    return FileResponse(image_load_path)

# .....................................................................................................................

def snap_get_one_image(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Build pathing to the file
    image_load_path = build_snapshot_image_pathing(IMAGE_FOLDER, camera_select, target_ems)
    if not os.path.exists(image_load_path):
        error_message = "No image at {}".format(target_ems)
        return bad_request_response(error_message)
    
    return FileResponse(image_load_path)

# .....................................................................................................................

def snap_get_one_b64_jpg(request):
        
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Build pathing to the file
    image_load_path = build_snapshot_image_pathing(IMAGE_FOLDER, camera_select, target_ems)
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
    
def snap_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Get data from db
    collection_ref = get_snapshot_collection(camera_select)
    no_newest_metadata, metadata_dict = get_newest_metadata(collection_ref, EPOCH_MS_FIELD)
    
    # Handle missing metadata
    if no_newest_metadata:
        error_message = "No metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(metadata_dict)

# .....................................................................................................................

def snap_get_bounding_times(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Request data from the db
    collection_ref = get_snapshot_collection(camera_select)
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

def snap_get_closest_epoch_by_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Build query components for the upper/lower bounding entries
    target_field = EPOCH_MS_FIELD
    lower_search_query = {target_field: {"$lte": target_ems}}
    upper_search_query = {target_field: {"$gte": target_ems}}
    
    # Request data from the db
    collection_ref = get_snapshot_collection(camera_select)
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

# .....................................................................................................................

def snap_get_epochs_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get data from db
    collection_ref = get_snapshot_collection(camera_select)
    epoch_ms_list = get_epoch_ms_list_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    return UJSONResponse(epoch_ms_list)

# .....................................................................................................................

def snap_get_closest_metadata_by_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Build aggregate query
    time_absdiff_cmd = {"$abs": {"$subtract": [target_ems, "$_id"]}}
    projection_cmd = {"$project": {"doc": "$$ROOT",  "time_absdiff": time_absdiff_cmd}}
    sort_cmd = {"$sort": {"time_absdiff": 1}}
    limit_cmd = {"$limit": 1}
    query_cmd_list = [projection_cmd, sort_cmd, limit_cmd]
    
    # Request data from the db
    collection_ref = get_snapshot_collection(camera_select)
    query_result = collection_ref.aggregate(query_cmd_list)
    
    # Deal with missing data
    first_entry = first_of_query(query_result)
    if first_entry is None:
        error_message = "No closest metadata for {}".format(target_ems)
        return no_data_response(error_message)
    
    # If we get here we probably have data so return the document data except for the id
    return_result = first_entry["doc"]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def snap_get_one_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_ems = request.path_params["epoch_ms"]
    
    # Get data from db
    collection_ref = get_snapshot_collection(camera_select)
    query_result = get_one_metadata(collection_ref, EPOCH_MS_FIELD, target_ems)

    # Deal with missing data
    if not query_result:
        error_message = "No metadata at {}".format(target_ems)
        return bad_request_response(error_message)
    
    return UJSONResponse(query_result)

# .....................................................................................................................

def snap_get_many_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get data from db
    collection_ref = get_snapshot_collection(camera_select)
    query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    return UJSONResponse(list(query_result))

# .....................................................................................................................

def snap_get_many_metadata_n_samples(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    n_samples = request.path_params["n"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Handle zero/negative sample cases
    if n_samples < 1:
        return UJSONResponse([])
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get data from db
    collection_ref = get_snapshot_collection(camera_select)
    query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Convert result to a list so we can subsample from it
    result_list = list(query_result)
    num_samples_total = len(result_list)
    
    # Handle cases where there are fewer (or equal) results than the number of samples requested
    if num_samples_total <= n_samples:
        return UJSONResponse(result_list)
    
    # Handle special case where only 1 sample is requested. We'll grab the middle one
    if n_samples == 1:
        middle_idx = int(num_samples_total / 2)
        return_result = [result_list[middle_idx]]
        return UJSONResponse(return_result)
    
    # Pick out n-samples from the result
    step_factor = (num_samples_total - 1) / (n_samples - 1)
    return_result = [result_list[int(round(k * step_factor))] for k in range(n_samples)]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def snap_get_many_metadata_skip_n_subsample(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    skip_n = request.path_params["n"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Make sure the skip value is positive, and get the subsample factor
    skip_n = max(0, skip_n)
    nth_subsample = 1 + skip_n
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Get data from db
    collection_ref = get_snapshot_collection(camera_select)
    query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Convert result to a list so we can subsample from it
    result_list = list(query_result)
    num_samples_total = len(result_list)
    
    # Handle cases with only 0, 1 or 2 entries (where we can't meaningfully skip samples)
    if num_samples_total < 3:
        return UJSONResponse(result_list)
    
    # If we have data, figure out the best first-index offset, so we evenly place the subsamples
    # For example, given a list: [1,2,3,4,5,6,7,8,9], skip 3
    #   -> Simplest solution: [1, 4, 7]
    #   ->   Better solution: [2, 5, 8]
    subsample_extent = int(1 + nth_subsample * int((num_samples_total - 1) / nth_subsample))
    first_index_offset = int((num_samples_total - subsample_extent) / 2)
    subsampled_list = result_list[first_index_offset::nth_subsample]
    
    return UJSONResponse(subsampled_list)

# .....................................................................................................................

def snap_count_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)

    # Request data from the db
    collection_ref = get_snapshot_collection(camera_select)
    query_result = get_count_in_time_range(collection_ref, start_ems, end_ems, EPOCH_MS_FIELD)
    
    # Convert to dictionary with count
    return_result = {"count": int(query_result)}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define build functions

# .....................................................................................................................

def get_snapshot_collection(camera_select):
    return MCLIENT[camera_select][COLLECTION_NAME]

# .....................................................................................................................

def build_snapshot_routes():
    
    # Bundle all snapshot routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", COLLECTION_NAME, *url_components])
    snapshot_routes = \
    [
     Route(url("get-bounding-times"),
               snap_get_bounding_times),
     
     Route(url("get-ems-list", "by-time-range", "{start_time}", "{end_time}"),
               snap_get_epochs_by_time_range),
     
     Route(url("get-closest-ems", "by-time-target", "{target_time}"),
               snap_get_closest_epoch_by_time),
     
     Route(url("get-newest-metadata"),
               snap_get_newest_metadata),
     
     Route(url("get-one-metadata", "by-ems", "{epoch_ms:int}"),
               snap_get_one_metadata),
     
     Route(url("get-closest-metadata", "by-time-target", "{target_time}"),
               snap_get_closest_metadata_by_time),
     
     Route(url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
               snap_get_many_metadata),
     
     Route(url("get-many-metadata", "by-time-range", "skip-n", "{start_time}", "{end_time}", "{n:int}"),
               snap_get_many_metadata_skip_n_subsample),
     
     Route(url("get-many-metadata", "by-time-range", "n-samples", "{start_time}", "{end_time}", "{n:int}"),
               snap_get_many_metadata_n_samples),
     
     Route(url("get-newest-image"),
               snap_get_newest_image),
     
     Route(url("get-one-image", "by-ems", "{epoch_ms:int}"),
               snap_get_one_image),
     
     Route(url("get-one-b64-jpg", "by-ems", "{epoch_ms:int}"),
               snap_get_one_b64_jpg),
     
     Route(url("count", "by-time-range", "{start_time}", "{end_time}"),
               snap_count_by_time_range)
    ]
    
    return snapshot_routes

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
COLLECTION_NAME = "snapshots"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


