#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  8 16:15:25 2020

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
from shutil import rmtree

from local.lib.image_pathing import build_base_image_pathing
from local.lib.image_pathing import get_old_snapshot_image_folders_list, get_old_background_image_folders_list

from local.lib.mongo_helpers import MCLIENT
from local.lib.timekeeper_utils import epoch_ms_to_local_isoformat
from local.lib.query_helpers import url_time_to_epoch_ms, get_closest_metadata_before_target_ems
from local.lib.response_helpers import parse_ujson_response

from local.routes.camerainfo import get_camera_info_collection
from local.routes.configinfo import get_config_info_collection
from local.routes.backgrounds import get_background_collection
from local.routes.snapshots import get_snapshot_collection
from local.routes.objects import get_object_collection
from local.routes.stations import get_station_collection
from local.routes.logging import log_to_server, get_logging_collection

from local.routes.camerainfo import COLLECTION_NAME as CAMINFO_COLLECTION_NAME
from local.routes.configinfo import COLLECTION_NAME as CFGINFO_COLLECTION_NAME
from local.routes.backgrounds import COLLECTION_NAME as BACKGROUNDS_COLLECTION_NAME
from local.routes.snapshots import COLLECTION_NAME as SNAPSHOTS_COLLECTION_NAME
from local.routes.objects import COLLECTION_NAME as OBJECTS_COLLECTION_NAME
from local.routes.stations import COLLECTION_NAME as STATIONS_COLLECTION_NAME

from local.routes.objects import FINAL_EPOCH_MS_FIELD as OBJ_FINAL_EMS_FIELD
from local.routes.stations import FINAL_EPOCH_MS_FIELD as STN_FINAL_EMS_FIELD

from starlette.responses import UJSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

# .....................................................................................................................

def _delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field):
    
    # Start timing
    t_start = perf_counter()
    
    # Send deletion command to the db
    filter_dict = {epoch_ms_field: {"$lt": oldest_allowed_ems}}
    delete_response = collection_ref.delete_many(filter_dict)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    return delete_response, time_taken_ms

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create deletion routes

# .....................................................................................................................

def delete_caminfos_by_cutoff(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Start timing
    t_start = perf_counter()
    
    # Find the closest camera info before the target time, since we'll want to keep it!
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_camera_info_collection(camera_select)
    no_result, entry_dict = get_closest_metadata_before_target_ems(collection_ref, target_ems, epoch_ms_field)
    
    # Determine the deletion time to use, depending on whether we found an older camera info
    oldest_allowed_ems = target_ems
    if not no_result:
        keep_oldest_ems = entry_dict[epoch_ms_field]
        oldest_allowed_ems = keep_oldest_ems - 1
    
    # Delete old camera info metadata!
    delete_response, _ = _delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    deletion_datetime_str = epoch_ms_to_local_isoformat(oldest_allowed_ems)
    return_result = {"deletion_datetime_isoformat": deletion_datetime_str,
                     "deletion_epoch_ms": oldest_allowed_ems,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def delete_cfginfos_by_cutoff(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Start timing
    t_start = perf_counter()
    
    # Find the closest config info before the target time, since we'll want to keep it!
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_config_info_collection(camera_select)
    no_result, entry_dict = get_closest_metadata_before_target_ems(collection_ref, target_ems, epoch_ms_field)
    
    # Determine the deletion time to use, depending on whether we found an older config info
    oldest_allowed_ems = target_ems
    if not no_result:
        keep_oldest_ems = entry_dict[epoch_ms_field]
        oldest_allowed_ems = keep_oldest_ems - 1
    
    # Delete old config info metadata!
    delete_response, _ = _delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    deletion_datetime_str = epoch_ms_to_local_isoformat(oldest_allowed_ems)
    return_result = {"deletion_datetime_isoformat": deletion_datetime_str,
                     "deletion_epoch_ms": oldest_allowed_ems,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def delete_backgrounds_by_cutoff(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Start timing
    t_start = perf_counter()
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Determine oldest background to delete
    
    # Find the closest background before the target time, since we'll want to keep it!
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_background_collection(camera_select)
    no_result, entry_dict = get_closest_metadata_before_target_ems(collection_ref, target_ems, epoch_ms_field)
    
    # Determine the deletion time to use, depending on whether we found an older background
    oldest_allowed_ems = target_ems
    if not no_result:
        keep_oldest_ems = entry_dict[epoch_ms_field]
        oldest_allowed_ems = keep_oldest_ems - 1
    
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Delete background metadata then image data
    
    # Delete old metadata
    # Important to do this before image data so that we won't have any metadata pointing at missing images!
    delete_response, _ = _delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    # Get list of all folder paths that hold data older than the allowable time
    old_image_folder_paths = get_old_background_image_folders_list(IMAGE_FOLDER, camera_select, oldest_allowed_ems)
    
    # Delete all the folders containing old background images
    for each_folder_path in old_image_folder_paths:
        rmtree(each_folder_path, ignore_errors = True)
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Clean up
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    deletion_datetime_str = epoch_ms_to_local_isoformat(oldest_allowed_ems)
    return_result = {"deletion_datetime_isoformat": deletion_datetime_str,
                     "deletion_epoch_ms": oldest_allowed_ems,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def delete_objects_by_cutoff(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    oldest_allowed_ems = url_time_to_epoch_ms(target_time)
    
    # Start timing
    t_start = perf_counter()
    
    # Delete old object metadata, based on when objects ended
    epoch_ms_field = OBJ_FINAL_EMS_FIELD
    collection_ref = get_object_collection(camera_select)
    delete_response, _ = _delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    deletion_datetime_str = epoch_ms_to_local_isoformat(oldest_allowed_ems)
    return_result = {"deletion_datetime_isoformat": deletion_datetime_str,
                     "deletion_epoch_ms": oldest_allowed_ems,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def delete_stations_by_cutoff(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_ems = url_time_to_epoch_ms(target_time)
    
    # Start timing
    t_start = perf_counter()
    
    # Find the closest station data before the target time, since we'll want to keep it!
    epoch_ms_field = STN_FINAL_EMS_FIELD
    collection_ref = get_station_collection(camera_select)
    no_result, entry_dict = get_closest_metadata_before_target_ems(collection_ref, target_ems, epoch_ms_field)
    
    # Determine the deletion time to use, depending on whether we found an older station data entry
    oldest_allowed_ems = target_ems
    if not no_result:
        keep_oldest_ems = entry_dict[epoch_ms_field]
        oldest_allowed_ems = keep_oldest_ems - 1
    
    # Delete old station metadata!
    delete_response, _ = _delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    deletion_datetime_str = epoch_ms_to_local_isoformat(oldest_allowed_ems)
    return_result = {"deletion_datetime_isoformat": deletion_datetime_str,
                     "deletion_epoch_ms": oldest_allowed_ems,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def delete_snapshots_by_cutoff(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    oldest_allowed_ems = url_time_to_epoch_ms(target_time)
    
    # Start timing
    t_start = perf_counter()
    
    # Delete old metadata
    # Important to do this before deleting image data, so we don't have any metadata pointing to missing images!
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_snapshot_collection(camera_select)
    delete_response, _ = _delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    # Get list of all folder paths that hold data older than the allowable time
    old_image_folder_paths = get_old_snapshot_image_folders_list(IMAGE_FOLDER, camera_select, oldest_allowed_ems)
    
    # Delete all the folders containing old snapshot images
    for each_folder_path in old_image_folder_paths:
        rmtree(each_folder_path, ignore_errors = True)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    deletion_datetime_str = epoch_ms_to_local_isoformat(oldest_allowed_ems)
    return_result = {"deletion_datetime_isoformat": deletion_datetime_str,
                     "deletion_epoch_ms": oldest_allowed_ems,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def delete_allrealtime_by_cutoff(request):
    
    # Call other delete routes
    caminfo_delete_response = delete_caminfos_by_cutoff(request)
    cfginfo_delete_response = delete_cfginfos_by_cutoff(request)
    backgrounds_delete_response = delete_backgrounds_by_cutoff(request)
    objects_delete_response = delete_objects_by_cutoff(request)
    stations_delete_response = delete_stations_by_cutoff(request)
    snapshots_delete_response = delete_snapshots_by_cutoff(request)
    
    # Need to parse ujson responses to get deletion counts for logging
    caminfo_result = parse_ujson_response(caminfo_delete_response)
    cfginfo_result = parse_ujson_response(cfginfo_delete_response)
    backgrounds_result = parse_ujson_response(backgrounds_delete_response)
    objects_result = parse_ujson_response(objects_delete_response)
    stations_result = parse_ujson_response(stations_delete_response)
    snapshots_result = parse_ujson_response(snapshots_delete_response)
    
    # Build output to provide feedback about deletion
    return_result = {CAMINFO_COLLECTION_NAME: caminfo_result,
                     CFGINFO_COLLECTION_NAME: cfginfo_result,
                     BACKGROUNDS_COLLECTION_NAME: backgrounds_result,
                     OBJECTS_COLLECTION_NAME: objects_result,
                     STATIONS_COLLECTION_NAME: stations_result,
                     SNAPSHOTS_COLLECTION_NAME: snapshots_result}
    
    # Log results on the database itself
    camera_select = request.path_params["camera_select"]
    log_to_server(MCLIENT, camera_select, "delete", return_result)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def delete_serverlogs_by_cutoff(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    log_type = request.path_params["log_type"]
    target_time = request.path_params["target_time"]
    oldest_allowed_ems = url_time_to_epoch_ms(target_time)
    
    # Start timing
    t_start = perf_counter()
    
    # Delete old log entries
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_logging_collection(camera_select, log_type)
    delete_response, _ = _delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output to provide feedback about deletion
    deletion_datetime_str = epoch_ms_to_local_isoformat(oldest_allowed_ems)
    return_result = {"deletion_datetime_isoformat": deletion_datetime_str,
                     "deletion_epoch_ms": oldest_allowed_ems,
                     "time_taken_ms": time_taken_ms,
                     "mongo_response": delete_response}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_deleting_routes():
    
    # Bundle all deleting routes
    route_group = "delete"
    url = lambda *url_components: "/".join(["/{camera_select:str}", route_group, *url_components])
    deleting_routes = \
    [
     Route(url(CAMINFO_COLLECTION_NAME, "by-cutoff", "{target_time}"),
               delete_caminfos_by_cutoff),
     
     Route(url(CFGINFO_COLLECTION_NAME, "by-cutoff", "{target_time}"),
               delete_cfginfos_by_cutoff),
     
     Route(url(BACKGROUNDS_COLLECTION_NAME, "by-cutoff", "{target_time}"),
               delete_backgrounds_by_cutoff),
     
     Route(url(OBJECTS_COLLECTION_NAME, "by-cutoff", "{target_time}"),
               delete_objects_by_cutoff),
     
     Route(url(STATIONS_COLLECTION_NAME, "by-cutoff", "{target_time}"),
               delete_stations_by_cutoff),
     
     Route(url(SNAPSHOTS_COLLECTION_NAME, "by-cutoff", "{target_time}"),
               delete_snapshots_by_cutoff),
     
     Route(url("all-realtime", "by-cutoff", "{target_time}"),
               delete_allrealtime_by_cutoff),
     
     Route(url("serverlogs", "{log_type:str}", "by-cutoff", "{target_time}"),
               delete_serverlogs_by_cutoff)
    ]
    
    return deleting_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Establish (global!) variable used to access the persistent image folder
IMAGE_FOLDER = build_base_image_pathing()

# Hard-code (global!) variable used to indicate timing field
DEFAULT_EMS_FIELD = "_id"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap



