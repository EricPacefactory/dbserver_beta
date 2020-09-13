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

from local.lib.mongo_helpers import connect_to_mongo

from local.lib.query_helpers import start_end_times_to_epoch_ms, get_many_metadata_in_time_range
from local.lib.query_helpers import get_epoch_ms_list_in_time_range

from local.lib.response_helpers import encode_jsongz_data

from local.lib.image_pathing import build_base_image_pathing, build_snapshot_image_pathing

from local.routes.snapshots import COLLECTION_NAME as SNAP_COLLECTION_NAME
from local.routes.snapshots import EPOCH_MS_FIELD as SNAP_EPOCH_MS_FIELD
from local.routes.snapshots import get_snapshot_collection

from local.routes.objects import COLLECTION_NAME as OBJ_COLLECTION_NAME
from local.routes.objects import find_by_time_range as find_objs_by_time_range
from local.routes.objects import get_object_collection

from local.routes.stations import COLLECTION_NAME as STATIONS_COLLECTION_NAME
from local.routes.stations import find_by_time_range as find_stns_by_time_range
from local.routes.stations import get_station_collection

from starlette.responses import UJSONResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocketDisconnect


# ---------------------------------------------------------------------------------------------------------------------
#%% Create object routes

# .....................................................................................................................

def websocket_route_info(request):
    
    ''' Helper route, used to document websocket route behaviour '''
    
    # Build a message meant to help document this set of routes
    msg_list = ["Not finished!"]
    
    info_dict = {"info": msg_list}
    
    return UJSONResponse(info_dict)

# .....................................................................................................................

async def snapshots_ws_get_many_metadata_by_time_range(websocket):
    
    # Get information from route url
    camera_select = websocket.path_params["camera_select"]
    start_time = websocket.path_params["start_time"]
    end_time = websocket.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_snapshot_collection(camera_select)
    query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, SNAP_EPOCH_MS_FIELD,
                                                   ascending_order = False)
    
    # Handle websocket connection
    await websocket.accept()
    try:
        for each_snap_md in query_result:
            await websocket.send_json(each_snap_md)
        #print("DEBUG: DISCONNECT")
    except WebSocketDisconnect:
        pass
    
    #print("DEBUG: CLOSING?")
    await websocket.close()
    #print("DEBUG: CLOSED")
    
    return

# .....................................................................................................................

async def snapshots_ws_get_many_images_by_time_range(websocket):
    
    # Get information from route url
    camera_select = websocket.path_params["camera_select"]
    start_time = websocket.path_params["start_time"]
    end_time = websocket.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_snapshot_collection(camera_select)
    epoch_ms_list = get_epoch_ms_list_in_time_range(collection_ref, start_ems, end_ems, SNAP_EPOCH_MS_FIELD,
                                                    ascending_order = False)
    
    # Handle websocket connection
    await websocket.accept()
    try:
        
        # First send the ems list data as a reference for which snapshot images are being sent
        await websocket.send_json(epoch_ms_list)
        
        # Next send every image
        for each_snap_ems in epoch_ms_list:
            
            # Build pathing to snapshot image file
            image_load_path = build_snapshot_image_pathing(IMAGE_FOLDER, camera_select, each_snap_ems)
            if not os.path.exists(image_load_path):
                await websocket.send_bytes(0)
            
            # Load and send snapshot image data
            with open(image_load_path, "rb") as in_file:
                image_bytes = in_file.read()            
            await websocket.send_bytes(image_bytes)
        #print("DEBUG: DISCONNECT")
        
    except WebSocketDisconnect:
        pass
    
    #print("DEBUG: CLOSING?")
    await websocket.close()
    #print("DEBUG: CLOSED")
    
    return

# .....................................................................................................................

async def objects_ws_get_many_metadata_gz_by_time_range(websocket):
    
    # Get initial websocket connection & camera select info
    camera_select = websocket.path_params["camera_select"]
    start_time = websocket.path_params["start_time"]
    end_time = websocket.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = find_objs_by_time_range(collection_ref, start_ems, end_ems,
                                           return_ids_only = False, ascending_order = False)
    
    # Handle websocket connection
    await websocket.accept()
    try:
        for each_obj_md in query_result:
            encoded_obj_md = encode_jsongz_data(each_obj_md, 3)
            await websocket.send_bytes(encoded_obj_md)
            #await websocket.send_json(each_obj_md) # .send_json(..., mode = "binary") ???
        #print("DEBUG: DISCONNECT")
        
    except WebSocketDisconnect:
        pass
    
    #print("DEBUG: CLOSING?")
    await websocket.close()
    #print("DEBUG: CLOSED")
    
    return

# .....................................................................................................................

async def stations_ws_get_many_metadata_gz_by_time_range(websocket):
    
    # Get information from route url
    camera_select = websocket.path_params["camera_select"]
    start_time = websocket.path_params["start_time"]
    end_time = websocket.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_station_collection(camera_select)
    query_result = find_stns_by_time_range(collection_ref, start_ems, end_ems,
                                           return_ids_only = False, ascending_order = False)
    
    # Handle websocket connection
    await websocket.accept()
    try:        
        for each_station_md in query_result:
            encoded_stn_md = encode_jsongz_data(each_station_md, 3)
            await websocket.send_bytes(encoded_stn_md)
        #print("DEBUG: DISCONNECT")
        
    except WebSocketDisconnect:
        pass
    
    #print("DEBUG: CLOSING?")
    await websocket.close()
    #print("DEBUG: CLOSED")
    
    return

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_websocket_routes():
    
    # Bundle all websocket routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", *url_components])
    obj_url = lambda *url_components: url(OBJ_COLLECTION_NAME, *url_components)
    snap_url = lambda *url_components: url(SNAP_COLLECTION_NAME, *url_components)
    station_url = lambda *url_components: url(STATIONS_COLLECTION_NAME, *url_components)
    websocket_routes = \
    [
     Route("/websockets/info", websocket_route_info),
     
     WebSocketRoute(snap_url("stream-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
                    snapshots_ws_get_many_metadata_by_time_range),
     
     WebSocketRoute(snap_url("stream-many-images", "by-time-range", "{start_time}", "{end_time}"),
                    snapshots_ws_get_many_images_by_time_range),
     
     WebSocketRoute(obj_url("stream-many-metadata-gz", "by-time-range", "{start_time}", "{end_time}"),
                    objects_ws_get_many_metadata_gz_by_time_range),
     
     WebSocketRoute(station_url("stream-many-metadata-gz", "by-time-range", "{start_time}", "{end_time}"),
                    stations_ws_get_many_metadata_gz_by_time_range)
    ]
    
    return websocket_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Establish (global!) variable used to access the persistent image folder
IMAGE_FOLDER = build_base_image_pathing()

# Connection to mongoDB
MCLIENT = connect_to_mongo()


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


