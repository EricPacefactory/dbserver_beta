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

from local.routes.snapshots import COLLECTION_NAME as SNAP_COLLECTION_NAME
from local.routes.snapshots import EPOCH_MS_FIELD as SNAP_EPOCH_MS_FIELD
from local.routes.snapshots import get_snapshot_collection

from local.routes.objects import COLLECTION_NAME as OBJ_COLLECTION_NAME
from local.routes.objects import find_by_time_range as find_objs_by_time_range
from local.routes.objects import get_object_collection

from local.routes.stations import COLLECTION_NAME as STATIONS_COLLECTION_NAME
from local.routes.stations import find_by_time_range as find_stns_by_time_range
from local.routes.stations import get_station_collection

from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocketDisconnect


# ---------------------------------------------------------------------------------------------------------------------
#%% Create object routes

# .....................................................................................................................

async def objects_ws_get_many_metadata_by_time_range(websocket):
    
    # Get initial websocket connection & camera select info
    camera_select = websocket.path_params["camera_select"]
    start_time = websocket.path_params["start_time"]
    end_time = websocket.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_object_collection(camera_select)
    query_result = find_objs_by_time_range(collection_ref, start_ems, end_ems, return_ids_only = False)
    
    # Handle websocket connection
    await websocket.accept()
    try:
        for each_obj_md in query_result:
            await websocket.send_json(each_obj_md) # .send_json(..., mode = "binary") ???
        #print("DEBUG: DISCONNECT")
    except WebSocketDisconnect:
        pass
    
    #print("DEBUG: CLOSING?")
    await websocket.close()
    #print("DEBUG: CLOSED")
    
    return

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
    query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, SNAP_EPOCH_MS_FIELD)
    
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

async def stations_ws_get_many_metadata_by_time_range(websocket):
    
    # Get information from route url
    camera_select = websocket.path_params["camera_select"]
    start_time = websocket.path_params["start_time"]
    end_time = websocket.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_station_collection(camera_select)
    query_result = find_stns_by_time_range(collection_ref, start_ems, end_ems, return_ids_only = False)
    
    # Handle websocket connection
    await websocket.accept()
    try:        
        for each_station_md in query_result:
            await websocket.send_json(each_station_md)
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
    
    # Set up websocket URL tag
    websocket_url_prefix = "ws"
    
    # Bundle all websocket routes
    url = lambda *url_components: "/".join(["/{camera_select:str}", websocket_url_prefix, *url_components])
    obj_url = lambda *url_components: url(OBJ_COLLECTION_NAME, *url_components)
    snap_url = lambda *url_components: url(SNAP_COLLECTION_NAME, *url_components)
    station_url = lambda *url_components: url(STATIONS_COLLECTION_NAME, *url_components)
    websocket_routes = \
    [
     WebSocketRoute(obj_url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
                    objects_ws_get_many_metadata_by_time_range),
     
     WebSocketRoute(snap_url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
                    snapshots_ws_get_many_metadata_by_time_range),
     
     WebSocketRoute(station_url("get-many-metadata", "by-time-range", "{start_time}", "{end_time}"),
                    stations_ws_get_many_metadata_by_time_range)
    ]
    
    return websocket_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Connection to mongoDB
MCLIENT = connect_to_mongo()


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


