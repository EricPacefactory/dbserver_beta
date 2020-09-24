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
from local.lib.query_helpers import get_closest_metadata_before_target_ems, get_one_metadata

from local.lib.response_helpers import encode_jsongz_data

from local.lib.image_pathing import build_base_image_pathing
from local.lib.image_pathing import build_snapshot_image_pathing, build_background_image_pathing

from local.routes.backgrounds import COLLECTION_NAME as BG_COLLECTION_NAME
from local.routes.backgrounds import EPOCH_MS_FIELD as BG_EPOCH_MS_FIELD
from local.routes.backgrounds import get_background_collection

from local.routes.snapshots import COLLECTION_NAME as SNAP_COLLECTION_NAME
from local.routes.snapshots import EPOCH_MS_FIELD as SNAP_EPOCH_MS_FIELD
from local.routes.snapshots import get_snapshot_collection

from local.routes.objects import COLLECTION_NAME as OBJ_COLLECTION_NAME
from local.routes.objects import OBJ_ID_FIELD
from local.routes.objects import find_by_time_range as find_objs_by_time_range
from local.routes.objects import get_object_collection

from local.routes.stations import COLLECTION_NAME as STATIONS_COLLECTION_NAME
from local.routes.stations import STN_ID_FIELD
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
    msg_list = ["Experimental/not finished!",
                "All data transfers use binary data!",
                "Each route will first send a gzipped-json list of ids/times representing the data being streamed",
                "The background/snapshot routes stream: metadata-jpg-metadata-jpg-... etc",
                "The object/station metadata routes stream gzipped-json data"]
    
    info_dict = {"info": msg_list}
    
    return UJSONResponse(info_dict)

# .....................................................................................................................

def ws_backgrounds_stream_many_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    raise NotImplementedError("Not a real route!")

async def backgrounds_ws_stream_many_metadata_and_images_by_time_range(ws_request):
    
    # Get information from route url
    camera_select = ws_request.path_params["camera_select"]
    start_time = ws_request.path_params["start_time"]
    end_time = ws_request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db. Careful to get 'active' entry along with range entries
    collection_ref = get_background_collection(camera_select)
    no_older_entry, active_entry = get_closest_metadata_before_target_ems(collection_ref, start_ems, BG_EPOCH_MS_FIELD)
    range_query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, BG_EPOCH_MS_FIELD)
    
    # Build output
    bg_md_list = [] if no_older_entry else [active_entry]
    bg_md_list += list(range_query_result)
    epoch_ms_list = [each_md[BG_EPOCH_MS_FIELD] for each_md in bg_md_list]
    
    # Handle websocket connection
    await ws_request.accept()
    try:
        # First send the ems list data as a reference for which background metadata are being sent
        encoded_ems_list = encode_jsongz_data(epoch_ms_list, 0)
        await ws_request.send_bytes(encoded_ems_list)
        
        # Next send both metadata & image data (sequentially)
        for each_bg_md, each_snap_ems in zip(bg_md_list, epoch_ms_list):
            
            # First send metadata
            await ws_request.send_json(each_bg_md, mode = "binary")
            
            # Build pathing to background image file
            image_load_path = build_background_image_pathing(IMAGE_FOLDER, camera_select, each_snap_ems)
            if not os.path.exists(image_load_path):
                await ws_request.send_bytes(b'')
            
            # Load and send background image data
            with open(image_load_path, "rb") as in_file:
                image_bytes = in_file.read()
            await ws_request.send_bytes(image_bytes)
        
    except WebSocketDisconnect:
        pass
    
    except Exception:
        pass
    
    # Make sure we shut-down the connection when we're done
    await ws_request.close()
    
    return

# .....................................................................................................................

def ws_snapshots_stream_many_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    raise NotImplementedError("Not a real route!")

async def snapshots_ws_stream_many_metadata_and_images_by_time_range_n_samples(ws_request):
    
    # Get information from route url
    camera_select = ws_request.path_params["camera_select"]
    start_time = ws_request.path_params["start_time"]
    end_time = ws_request.path_params["end_time"]
    n_samples = ws_request.path_params["n"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # Request data from the db
    collection_ref = get_snapshot_collection(camera_select)
    query_result = get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, SNAP_EPOCH_MS_FIELD,
                                                   ascending_order = False)
    
    # Convert result to a list so we can subsample from it
    snap_md_list = list(query_result)
    num_samples_total = len(snap_md_list)
    
    # Only sub-sample if the number of requested samples is less than the actual number of samples
    # -> Also, treat n_samples = 0 as a special case, indicating no-subsampling
    need_to_subsample = (0 < n_samples < num_samples_total)
    if need_to_subsample:
        step_factor = (num_samples_total - 1) / (n_samples - 1)
        idx_list = [int(round(k * step_factor)) for k in range(n_samples)]
        snap_md_list = [snap_md_list[each_idx] for each_idx in idx_list]
    
    # Build epoch listing
    epoch_ms_list = [each_snap_md[SNAP_EPOCH_MS_FIELD] for each_snap_md in snap_md_list]
    
    # Handle websocket connection
    await ws_request.accept()
    try:
        # First send the ems list data as a reference for which snapshot metadata are being sent, then send each entry
        encoded_ems_list = encode_jsongz_data(epoch_ms_list, 0)
        await ws_request.send_bytes(encoded_ems_list)
        
        # Next send both metadata & image data (sequentially)
        for each_snap_md, each_snap_ems in zip(snap_md_list, epoch_ms_list):
            
            # First send metadata
            await ws_request.send_json(each_snap_md, mode = "binary")
            
            # Build pathing to snapshot image file
            image_load_path = build_snapshot_image_pathing(IMAGE_FOLDER, camera_select, each_snap_ems)
            if not os.path.exists(image_load_path):
                await ws_request.send_bytes(b'')
            
            # Load and send snapshot image data
            with open(image_load_path, "rb") as in_file:
                image_bytes = in_file.read()
            await ws_request.send_bytes(image_bytes)
        
    except WebSocketDisconnect:
        pass
    
    except Exception:
        pass
    
    # Make sure we shut-down the connection when we're done
    await ws_request.close()
    
    return

# .....................................................................................................................

def ws_objects_stream_many_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    raise NotImplementedError("Not a real route!")

async def objects_ws_stream_many_metadata_gz_by_time_range(ws_request):
    
    # Get initial websocket connection & camera select info
    camera_select = ws_request.path_params["camera_select"]
    start_time = ws_request.path_params["start_time"]
    end_time = ws_request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # First request all object ids from the db
    collection_ref = get_object_collection(camera_select)
    query_result = find_objs_by_time_range(collection_ref, start_ems, end_ems,
                                           return_ids_only = True, ascending_order = False)
    obj_ids_list = [each_entry[OBJ_ID_FIELD] for each_entry in query_result]
    
    # Handle websocket connection
    await ws_request.accept()
    try:
        
        # First send the list of ids as a reference for which object data is are being sent, then send each entry
        encoded_ids_list = encode_jsongz_data(obj_ids_list, 0)
        await ws_request.send_bytes(encoded_ids_list)
        for each_obj_id in obj_ids_list:
            each_obj_md = get_one_metadata(collection_ref, OBJ_ID_FIELD, each_obj_id)
            encoded_obj_md = encode_jsongz_data(each_obj_md, 3)
            await ws_request.send_bytes(encoded_obj_md)
        #print("DEBUG: DISCONNECT")
        
    except WebSocketDisconnect:
        pass
    
    except Exception:
        pass
    
    #print("DEBUG: CLOSING?")
    
    # Make sure we shut-down the connection when we're done
    await ws_request.close()
    #print("DEBUG: CLOSED")
    
    return

# .....................................................................................................................

def ws_stations_stream_many_DUMMY(): # Included since spyder IDE hides async functions in outline view!
    raise NotImplementedError("Not a real route!")

async def stations_ws_stream_many_metadata_gz_by_time_range(ws_request):
    
    # Get information from route url
    camera_select = ws_request.path_params["camera_select"]
    start_time = ws_request.path_params["start_time"]
    end_time = ws_request.path_params["end_time"]
    
    # Convert start/end times to ems values
    start_ems, end_ems = start_end_times_to_epoch_ms(start_time, end_time)
    
    # First request all station data ids from the db
    collection_ref = get_station_collection(camera_select)
    query_result = find_stns_by_time_range(collection_ref, start_ems, end_ems,
                                           return_ids_only = True, ascending_order = False)
    stn_ids_list = [each_entry[STN_ID_FIELD] for each_entry in query_result]
    
    # Handle websocket connection
    await ws_request.accept()
    try:
        
        # First send the list of ids as a reference for which station data is are being sent
        encoded_ids_list = encode_jsongz_data(stn_ids_list, 0)
        await ws_request.send_bytes(encoded_ids_list)
        
        # Next send every individual metadata entry (with gzip encoding)
        for each_stn_id in stn_ids_list:
            each_stn_md = get_one_metadata(collection_ref, STN_ID_FIELD, each_stn_id)
            encoded_stn_md = encode_jsongz_data(each_stn_md, 3)
            await ws_request.send_bytes(encoded_stn_md)
        #print("DEBUG: DISCONNECT")
        
    except WebSocketDisconnect:
        pass
    
    except Exception:
        pass
    
    #print("DEBUG: CLOSING?")
    # Make sure we shut-down the connection when we're done
    await ws_request.close()
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
    bg_url = lambda *url_components: url(BG_COLLECTION_NAME, *url_components)
    snap_url = lambda *url_components: url(SNAP_COLLECTION_NAME, *url_components)
    obj_url = lambda *url_components: url(OBJ_COLLECTION_NAME, *url_components)
    station_url = lambda *url_components: url(STATIONS_COLLECTION_NAME, *url_components)
    websocket_routes = \
    [
     Route("/websockets/info", websocket_route_info),
     
     WebSocketRoute(bg_url("stream-many-metadata-and-images", "by-time-range", "{start_time}", "{end_time}"),
                    backgrounds_ws_stream_many_metadata_and_images_by_time_range),
     
     WebSocketRoute(snap_url("stream-many-metadata-and-images", "by-time-range", "n-samples",
                             "{start_time}", "{end_time}", "{n:int}"),
                    snapshots_ws_stream_many_metadata_and_images_by_time_range_n_samples),
     
     WebSocketRoute(obj_url("stream-many-metadata-gz", "by-time-range", "{start_time}", "{end_time}"),
                    objects_ws_stream_many_metadata_gz_by_time_range),
     
     WebSocketRoute(station_url("stream-many-metadata-gz", "by-time-range", "{start_time}", "{end_time}"),
                    stations_ws_stream_many_metadata_gz_by_time_range)
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


