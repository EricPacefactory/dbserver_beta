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

from local.lib.mongo_helpers import connect_to_mongo, post_many_to_mongo
from local.lib.response_helpers import post_success_response, not_allowed_response, bad_request_response
from local.lib.image_pathing import build_base_image_pathing, build_image_pathing

from local.routes.objects import set_object_indexing, check_object_indexing, get_object_collection

from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create generic posting functions

# .....................................................................................................................

async def post_metadata_by_collection(request, collection_name):
    
    # Get data for posting
    camera_select = request.path_params["camera_select"]
    post_data_json = await request.json()
    
    # Send metadata to mongo
    post_success, mongo_response = post_many_to_mongo(MCLIENT, camera_select, collection_name, post_data_json)
    
    # Return an error response if there was a problem posting
    # Hard-coded: assuming the issue is with duplicate entries
    if not post_success:
        additional_response_dict = {"mongo_response": mongo_response}
        error_message = "Error posting {} metadata. Entries likely exist already!".format(collection_name)
        return not_allowed_response(error_message, additional_response_dict)
    
    return post_success_response()

# .....................................................................................................................

async def post_image_data_by_collection(request, collection_name):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    image_epoch_ms = request.path_params["epoch_ms"]
    
    # Generate the image file pathing, so we can first make sure the image doesn't already exist
    image_save_path = build_image_pathing(IMAGE_FOLDER, camera_select, collection_name, image_epoch_ms,
                                          create_folder_if_missing = True)
    
    # Return error if the image file has already been stored
    if os.path.exists(image_save_path):
        error_message = "Can't upload, image already exists ({})".format(image_epoch_ms)
        return not_allowed_response(error_message)
    
    # Get the image data from the post body
    image_data = await request.body()
    if not image_data:
        error_message = "No {} image data in body ({})".format(collection_name, image_epoch_ms)
        return bad_request_response(error_message)
    
    # Save the data to the filesystem (not mongodb!)
    with open(image_save_path, "wb") as out_file:
        out_file.write(image_data)
    
    return post_success_response(image_epoch_ms)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define metadata routes

# .....................................................................................................................

async def post_camerainfo(request):
    
    # For clarity
    collection_name = "camerainfo"
    camera_select = request.path_params["camera_select"]
    
    # Use standard metadata posting
    post_response = await post_metadata_by_collection(request, collection_name)
    
    # Try to set object indexing every time new camera info is posted
    # (which should be infrequent, but indicates the camera has reset)
    collection_ref = get_object_collection(camera_select)
    object_indexes_are_set = check_object_indexing(collection_ref)
    if not object_indexes_are_set:
        set_object_indexing(collection_ref)
        #print("  --> Object indexing set for {}".format(camera_select))
    
    return post_response

# .....................................................................................................................

async def post_background_metadata(request):
    
    # For clarity
    collection_name = "backgrounds"
    
    return await post_metadata_by_collection(request, collection_name)

# .....................................................................................................................

async def post_object_data(request):
    
    # For clarity
    collection_name = "objects"
    
    return await post_metadata_by_collection(request, collection_name)

# .....................................................................................................................

async def post_snapshot_metadata(request):
    
    # For clarity
    collection_name = "snapshots"
    
    return await post_metadata_by_collection(request, collection_name)

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Define image routes

# .....................................................................................................................

async def post_background_image_data(request):
    
    # For clarity
    collection_name = "backgrounds"
    
    return await post_image_data_by_collection(request, collection_name)

# .....................................................................................................................

async def post_snapshot_image_data(request):
    
    # For clarity
    collection_name = "snapshots"
    
    return await post_image_data_by_collection(request, collection_name)

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_posting_routes():
    
    # Bundle all data posting routes
    post_url = lambda post_route: "".join(["/{camera_select:str}", post_route])
    post_data_routes = \
    [
     Route(post_url("/bdb/metadata/camerainfo"), post_camerainfo, methods=["POST"]),
     Route(post_url("/bdb/metadata/backgrounds"), post_background_metadata, methods=["POST"]),
     Route(post_url("/bdb/metadata/objects"), post_object_data, methods=["POST"]),
     Route(post_url("/bdb/metadata/snapshots"), post_snapshot_metadata, methods=["POST"]),
     Route(post_url("/bdb/image/backgrounds/{epoch_ms:int}"), post_background_image_data, methods=["POST"]),
     Route(post_url("/bdb/image/snapshots/{epoch_ms:int}"), post_snapshot_image_data, methods=["POST"])
    ]
    
    return post_data_routes

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


