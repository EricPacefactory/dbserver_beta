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

from starlette.routing import Route



# ---------------------------------------------------------------------------------------------------------------------
#%% Create data posting routes

# .....................................................................................................................

def post_metadata(collection_name):
    
    '''
    Function used to create functions used for handling metadata POSTing
    Inputs:
        collection_name: (String) --> Specify the collection of metadata to handle. 
                                      Should be something like "snapshots", "objects", "camerainfo" etc.
    
    Outputs:
        post_metadata_by_collection (function)
        
        (The output should be used to create a Route object)
        (Ex: Route("/url/to/post/metadata", post_metadata_by_collection)
    '''
    
    async def post_metadata_by_collection(request):
        
        # Convert post data to python data
        post_data = await request.json()
        
        # Get the camera selection from the url path, and send off the data!
        camera_select = request.path_params["camera_select"]
        post_success, mongo_response = post_many_to_mongo(MCLIENT, camera_select, collection_name, post_data)
        
        # Return an error response if there was a problem posting 
        # Hard-coded: assuming the issue is with duplicate entries
        if not post_success:
            additional_response_dict = {"mongo_response": mongo_response}
            error_message = "Error posting metadata. Entries likely exist already!"
            return not_allowed_response(error_message, additional_response_dict)
        
        return post_success_response()
    
    return post_metadata_by_collection

# .....................................................................................................................

def post_image(image_category):
    
    ''' 
    Function used to create functions used for handling image POSTing 
    Inputs:
        image_category: (String) --> Specify the category of image to handle. 
                                     Likely going to be either "snapshots" or "backgrounds"
    
    Outputs:
        post_image_by_category (function)
        
        (The output should be used to create a Route object)
        (Ex: Route("/url/to/post/image", post_image_by_category)
    '''

    async def post_image_by_category(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        image_epoch_ms = request.path_params["epoch_ms"]
        
        # Generate the image file pathing, so we can first make sure the image doesn't already exist
        image_save_path = build_image_pathing(IMAGE_FOLDER, camera_select, image_category, image_epoch_ms,
                                              create_folder_if_missing = True)
        
        # Return error if the image file has already been stored
        if os.path.exists(image_save_path):
            error_message = "Can't upload, image already exists ({})".format(image_epoch_ms)
            return not_allowed_response(error_message)
        
        # Get the image data from the post body
        image_data = await request.body()
        if not image_data:
            error_message = "No {} image data in body ({})".format(image_category, image_epoch_ms)
            return bad_request_response(error_message)
        
        # Save the data to the filesystem (not mongodb!)
        with open(image_save_path, "wb") as out_file:
            out_file.write(image_data)
        
        return post_success_response(image_epoch_ms)
    
    return post_image_by_category

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
     Route(post_url("/bdb/metadata/camerainfo"), post_metadata("camerainfo"), methods=["POST"]),
     Route(post_url("/bdb/metadata/snapshots"), post_metadata("snapshots"), methods=["POST"]),
     Route(post_url("/bdb/metadata/objects"), post_metadata("objects"), methods=["POST"]),
     Route(post_url("/bdb/metadata/backgrounds"), post_metadata("backgrounds"), methods=["POST"]),
     Route(post_url("/bdb/image/snapshots/{epoch_ms:int}"), post_image("snapshots"), methods=["POST"]),
     Route(post_url("/bdb/image/backgrounds/{epoch_ms:int}"), post_image("backgrounds"), methods=["POST"])
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


