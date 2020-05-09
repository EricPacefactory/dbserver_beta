#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 15:07:15 2020

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

import ujson

from starlette.responses import UJSONResponse 
from starlette.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_405_METHOD_NOT_ALLOWED


# ---------------------------------------------------------------------------------------------------------------------
#%% Response functions

# .....................................................................................................................

def no_data_response(error_message, additional_response_dict = None):
    
    ''' Helper function to respond when there is no data '''
    
    response_dict = {"error": error_message}
    if additional_response_dict is not None:
        response_dict.update(additional_response_dict)
    
    return UJSONResponse({"error": error_message}, status_code = HTTP_404_NOT_FOUND)

# .....................................................................................................................

def bad_request_response(error_message, additional_response_dict = None):
    
    ''' Helper function for bad requests '''
    
    response_dict = {"error": error_message}
    if additional_response_dict is not None:
        response_dict.update(additional_response_dict)
    
    return UJSONResponse({"error": error_message}, status_code = HTTP_400_BAD_REQUEST)

# .....................................................................................................................

def not_allowed_response(error_message, additional_response_dict = None):
    
    ''' Helper function for requests doing something they shouldn't (e.g. trying to save an existing entry) '''
    
    response_dict = {"error": error_message}
    if additional_response_dict is not None:
        response_dict.update(additional_response_dict)
        
    return UJSONResponse(response_dict, status_code = HTTP_405_METHOD_NOT_ALLOWED)

# .....................................................................................................................

def post_success_response(success_message = True, additional_response_dict = None):
    
    ''' Helper function for post requests, when data is successfully added to the db '''
    
    response_dict = {"success": success_message}
    if additional_response_dict is not None:
        response_dict.update(additional_response_dict)
        
    return UJSONResponse(response_dict, status_code = HTTP_201_CREATED)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Helpers

# .....................................................................................................................

def parse_ujson_response(ujson_response_object):
    
    # Check if the response is valid
    response_status_code = ujson_response_object.status_code
    if response_status_code == 404:
        return no_data_response("data not found")
    elif response_status_code != 200:
        return bad_request_response("bad request")
    
    # If we get here, we got a valid response and need to parsee the json string data
    response_json_data = ujson_response_object.body
    response_dict = ujson.loads(response_json_data)
    
    return response_dict

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap

