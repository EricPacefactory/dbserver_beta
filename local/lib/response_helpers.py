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

from starlette.responses import UJSONResponse 
from starlette.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_405_METHOD_NOT_ALLOWED


# ---------------------------------------------------------------------------------------------------------------------
#%% Response functions

# .....................................................................................................................

def first_of_query(query_result, return_if_missing = None):
    
    ''' 
    Helper function for dealing with mongodb queries that are expected to return 1 result in a 'list' format
    Also handles errors if there is no result
    '''
    
    try:
        return_result = next(query_result)
    except StopIteration:
        return_result = return_if_missing
    
    return return_result

# .....................................................................................................................

def no_data_response(error_message):
    
    ''' Helper function to respond when there is no data '''
    
    return UJSONResponse({"error": error_message}, status_code = HTTP_404_NOT_FOUND)

# .....................................................................................................................

def bad_request_response(error_message):
    
    ''' Helper function for bad requests '''
    
    return UJSONResponse({"error": error_message}, status_code = HTTP_400_BAD_REQUEST)

# .....................................................................................................................

def not_allowed_response(error_message):
    
    ''' Helper function for requests doing something they shouldn't (e.g. trying to save an existing entry) '''
    
    return UJSONResponse({"error": error_message}, status_code = HTTP_405_METHOD_NOT_ALLOWED)

# .....................................................................................................................

def post_success_response(success_message = True):
    
    ''' Helper function for post requests, when data is successfully added to the db '''
    
    return UJSONResponse({"success": success_message}, status_code = HTTP_201_CREATED)


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap
