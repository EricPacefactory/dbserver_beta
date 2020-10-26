#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 15:51:25 2020

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

from time import sleep

import pymongo

from local.lib.environment import get_mongo_protocol, get_mongo_host, get_mongo_port
from local.lib.quitters import ide_quit


# ---------------------------------------------------------------------------------------------------------------------
#%% Define classes

# .....................................................................................................................

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define functions

# .....................................................................................................................

def check_mongo_connection(mongo_client):
    
    '''
    Helper function which checks mongo connection. Also returns the mongo server info (as a dictionary)
    Inputs:
        mongo_client: (pymongo.MongoClient object)
    
    Outputs:
        is_connected (boolean), server_info_dict
    '''
    
    # Initialize outputs
    is_connected = False
    server_info_dict = {}
    
    # Try to make a request from mongo
    try:
        server_info_dict = mongo_client.server_info()
        is_connected = True
    except pymongo.errors.ServerSelectionTimeoutError:
        pass
    
    return is_connected, server_info_dict

# .....................................................................................................................

def connect_to_mongo(connection_timeout_ms = 2500, max_connection_attempts = 10):
    
    ''' Helper function used to establish a conection to mongoDB '''
    
    # Initialize outputs
    connection_success = False
    mongo_client = None
    
    # For clarity
    connect_immediately = True
    log_name = "dbserver"
    max_idle_time_ms = (60 * 60 * 1000)
    
    # Get configuration data
    mongo_protocol = get_mongo_protocol()
    mongo_host = get_mongo_host()
    mongo_port = get_mongo_port()
    
    # Build database access url
    mongo_url = "{}://{}:{}/".format(mongo_protocol, mongo_host, mongo_port)
    
    # Get database object for manipulation
    mongo_client = pymongo.MongoClient(mongo_url,
                                       tz_aware = False,
                                       serverSelectionTimeoutMS = connection_timeout_ms,
                                       connect = connect_immediately,
                                       appname = log_name,
                                       maxIdleTimeMS = max_idle_time_ms)
    
    # Repeatedly try to connect to MongoDB
    try:
        for k in range(max_connection_attempts):
            connection_success, server_info = check_mongo_connection(mongo_client)
            if connection_success:
                break
            
            # If we get here, we didn't connect, so provide some feedback and try again
            print("",
                  "ERROR:",
                  "Server couldn't connect to database",
                  "@ {}".format(mongo_url),
                  "  --> Trying again (attempt {})".format(1 + k), sep = "\n", flush = True)
            sleep(3)
        
        # Print additional warning indicator if we fail to connect after repeated attempts
        if not connection_success:
            print("",
                  "Connection attempts to database failed!",
                  "Server will start up anyways, but requests may not work...", sep = "\n", flush = True)
        
    except KeyboardInterrupt:
        mongo_client.close()
        ide_quit("Connection error. Quitting...")
    
    return connection_success, mongo_client

# .....................................................................................................................

def post_one_to_mongo(mongo_client, database_name, collection_name, data_to_insert):
    
    ''' Helper function for posting a single entry into mongodb '''
    
    db_ref = mongo_client[database_name]
    collection_ref = db_ref[collection_name]
    
    # Try to post the data
    post_success = False
    mongo_response = {}
    try:
        collection_ref.insert_one(data_to_insert)
        post_success = True
        
    except pymongo.errors.DuplicateKeyError as dupe_key_error:
        mongo_response = {"error": "duplicate key error",
                          "details": dupe_key_error.details}
    
    except Exception as err:
        mongo_response = {"error": str(err)}
    
    return post_success, mongo_response

# .....................................................................................................................

def post_many_to_mongo(mongo_client, database_name, collection_name, data_to_insert):
    
    ''' Helper function which is able to post many entries into mongodb at once '''
    
    db_ref = mongo_client[database_name]
    collection_ref = db_ref[collection_name]
    
    # Try to post the data
    data_to_insert_list = convert_to_many(data_to_insert)
    post_success = False
    mongo_response = {}
    try:
        # Insert many without ordering
        # Note that duplicate entries will cause a BulkWriteError,
        #   however, using un-ordered insert means all non-duplicate entries will still be inserted!
        collection_ref.insert_many(data_to_insert_list, ordered = False)
        post_success = True
        
    except pymongo.errors.BulkWriteError as bulk_write_error:
        # In this case, duplicates exist, but un-ordered insert means all non-duplicates will still succeed
        post_success = False
        cleaned_details = bulk_write_error.details
        cleaned_details.pop("writeErrors")
        mongo_response = {"error": "bulk write error",
                          "details": cleaned_details}
        
    except Exception as err:
        mongo_response = {"error": str(err)}
    
    return post_success, mongo_response

# .....................................................................................................................

def convert_to_many(data_to_insert):
    
    ''' Helper function which ensures all data is treated as 'many' i.e. as a list '''
    
    if type(data_to_insert) is list:
        return data_to_insert
    
    return list(data_to_insert)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Indexing helpers

# .....................................................................................................................

def check_collection_indexing(collection_ref, index_key_list):
    
    ''' Helper function which can be used to check if a set of key-names are indexed already '''
    
    # Get current keys being indexed
    current_index_info_dict = collection_ref.index_information()
    
    # Loop over all target keys and check if they're in the set of keys already indexed
    target_set_list = []
    for each_target_key in index_key_list:
        target_is_set = any(each_target_key in each_key for each_key in current_index_info_dict.keys())
        target_set_list.append(target_is_set)
    
    # Consider the indexes set if all the target keys are already set
    indexes_already_set = all(target_set_list)
    
    return indexes_already_set

# .....................................................................................................................

def set_collection_indexing(collection_ref, index_key_list):
    
    ''' Helper function which can be used to set up indexing on a list of key-names '''
    
    # Add each key, one-by-one to the collection indexing
    mongo_response_list = [collection_ref.create_index(each_target_key) for each_target_key in index_key_list]
    
    return mongo_response_list

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Mongo client helpers

# .....................................................................................................................

def get_camera_names_list(mongo_client, sort_names = True):
    
    ''' Helper function which gets all camera names (equiv. to database names, not including mongo built-ins) '''
    
    # Request data from all dbs
    all_db_names_list = mongo_client.list_database_names()
    
    # Remove built-in databases
    ignore_db_names = {"admin", "local", "config"}
    camera_names_list = [each_name for each_name in all_db_names_list if each_name not in ignore_db_names]
    
    # Sort if needed
    if sort_names:
        camera_names_list = sorted(camera_names_list)
    
    return camera_names_list

# .....................................................................................................................

def get_collection_names_list(mongo_client, camera_select):
    
    ''' Helper function which returns a list of collection names, for a given camera '''
    
    return mongo_client[camera_select].list_collection_names()

# .....................................................................................................................

def remove_camera_entry(mongo_client, camera_select):
    
    ''' Helper function which removes a camera entry from the database '''
    
    return mongo_client.drop_database(camera_select)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Setup initial (shared) connection

# Import from other scripts (e.g. routes) to access mongo!
_, MCLIENT = connect_to_mongo()
#print("DEBUG: MongoClient created!")

# Set up git usage



# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap



