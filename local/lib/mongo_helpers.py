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

def connect_to_mongo(connection_timeout_ms = 4000):
    
    ''' Helper function used to establish a conection to mongoDB '''
    
    # Get configuration data
    mongo_protocol = get_mongo_protocol()
    mongo_host = get_mongo_host()
    mongo_port = get_mongo_port()
    
    # Build database access url
    mongo_url = "{}://{}:{}/".format(mongo_protocol, mongo_host, mongo_port)
    
    # Get database object for manipulation
    mongo_client = pymongo.MongoClient(mongo_url, tz_aware = False, serverSelectionTimeoutMS = connection_timeout_ms)
    
    # Try to force a connection to the database
    try:
        mongo_client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError:
        print("",
              "ERROR:",
              "Server couldn't connect to database",
              "@ {}".format(mongo_url),
              "", sep = "\n")
        ide_quit("Connection error...")
    
    return mongo_client

# .....................................................................................................................

def post_one_to_mongo(mongo_client, database_name, collection_name, data_to_insert):
    
    ''' Helper function for posting a single entry into mongodb '''
    
    db_ref = mongo_client[database_name]
    collection_ref = db_ref[collection_name]
    
    # Try to post the data
    post_success = False
    try:
        collection_ref.insert_one(data_to_insert)
        post_success = True
    except pymongo.errors.DuplicateKeyError:
        pass
    
    return post_success

# .....................................................................................................................

def post_many_to_mongo(mongo_client, database_name, collection_name, data_to_insert, ordered_data = False):
    
    ''' Helper function which is able to post many entries into mongodb at once '''
    
    db_ref = mongo_client[database_name]
    collection_ref = db_ref[collection_name]
    
    # Try to post the data
    data_to_insert_list = convert_to_many(data_to_insert)
    post_success = False
    try:
        collection_ref.insert_many(data_to_insert_list, ordered = ordered_data)
        post_success = True
    except pymongo.errors.BulkWriteError:
        pass
    
    return post_success

# .....................................................................................................................

def convert_to_many(data_to_insert):
    
    ''' Helper function which ensures all data is treated as 'many' i.e. as a list '''
    
    if type(data_to_insert) is list:
        return data_to_insert
    
    return list(data_to_insert)

# .....................................................................................................................
# .....................................................................................................................
    
# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap



