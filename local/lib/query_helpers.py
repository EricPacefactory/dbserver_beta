#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  8 17:01:25 2020

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

from local.lib.timekeeper_utils import any_time_type_to_epoch_ms

from pymongo import ASCENDING, DESCENDING


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

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

def url_time_to_epoch_ms(url_time):
    
    # For clarity
    url_time_typed = int(url_time) if url_time.isnumeric() else url_time
    ems_time = any_time_type_to_epoch_ms(url_time_typed)
    
    return ems_time

# .....................................................................................................................

def start_end_times_to_epoch_ms(start_time, end_time):
    return url_time_to_epoch_ms(start_time), url_time_to_epoch_ms(end_time)

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Query functions

# .....................................................................................................................
    
def get_all_ids(collection_ref, sort_field = "_id", ascending_order = True):
    
    # Build query
    query_dict = None
    projection_dict = {}
    
    # Request data from the db
    sort_order = ASCENDING if ascending_order else DESCENDING
    query_result = collection_ref.find(query_dict, projection_dict).sort(sort_field, sort_order)
    
    return query_result

# .....................................................................................................................

def get_one_metadata(collection_ref, target_field, target_value):
    
    # Build query
    query_dict = {target_field: target_value}
    projection_dict = None
    
    # Request data from the db
    query_result = collection_ref.find_one(query_dict, projection_dict)
    
    return query_result

# .....................................................................................................................
    
def get_newest_metadata(collection_ref, epoch_ms_field = "_id"):
    
    # Request data from the db
    query_result = collection_ref.find().sort(epoch_ms_field, DESCENDING).limit(1)
    
    # Pull out a single entry (there should only be one or it could be empty)
    metadata_dict = first_of_query(query_result, return_if_missing = None)
    no_newest_metadata = (metadata_dict is None)
    
    return no_newest_metadata, metadata_dict

# .....................................................................................................................

def get_oldest_metadata(collection_ref, epoch_ms_field = "_id"):
    
    # Request data from the db
    query_result = collection_ref.find().sort(epoch_ms_field, ASCENDING).limit(1)
    
    # Pull out a single entry (there should only be one or it could be empty)
    metadata_dict = first_of_query(query_result, return_if_missing = None)
    no_oldest_metadata = (metadata_dict is None)
    
    return no_oldest_metadata, metadata_dict

# .....................................................................................................................

def get_closest_metadata_before_target_ems(collection_ref, target_ems, epoch_ms_field = "_id"):
    
    '''
    Helper function which returns the query result for the closest entry 
    in a collection before a given epoch_ms time. 
    Often needed for data that is relevant for time intervals (e.g. camera info or backgrounds)
    
    Inputs:
        collection_ref --> a pymongo.MongoClient(...) object, which has a collection selected already
        
        target_ems --> (integer) Epoch millisecond value which is used as target time from which we'll
                       search backwards from to find the 'closest' entry in the collection
                       
        epoch_ms_field --> (string) The field which we represents an epoch_ms time value for the given collection
        
    Outputs:
        no_older_entry (boolean), entry_dict (dictionary or None)
    
    '''
    
    # Find the first entry before (or at) the target time
    query_dict = {epoch_ms_field: {"$lte": target_ems}}
    projection_dict = None
    
    # Request data from the db
    query_result = collection_ref.find(query_dict, projection_dict).sort(epoch_ms_field, DESCENDING).limit(1)
    
    # Try to get the newest data from the given list
    # (which may be empty if there is no entry before the target time)
    metadata_dict = first_of_query(query_result, return_if_missing = None)
    no_older_metadata = (metadata_dict is None)
        
    return no_older_metadata, metadata_dict

# .....................................................................................................................

def get_many_metadata_since_target_ems(collection_ref, target_ems, epoch_ms_field = "_id",
                                       ascending_order = True):
    
    # Build query
    query_dict = {epoch_ms_field: {"$gte": target_ems}}
    projection_dict = None
    
    # Request data from the db
    sort_order = ASCENDING if ascending_order else DESCENDING
    query_result = collection_ref.find(query_dict, projection_dict).sort(epoch_ms_field, sort_order)
    
    return query_result

# .....................................................................................................................

def get_many_metadata_in_time_range(collection_ref, start_ems, end_ems, epoch_ms_field = "_id",
                                    ascending_order = True):
    
    # Build query
    query_dict = {epoch_ms_field: {"$gte": start_ems, "$lt": end_ems}}
    projection_dict = None
    
    # Request data from the db
    sort_order = ASCENDING if ascending_order else DESCENDING
    query_result = collection_ref.find(query_dict, projection_dict).sort(epoch_ms_field, sort_order)
    
    return query_result

# .....................................................................................................................

def get_epoch_ms_list_in_time_range(collection_ref, start_ems, end_ems, epoch_ms_field = "_id",
                                    ascending_order = True):
    
    # Build query
    query_dict = {epoch_ms_field: {"$gte": start_ems, "$lt": end_ems}}
    projection_dict = {}
    
    # Request data from the db
    sort_order = ASCENDING if ascending_order else DESCENDING
    query_result = collection_ref.find(query_dict, projection_dict).sort(epoch_ms_field, sort_order)
    
    # Pull out the epoch values into a list, instead of returning a list of dictionaries
    epoch_ms_list = [each_entry[epoch_ms_field] for each_entry in query_result]    
    
    return epoch_ms_list

# .....................................................................................................................

def get_count_in_time_range(collection_ref, start_ems, end_ems, epoch_ms_field = "_id"):
    
    # Build query
    query_dict = {epoch_ms_field: {"$gte": start_ems, "$lt": end_ems}}
    projection_dict = None
    
    # Request data from the db
    query_result = collection_ref.count_documents(query_dict, projection_dict)
    
    return query_result

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap

