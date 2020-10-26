#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 27 12:46:31 2020

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

from time import perf_counter
from shutil import disk_usage, which
from subprocess import check_output

from local.lib.mongo_helpers import MCLIENT
from local.lib.mongo_helpers import get_collection_names_list, get_camera_names_list

from local.lib.pathing import BASE_DATA_FOLDER_PATH
from local.lib.response_helpers import calculate_time_taken_ms

from starlette.responses import UJSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create diagnostics routes

# .....................................................................................................................

def get_connections_info(request):
    
    ''' Route which returns info regarding the number of connections to mongoDB '''
    
    # For convenience
    connections_err =  {"error": "Couldn't access connections info!"}
    
    # Start timing
    t_start = perf_counter()
    
    # Use the 'admin' database to list out the current connections (globally?)
    admin_name = "admin"
    admin_ref = MCLIENT.get_database(admin_name)
    return_result = admin_ref.command("serverStatus").get("connections", connections_err)
    
    # End timing
    t_end = perf_counter()
    
    # Bundle returned data
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    return_result["time_taken_ms"] = time_taken_ms
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_index_tree(request):
    
    ''' Function which lists all indices across all collections/cameras '''
    
    # Initialize output
    indices_tree = {}
    
    # Start timing
    t_start = perf_counter()
    
    # Loop over every collection of every camera and get index information
    camera_names_list = get_camera_names_list(MCLIENT)
    for each_camera_name in camera_names_list:
        
        # Add each camera name to the tree
        indices_tree[each_camera_name] = {}
        
        # Get indices for each collection
        camera_collection_names_list = get_collection_names_list(MCLIENT, each_camera_name)
        for each_collection_name in camera_collection_names_list:
            
            # Get indexing info
            collection_ref = MCLIENT[each_camera_name][each_collection_name]
            index_info_dict = collection_ref.index_information()
            
            # Remove '_id' entries to declutter, since they can be assumed (why is there an extra underscore?)
            if "_id_" in index_info_dict:
                del index_info_dict["_id_"]
            
            # Add the list of (non-id) indexes to the tree, but only if the list is not empty (helps declutter)
            index_keys_list = list(index_info_dict.keys())
            list_not_empty = (len(index_keys_list) > 0)
            if list_not_empty:
                indices_tree[each_camera_name][each_collection_name] = index_keys_list
    
    # End timing
    t_end = perf_counter()
    
    # Add timing info to response
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    indices_tree["time_taken_ms"] = time_taken_ms
    
    return UJSONResponse(indices_tree)

# .....................................................................................................................

def get_memory_usage(request):
    
    '''
    Route which returns info about memory usage (RAM & swap)
    Relies on the output from the 'free' command, which looks like:
    
    $ free -b
                  total        used        free      shared  buff/cache   available
    Mem:     8359682048  1588756480  1553772544    44699648  5217153024  6440677376
    Swap:    2147479552           0  2147479552
    '''
    
    # First make sure the free command is available
    mem_cmd = "free"
    command_bin_path = which(mem_cmd)
    if command_bin_path is None:
        return_result = {"error": "Memory-check program ({}) is not present!".format(mem_cmd)}
        return UJSONResponse(return_result)
    
    # Initialize outputs, in case this fails
    ram_bytes_dict = "error"
    swap_bytes_dict = "error"
    ram_percent_usage = "error"
    
    # Start timing, mostly for fun (and vaguely useful as a heads-up if this command is super slow)
    t_start = perf_counter()
    
    try:
        # Get result from the free command and split into rows
        free_memory_response = check_output([mem_cmd, "-b"], universal_newlines = True)
        free_memory_response_rows = free_memory_response.splitlines()
        
        # Split rows of free command into header/RAM/swap
        headers_str_list = free_memory_response_rows[0].split()
        ram_str_list = free_memory_response_rows[1].split()
        swap_str_list = free_memory_response_rows[2].split()
        
        # Build RAM output
        ram_int_list = (int(each_str) for each_str in ram_str_list[1:])
        ram_bytes_dict = dict(zip(headers_str_list, ram_int_list))
        
        # Build swap output
        swap_int_list = (int(each_str) for each_str in swap_str_list[1:])
        swap_bytes_dict = dict(zip(headers_str_list, swap_int_list))
        
        # Try to calculate a simple 'human-readable' value for output
        ram_percent_usage = int(round(100 * ram_bytes_dict["used"] / ram_bytes_dict["total"]))
        
    except (ValueError, IndexError, TypeError) as err:
        print("Error checking memory usage ({})".format(err.__class__.__name__))
        print(err)
    
    # End timing
    t_end = perf_counter()
    
    # Bundle returned data
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    return_result = {"ram_bytes": ram_bytes_dict,
                     "swap_bytes": swap_bytes_dict,
                     "ram_percent_usage": ram_percent_usage,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_disk_usage_for_images(request):
    
    ''' Route which returns info regarding the current disk (HDD/SSD) usage '''
    
    # Start timing
    t_start = perf_counter()
    
    # Find the disk usage of the partition holding the file system data
    total_bytes, used_bytes, free_bytes = disk_usage(BASE_DATA_FOLDER_PATH)
    
    # Calculate a simple 'human-readable' value for output
    percent_usage = int(round(100 * used_bytes / total_bytes))
    
    # End timing
    t_end = perf_counter()
    
    # Bundle returned data
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    return_result = {"total_bytes": total_bytes,
                     "used_bytes": used_bytes,
                     "free_bytes": free_bytes,
                     "percent_usage": percent_usage,
                     "note": "Usage for drive containing file system data only! May not account for metadata storage",
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_metadata_bytes_per_camera(request):
    
    ''' Route which returns info regarding the current disk usage for mongoDB data, per camera '''
    
    # Initialize output
    size_if_error = -1
    size_per_camera_bytes = {}
    
    # Start timing
    t_start = perf_counter()
    
    # Get list of cameras (mongoDB 'dbs') that we're interested in
    camera_names_list = get_camera_names_list(MCLIENT)
    
    # Find the 'sizeOnDisk' of each database (i.e. camera) on mongoDB
    total_size_on_disk_bytes = 0
    databases_info_list = MCLIENT.list_databases()
    for each_list_entry in databases_info_list:
        
        # Skip non-camera camera entries
        db_name = each_list_entry.get("name")
        if db_name not in camera_names_list:
            continue
        
        # Get/store sizing info
        size_on_disk_bytes = int(each_list_entry.get("sizeOnDisk", size_if_error))
        size_per_camera_bytes[db_name] = size_on_disk_bytes
        total_size_on_disk_bytes += size_on_disk_bytes if size_on_disk_bytes > 0 else 0
    
    # End timing
    t_end = perf_counter()
    
    # Bundle returned data
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    return_result = {"camera_metadata_bytes": size_per_camera_bytes,
                     "total_bytes": total_size_on_disk_bytes,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_document_count_tree(request):
    
    ''' Function which lists all database -> collection -> document counts '''
    
    # Initialize output
    doc_count_tree = {}
    
    # Start timing
    t_start = perf_counter()
    
    # Loop over every collection of every camera and count all documents
    camera_names_list = sorted(get_camera_names_list(MCLIENT))
    for each_camera_name in camera_names_list:
        
        # Add each camera name to the tree
        doc_count_tree[each_camera_name] = {}
        
        # Loop over all collections for the given camera
        camera_collection_names_list = sorted(get_collection_names_list(MCLIENT, each_camera_name))
        for each_collection_name in camera_collection_names_list:
            
            # Get the document count for each collection
            collection_ref = MCLIENT[each_camera_name][each_collection_name]
            collection_document_count = collection_ref.count_documents({})
            
            # Store results nested by camera & collection name
            doc_count_tree[each_camera_name][each_collection_name] = collection_document_count
    
    # End timing
    t_end = perf_counter()
    
    # Add timing info to response
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    doc_count_tree["time_taken_ms"] = time_taken_ms
    
    return UJSONResponse(doc_count_tree)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_diagnostics_routes():
    
    # Bundle all diagnostics routes
    diagnostics_routes = \
    [
     Route("/get-connections-info", get_connections_info),
     Route("/get-index-tree", get_index_tree),
     Route("/get-memory-usage", get_memory_usage),
     Route("/get-disk-usage", get_disk_usage_for_images),
     Route("/get-metadata-usage", get_metadata_bytes_per_camera),
     Route("/get-document-count-tree", get_document_count_tree)
    ]
    
    return diagnostics_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Nothing so far!


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


