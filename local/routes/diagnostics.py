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

from local.lib.image_pathing import build_base_image_pathing
from local.lib.response_helpers import calculate_time_taken_ms

from starlette.responses import UJSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create diagnostics routes

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
    
    # Find the (full-drive) usage of the partition holding the image data
    base_image_path = build_base_image_pathing()
    total_bytes, used_bytes, free_bytes = disk_usage(base_image_path)
    
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
                     "note": "Usage for drive containing images only! May not account for metadata storage",
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
    databases_info_list = MCLIENT.list_databases()
    for each_list_entry in databases_info_list:
        
        # Skip non-camera camera entries
        db_name = each_list_entry.get("name")
        if db_name not in camera_names_list:
            continue
        
        # Get/store sizing info
        size_on_disk_bytes = int(each_list_entry.get("sizeOnDisk", size_if_error))
        size_per_camera_bytes[db_name] = size_on_disk_bytes
    
    # End timing
    t_end = perf_counter()
    
    # Bundle returned data
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    return_result = {"metadata_size_on_disk_bytes": size_per_camera_bytes,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_mongo_connections(request):
    
    ''' Route which returns info regarding the number of connections to mongoDB '''
    
    # For convenience
    connections_err =  {"error": "Couldn't access connections info!"}
    
    # Initialize output
    connections_dict = {}
    
    # Start timing
    t_start = perf_counter()
    
    # Use the 'admin' database to list out the current connections (globally?)
    admin_name = "admin"
    admin_ref = MCLIENT.get_database(admin_name)
    admin_connections = admin_ref.command("serverStatus").get("connections", connections_err)
    connections_dict[admin_name] = admin_connections
    
    # Then get connections per-camera
    camera_names_list = get_camera_names_list(MCLIENT)
    for each_camera_name in camera_names_list:
        camera_db_ref = MCLIENT.get_database(each_camera_name)
        camera_connections = camera_db_ref.command("serverStatus").get("connections", connections_err)
        connections_dict[each_camera_name] = camera_connections
    
    # End timing
    t_end = perf_counter()
    
    # Bundle returned data
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    return_result = {"connections": connections_dict,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_all_indices(request):
    
    ''' Function which lists all indices across all collections/cameras '''
    
    # Loop over every collection of every camera and get index information
    indices_tree = {}
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
            
            # Add the list of (non-id) indexes to the tree
            index_keys_list = list(index_info_dict.keys())
            indices_tree[each_camera_name][each_collection_name] = index_keys_list
    
    return UJSONResponse(indices_tree)

# .....................................................................................................................

def get_document_count_tree(request):
    
    ''' Function which lists all database -> collection -> document counts '''
    
    # Loop over every collection of every camera and count all documents
    doc_count_tree = {}
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
     Route("/get-memory-usage", get_memory_usage),
     Route("/get-disk-usage", get_disk_usage_for_images),
     Route("/get-metadata-usage", get_metadata_bytes_per_camera),
     Route("/get-document-count-tree", get_document_count_tree),
     Route("/get-all-indices", get_all_indices),
     Route("/get-connections-info", get_mongo_connections)
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

