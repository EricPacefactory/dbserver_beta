#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 16:28:28 2020

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

from shutil import move as sh_move
from shutil import rmtree as sh_remove_recursively

from local.lib.mongo_helpers import MCLIENT, get_camera_names_list, get_collection_names_list, remove_camera_collection
from local.lib.response_helpers import no_data_response

from starlette.responses import JSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

# .....................................................................................................................

def move_folders_recursively(source_folder_path, destination_folder_path,
                             debug_mode = False, _indent = ""):
    
    ''' Wrapper around using shutil.move function, but works in cases where original folder already exists! '''
    
    # Helper functions used to enable a debug mode (flips between moving files or just printing move result)
    debug_print = lambda src, dst: print("{}Move: {}  ->  {}".format(_indent, os.path.basename(src), dst))
    move_func = lambda src, dst: debug_print(src, dst) if debug_mode else sh_move(src, dst)
    
    #  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .
    
    # Bail if the source folder doesn't exist to begin with!
    source_doesnt_exist = (not os.path.exists(source_folder_path))
    if source_doesnt_exist:
        return None
    
    # Handle case where the source folder is actually a file (by operating on the parent folder instead)
    source_is_file = os.path.isfile(source_folder_path)
    if source_is_file:
        source_folder_path = os.path.dirname(source_folder_path)
    
    #  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .
    
    # First check if we can get away with regular move function
    source_folder_name = os.path.basename(source_folder_path)
    target_move_folder_path = os.path.join(destination_folder_path, source_folder_name)
    can_move_direct = (not os.path.exists(target_move_folder_path))
    if can_move_direct:
        move_func(source_folder_path, destination_folder_path)
        return target_move_folder_path
    
    # If we get here, the destination already exists, so provide some feedback in debug mode
    if debug_mode:
        print("{}Exists: {}".format(_indent, source_folder_name))
    
    #  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .
    
    # Get a list of all of the contents of the source folder, so we can try moving those instead
    child_contents_list = os.listdir(source_folder_path)
    
    # Separate the contents into files & folder, since these need to be treated differently
    child_file_list = []
    child_folder_list = []
    for each_entry in child_contents_list:
        entry_path = os.path.join(source_folder_path, each_entry)
        is_folder = os.path.isdir(entry_path)
        if is_folder:
            child_folder_list.append(each_entry)
        else:
            child_file_list.append(each_entry)
    
    #  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .
    
    # Move all child files directly (assuming they don't already exist!)
    num_child_files = len(child_file_list)
    num_child_files_moved = 0
    for each_child_file in child_file_list:
        
        # Build source/destination pathing for each child file
        child_file_src_path = os.path.join(source_folder_path, each_child_file)
        child_file_dst_path = os.path.join(target_move_folder_path, each_child_file)
        
        # Don't bother moving files that already exist at the destination
        file_already_exists = os.path.exists(child_file_dst_path)
        if file_already_exists:
            continue
        
        # If we get here, we can move the file!
        move_func(child_file_src_path, child_file_dst_path)
        num_child_files_moved += 1
    
    # Some feedback in debug mode
    if debug_mode and (num_child_files > 0):
        print("  {}Move: {} / {} files".format(_indent, num_child_files_moved, num_child_files))
    
    #  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .
    
    # Recursively try to move all subdirectories
    for each_child_folder in child_folder_list:
        
        # Build source/destination pathing for each child folder
        child_folder_src_path = os.path.join(source_folder_path, each_child_folder)
        child_folder_dst_path = os.path.join(destination_folder_path, source_folder_name)
        
        # Recursively apply this same function to move remaining files!
        move_folders_recursively(child_folder_src_path, child_folder_dst_path, debug_mode,
                                 _indent = "  {}".format(_indent))
    
    #  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .
    
    # Finally, if we get here, we need to remove the original source folder (and all contents)
    # since it wouldn't have been moved if the destination already existed!
    if not debug_mode:
        sh_remove_recursively(source_folder_path, ignore_errors = True)
    
    return target_move_folder_path

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Create camera info routes

# .....................................................................................................................

def info_route(request):
    
    '''
    Route which returns info about the forward compatibility routes
    '''
    
    info_msg = \
    {"info": ["Set of routes used to manually apply updates to the dbserver,",
              "for the sake of forward-compatibility. This can include changes",
              "to the underlying filesystem, for example, or changes to how",
              "data is organized on the database",
              "Ideally, avoids the need for in-system manual changes when possible...",
              "Note that these routes should only be called once in order to get older",
              "versions update-to-date. Repeat calls should hopefully be harmless",
              "but there are no guarantees! (depends on what updates are needed)"],
     "updates": {
                 "oct_26_2020": ["- changes to filesystem!",
                                 "- moves camera image data a new location"],
                 "oct_28_2020": ["- changes to database routes & stored data",
                                 "- removes serverlogs-* collection data for all cameras",
                                 "- serverlogs routing was also removed as part of this update"]
                }
    }
    
    return JSONResponse(info_msg)

# .....................................................................................................................

def oct_26_2020_route(request):
    
    '''
    Route which performs update based on changes to file-system from October 26, 2020 update
    Main change is a restructuring of the persistent data storage
    - Main docker files (i.e. code) is now stored under:
        ~/from_docker   (previously ~/dbserver)
    
    - Persistent data within docker container is now stored under:
        ~/volume        (previously ~/images_dbserver)
    
    - Persistent data is now structured differently, to separate code, system data & camera data
        - code (under ~/volume/code) is for a future update, so that code can be updated independent of docker image
        - data (under ~/volume/data) is now used to store all persistent (non-code) data
    
    - In addition to new folder pathing, data is now structured with better granularity
      Uses following structure:
        ~/volume/data
            -> /cameras
                -> /(camera name 1)
                -> /(camera name 2)
                -> etc...
            -> /system
                -> /configs
                    -> ...
                -> /logs
                    -> autodelete
                        -> ...
                    -> ...
    
    Goal of this route is to move existing camera image data from '~/images_dbserver/(camera names...)'
    into the newer volume pathing: ~/volume/data/cameras/(camera names...)
    '''
    
    # Set up pathing to old data
    # -> Note that due to volume mapping, old camera folders will appear in new 'volume' folder
    # -> Also, the new 'data' & 'code' folders will exist alongside the old camera folders and should not be moved!
    home_folder_path = os.path.expanduser("~")
    new_volume_path = os.path.join(home_folder_path, "volume")
    
    # Bail if we don't find the new volume path
    no_volume_folder_found = (not os.path.exists(new_volume_path))
    if no_volume_folder_found:
        error_message = "Couldn't find new volume path: {}".format(new_volume_path)
        return no_data_response(error_message)
    
    # Get list of 'files' (and folders) in the volume folder, then exclude the new data/code folders
    volume_entry_list = os.listdir(new_volume_path)
    camera_entry_list = [each_entry for each_entry in volume_entry_list if each_entry not in ("data", "code")]
    
    # Build paths to each file listing, and then remove non-folder entries (not expecting anything but folders)
    camera_entry_paths_list = [os.path.join(new_volume_path, each_file) for each_file in camera_entry_list]
    camera_folder_paths_list = [each_path for each_path in camera_entry_paths_list if os.path.isdir(each_path)]
    
    # Bail if there are no cameras listed (may have already performed update!)
    no_cameras_to_move = (len(camera_folder_paths_list) == 0)
    if no_cameras_to_move:
        warning_message = {"warning": "No camera folders found to move! Update may already be complete"}
        return JSONResponse(warning_message)
    
    try:
        
        # If we get here, move the camera folders!
        new_cameras_volume_path = os.path.join(new_volume_path, "data", "cameras")
        move_results_list = []
        for each_camera_folder_path in camera_folder_paths_list:
            each_move_result_path = move_folders_recursively(each_camera_folder_path, new_cameras_volume_path)
            move_results_list.append(each_move_result_path)
        
        # Construct return response
        return_result = {"success": True, "moved_paths_list": move_results_list}
        
    except Exception as err:
        return_result = {"success": False, "error": str(err)}
    
    return JSONResponse(return_result)

# .....................................................................................................................

def oct_28_2020_route(request):
    
    '''
    Route which removes 'serverlogs' entries from the database
    These were originally intended to help store logging info about server-side operations
    but were only ever used by the deletion functions (which got a different form of logging as of oct26)
    There is/was no automatic maintenance performed on the serverlogs, so they can grow/get messy over time,
    and without anyone actively using them, they may as well be deleted!
    '''
    
    # For clarity
    # --> All server log collections contained the following string to identify them, so we'll use it to remove them
    target_prefix = "serverlogs-"
    
    # Get all cameras
    camera_names_list = get_camera_names_list(MCLIENT)
    
    # Get all collection names, so we can check if there are any serverlogs to remove
    collections_removed_dict = {}
    for each_camera_name in camera_names_list:
        
        # Get all collections for the given camera and remove any with the target (server log) prefix
        camera_collection_names_list = get_collection_names_list(MCLIENT, each_camera_name)
        collections_removed_list = []
        for each_collection_name in camera_collection_names_list:
            is_serverlog_collection = each_collection_name.startswith(target_prefix)
            if is_serverlog_collection:
                remove_camera_collection(MCLIENT, each_camera_name, each_collection_name)
                collections_removed_list.append(each_collection_name)
        
        # Store all removed collections for reporting
        collections_removed_dict[each_camera_name] = collections_removed_list
    
    # Bundle results for feedback
    return_result = {"success": True, "collections_removed": collections_removed_dict}
    
    return JSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_compatibility_routes():
    
    # Bundle all camera info routes
    url = lambda *url_components: "/".join(["/compatibility", *url_components])
    compatibility_routes = \
    [
     Route(url("info"), info_route),
     Route(url("oct_26_2020"), oct_26_2020_route),
     Route(url("oct_28_2020"), oct_28_2020_route)
    ]
    
    return compatibility_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Nothing!


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap

