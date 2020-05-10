#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 15:54:43 2020

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

from local.lib.environment import get_env_images_folder
from local.lib.timekeeper_utils import epoch_ms_to_image_folder_names, image_folder_names_to_epoch_ms


# ---------------------------------------------------------------------------------------------------------------------
#%% Define classes

# .....................................................................................................................

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Define functions

# .....................................................................................................................

def build_base_image_pathing(*, error_if_using_dropbox = True):
    
    ''' Helper function which generates the base folder pathing for storing images persistently '''
    
    # Check if an image folder path was provided in environment variables
    env_image_folder_path = get_env_images_folder()
    base_image_folder = env_image_folder_path
    
    # If there is no path provided through the environment, just store things in the same folder as this script 
    if env_image_folder_path is None:
        this_script_folder_path = os.path.dirname(os.path.abspath(__file__))
        project_root_folder = os.path.dirname(os.path.dirname(this_script_folder_path))
        base_image_folder = os.path.join(project_root_folder, "images_dbserver")
    
    # Avoid syncing 'persistent' data
    dropbox_in_path = ("dropbox" in base_image_folder.lower())
    if dropbox_in_path and error_if_using_dropbox:
        raise EnvironmentError("Can't run dbserver from a dropbox folder!\n @ {}".format(__file__))
    
    # Make sure the folder path exists!
    os.makedirs(base_image_folder, exist_ok = True)
    
    return base_image_folder

# .....................................................................................................................

def build_camera_image_path(base_image_path, camera_select, *path_joins):
    return os.path.join(base_image_path, camera_select, *path_joins)

# .....................................................................................................................

def build_image_pathing(base_image_folder_path, camera_select, image_folder_type, epoch_ms, 
                        create_folder_if_missing = False):
    
    ''' Function which generates local folder pathing to save uploaded images '''
    
    # Figure out the folder pathing for the given timestamp
    date_folder_name, hour_folder_name = epoch_ms_to_image_folder_names(epoch_ms)
    
    # Build target file name folder pathing
    image_file_name = "{}.jpg".format(epoch_ms)
    image_folder_path = build_camera_image_path(base_image_folder_path, camera_select,
                                                image_folder_type, date_folder_name, hour_folder_name)
    image_file_path = os.path.join(image_folder_path, image_file_name)
    
    # Create the folder path if needed
    if create_folder_if_missing:
        os.makedirs(image_folder_path, exist_ok = True)
    
    return image_file_path

# .....................................................................................................................

def get_old_image_folders_list(base_image_folder_path, camera_select, image_folder_type, oldest_allowed_ems):
    
    ''' Helper function which provides pathing to all image date folders (likely used for deletion!) '''
    
    # Get list of all date folders for the provided camera & image type
    image_type_folder_path = build_camera_image_path(base_image_folder_path, camera_select, image_folder_type)
    
    # Get list of all date folders
    date_folder_names_list = os.listdir(image_type_folder_path)
    
    # Go through all hour folders (for all dates)
    old_image_folders_path = []
    for each_date_name in date_folder_names_list:
        
        # Build pathing to the parent date folder
        date_folder_path = os.path.join(image_type_folder_path, each_date_name)
        
        # Return the parent date folder path if the beginning of the day would be too old (and skip hours)
        beginning_of_day_ems, _ = image_folder_names_to_epoch_ms(each_date_name)
        if beginning_of_day_ems < oldest_allowed_ems:
            old_image_folders_path.append(date_folder_path)
            continue
        
        # Loop over every hour folder and store it's path if it's 'too old'
        hour_folder_names_list = os.listdir(date_folder_path)
        for each_hour_name in hour_folder_names_list:
            
            # Consider an hour folder 'too old' if the end of the hour is older than the oldest allowed time
            # (as opposed to using the start of the hour, since some files within may be new enough)
            _, end_of_hour_ems = image_folder_names_to_epoch_ms(each_date_name, each_hour_name)
            if end_of_hour_ems < oldest_allowed_ems:
                hour_folder_path = os.path.join(date_folder_path, each_hour_name)
                old_image_folders_path.append(hour_folder_path)
    
    return old_image_folders_path

# .....................................................................................................................

def get_old_snapshot_image_folders_list(base_image_folder_path, camera_select, oldest_allowed_ems):
    
    '''
    Helper function which hard-codes the 'snapshots' image folder type,
    to avoid callers having to know the correct string to use
    '''
    
    return get_old_image_folders_list(base_image_folder_path, camera_select, "snapshots", oldest_allowed_ems)

# .....................................................................................................................

def get_old_background_image_folders_list(base_image_folder_path, camera_select, oldest_allowed_ems):
    
    '''
    Helper function which hard-codes the 'backgrounds' image folder type,
    to avoid callers having to know the correct string to use
    '''
    
    return get_old_image_folders_list(base_image_folder_path, camera_select, "backgrounds", oldest_allowed_ems)

# .....................................................................................................................
# .....................................................................................................................
    
# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    # Create pathing examples
    base_image_path = build_base_image_pathing(error_if_using_dropbox = False)
    example_image_path = build_image_pathing(base_image_path, "example_camera", "snapshots", 12345, 
                                             create_folder_if_missing = False)
    
    # Print out example pathing for quick checks
    print("", "Image base folder:", base_image_path, sep = "\n")
    print("", "Example image path:", example_image_path, sep = "\n")


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap



