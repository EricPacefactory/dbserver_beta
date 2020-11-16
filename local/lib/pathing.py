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

from local.lib.environment import get_env_data_folder
from local.lib.timekeeper_utils import epoch_ms_to_image_folder_names, image_folder_names_to_epoch_ms

from local.eolib.utils.use_git import Git_Reader


# ---------------------------------------------------------------------------------------------------------------------
#%% Define classes

# .....................................................................................................................

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define shared functions

# .....................................................................................................................

def build_base_data_folder_path(*, error_if_using_dropbox = True):
    
    ''' Helper function which generates the folder pathing for storing persistent data '''
    
    # Check if a data folder is defined in the environment (which should occur on proper installs/docker)
    data_folder_path = get_env_data_folder()
    
    # If there is no path provided through the environment, just store things in the same folder as this script 
    if data_folder_path is None:
        this_script_folder_path = os.path.dirname(os.path.abspath(__file__))
        project_root_folder = os.path.dirname(os.path.dirname(this_script_folder_path))
        data_folder_path = os.path.join(project_root_folder, "volume", "data")
    
    # Avoid syncing 'persistent' data
    dropbox_in_path = ("dropbox" in data_folder_path.lower())
    if dropbox_in_path and error_if_using_dropbox:
        raise EnvironmentError("Can't run dbserver from a dropbox folder!\n @ {}".format(data_folder_path))
    
    # Make sure the folder exists
    os.makedirs(data_folder_path, exist_ok = True)
    
    return data_folder_path

# .....................................................................................................................

def build_system_data_folder_path(base_data_folder_path, *path_joins):
    ''' Function which returns paths for storing system-related persistent data (e.g configs) '''
    return os.path.join(base_data_folder_path, "system", *path_joins)

# .....................................................................................................................

def build_cameras_data_folder_path(base_data_folder_path, *path_joins):
    ''' Function which returns paths for storing camera-related persistent data (e.g. images) '''
    return os.path.join(base_data_folder_path, "cameras", *path_joins)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define secondary shared pathing functions

# .....................................................................................................................

def build_system_configs_folder_path(base_data_folder_path, *path_joins):
    ''' Function which returns paths for storing (persistent) system configs '''
    return build_system_data_folder_path(base_data_folder_path, "configs", *path_joins)

# .....................................................................................................................

def build_system_logs_folder_path(base_data_folder_path, *path_joins):
    ''' Function which returns paths for storing system logging data '''
    return build_system_data_folder_path(base_data_folder_path, "logs", *path_joins)

# .....................................................................................................................

def build_camera_data_path(base_data_folder_path, camera_select, *path_joins):
    ''' Function which returns paths for storing (file system) data per-camera '''
    return build_cameras_data_folder_path(base_data_folder_path, camera_select, *path_joins)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define image pathing functions

# .....................................................................................................................

def build_image_pathing(base_data_folder_path, camera_select, image_folder_type, epoch_ms, 
                        create_folder_if_missing = False):
    
    ''' Function which generates local folder pathing to save uploaded images '''
    
    # Figure out the folder pathing for the given timestamp
    date_folder_name, hour_folder_name = epoch_ms_to_image_folder_names(epoch_ms)
    
    # Build target file name folder pathing
    image_file_name = "{}.jpg".format(epoch_ms)
    image_folder_path = build_camera_data_path(base_data_folder_path, camera_select, 
                                               image_folder_type, date_folder_name, hour_folder_name)
    image_file_path = os.path.join(image_folder_path, image_file_name)
    
    # Create the folder path if needed
    if create_folder_if_missing:
        os.makedirs(image_folder_path, exist_ok = True)
    
    return image_file_path

# .....................................................................................................................

def build_snapshot_image_pathing(base_data_folder_path, camera_select, epoch_ms, create_folder_if_missing = False):
    return build_image_pathing(base_data_folder_path, camera_select, "snapshots", epoch_ms, create_folder_if_missing)

# .....................................................................................................................

def build_background_image_pathing(base_data_folder_path, camera_select, epoch_ms, create_folder_if_missing = False):
    return build_image_pathing(base_data_folder_path, camera_select, "backgrounds", epoch_ms, create_folder_if_missing)

# .....................................................................................................................

def get_old_image_folders_list(base_data_folder_path, camera_select, image_folder_type, oldest_allowed_ems):
    
    ''' Helper function which provides pathing to all image date folders (likely used for deletion!) '''
    
    # Get list of all date folders for the provided camera & image type
    image_type_folder_path = build_camera_data_path(base_data_folder_path, camera_select, image_folder_type)
    
    # Get list of all date folders
    date_folder_names_list = os.listdir(image_type_folder_path)
    
    # Go through all hour folders (for all dates)
    old_image_folders_path = []
    for each_date_name in date_folder_names_list:
        
        # Build pathing to the parent date folder
        date_folder_path = os.path.join(image_type_folder_path, each_date_name)
        
        # Return the parent date folder path if the end of the day would be too old
        # (since all hours of the same day would also be too old!)
        _, end_of_day_ems = image_folder_names_to_epoch_ms(each_date_name, hour_folder_name = "23")
        if end_of_day_ems < oldest_allowed_ems:
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

def get_old_snapshot_image_folders_list(base_data_folder_path, camera_select, oldest_allowed_ems):
    
    '''
    Helper function which hard-codes the 'snapshots' image folder type,
    to avoid callers having to know the correct string to use
    '''
    
    return get_old_image_folders_list(base_data_folder_path, camera_select, "snapshots", oldest_allowed_ems)

# .....................................................................................................................

def get_old_background_image_folders_list(base_data_folder_path, camera_select, oldest_allowed_ems):
    
    '''
    Helper function which hard-codes the 'backgrounds' image folder type,
    to avoid callers having to know the correct string to use
    '''
    
    return get_old_image_folders_list(base_data_folder_path, camera_select, "backgrounds", oldest_allowed_ems)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define globals

# Create pathing to the base data folder path, which is shared in all other pathing
BASE_DATA_FOLDER_PATH = build_base_data_folder_path(error_if_using_dropbox = True)

# Create pathing to commonly used (base) persistent folders
BASE_SYSTEM_CONFIGS_FOLDER_PATH = build_system_configs_folder_path(BASE_DATA_FOLDER_PATH)
BASE_SYSTEM_LOGS_FOLDER_PATH = build_system_logs_folder_path(BASE_DATA_FOLDER_PATH)
BASE_CAMERAS_FOLDER_PATH = build_cameras_data_folder_path(BASE_DATA_FOLDER_PATH)

# Make sure all the important base folder pathing is created
os.makedirs(BASE_DATA_FOLDER_PATH, exist_ok = True)
os.makedirs(BASE_SYSTEM_CONFIGS_FOLDER_PATH, exist_ok = True)
os.makedirs(BASE_SYSTEM_LOGS_FOLDER_PATH, exist_ok = True)
os.makedirs(BASE_CAMERAS_FOLDER_PATH, exist_ok = True)

# Set up git repo access
GIT_READER = Git_Reader(None)


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    # Create pathing examples
    example_image_path = build_image_pathing(BASE_CAMERAS_FOLDER_PATH, "example_camera", "snapshots", 12345, 
                                             create_folder_if_missing = False)
    
    # Print out example pathing for quick checks
    print("", "Data folder path:", BASE_DATA_FOLDER_PATH, sep = "\n")
    print("", "Cameras base folder:", BASE_CAMERAS_FOLDER_PATH, sep = "\n")
    print("", "Example image path:", example_image_path, sep = "\n")


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap



