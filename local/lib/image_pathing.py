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
from local.lib.timekeeper_utils import epoch_ms_to_utc_datetime


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
        raise EnvironmentError("Can't run dbserver from a dropbox folder!")
    
    # Make sure the folder path exists!
    os.makedirs(base_image_folder, exist_ok = True)
    
    return base_image_folder

# .....................................................................................................................

def build_image_pathing(base_image_folder_path, camera_select, image_folder_type, epoch_ms, 
                        create_folder_if_missing = False):
    
    ''' Function which generates local folder pathing to save uploaded images '''
    
    # Figure out the folder pathing for the given timestamp
    target_time_dt = epoch_ms_to_utc_datetime(epoch_ms)
    target_date_str = target_time_dt.strftime("%Y-%m-%d")
    target_hour_str = target_time_dt.strftime("%H")
    
    # Build target file name folder pathing
    image_file_name = "{}.jpg".format(epoch_ms)
    image_folder_path = os.path.join(base_image_folder_path, camera_select,
                                     image_folder_type, target_date_str, target_hour_str)
    image_file_path = os.path.join(image_folder_path, image_file_name)
    
    # Create the folder path if needed
    if create_folder_if_missing:
        os.makedirs(image_folder_path, exist_ok = True)
    
    return image_file_path

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



