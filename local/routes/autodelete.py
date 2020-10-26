#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  5 10:58:40 2020

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

from local.lib.environment import get_env_upper_max_disk_usage_pct

from local.lib.mongo_helpers import MCLIENT, get_camera_names_list

from local.lib.data_deletion import AD_SETTINGS, get_oldest_snapshot_dt, delete_by_disk_usage, delete_by_days

from local.lib.response_helpers import not_allowed_response

from starlette.responses import UJSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create autodelete routes

# .....................................................................................................................

def autodelete_get_settings(request):
    
    # Read current autodelete settings
    hour_to_run, days_to_keep, max_disk_usage_pct = AD_SETTINGS.get_settings()
    return_response = {"hour_to_run": hour_to_run,
                       "days_to_keep": days_to_keep,
                       "max_disk_usage_pct": max_disk_usage_pct}
    
    return UJSONResponse(return_response)

# .....................................................................................................................

def autodelete_set_max_disk_usage_pct(request):
    
    # Get information from route url
    max_usage_pct = request.path_params["max_usage_pct"]
    
    # Don't allow disk usage above a given system maximum
    system_upper_max_usage_pct = get_env_upper_max_disk_usage_pct()
    if max_usage_pct > system_upper_max_usage_pct:
        error_message = "Not allowed to set max disk usage above {}%".format(system_upper_max_usage_pct)
        return not_allowed_response(error_message)
    
    # Update storage of 'max disk usage' setting
    setting_is_valid = AD_SETTINGS.set_max_disk_usage_pct(max_usage_pct)
    
    # Return confirmation + info about manually forcing deletion if needed
    return_result = {"success": setting_is_valid,
                     "note": ["New setting will not take effect until next scheduled deletion (tomorrow morning)",
                              "- Use the 'delete by disk usage' url to force an immediate deletion if needed"]}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def autodelete_set_days_to_keep(request):
    
    # Get information from route url
    days_to_keep = request.path_params["days_to_keep"]
    
    # Don't allow values less than zero
    if days_to_keep < 1:
        error_message = "Not allowed to set less than 1 day to keep!"
        return not_allowed_response(error_message)
    
    # Update storage of 'days to keep' setting
    setting_is_valid = AD_SETTINGS.set_days_to_keep(days_to_keep)
    
    # Return confirmation + info about manually forcing deletion if needed
    return_result = {"success": setting_is_valid,
                     "note": ["New setting will not take effect until next scheduled deletion (tomorrow morning)",
                              "- Use the 'delete by days to keep' url to force an immediate deletion if needed"]}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def autodelete_manual_delete_by_disk_usage(request):
    
    '''
    Route used to manually trigger a deletion check based on the max disk usage setting
    Most likely to be needed after updating the settings (and wanting immediate deletion update)
    '''
    
    # Start timing
    t_start = perf_counter()
    
    # Get data needed to delete by disk usage
    camera_names_list = camera_names_list = get_camera_names_list(MCLIENT, sort_names = True)
    _, _, max_disk_usage_pct = AD_SETTINGS.get_settings()
    oldest_data_dt, _ = get_oldest_snapshot_dt(camera_names_list)
    
    # Run deletion
    delete_by_disk_usage(MCLIENT, camera_names_list, oldest_data_dt, max_disk_usage_pct)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    return_result = {"time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def autodelete_manual_delete_by_days_to_keep(request):
    
    '''
    Route used to manually trigger a deletion check based on the number of days to keep setting
    Most likely to be needed after updating the settings (and wanting immediate deletion update)
    '''
    
    # Start timing
    t_start = perf_counter()
    
    # Get data needed to delete by days to keep
    camera_names_list = camera_names_list = get_camera_names_list(MCLIENT)
    _, days_to_keep, _ = AD_SETTINGS.get_settings()
    oldest_data_dt, _ = get_oldest_snapshot_dt(camera_names_list)
    
    # Run deletion
    delete_by_days(MCLIENT, camera_names_list, oldest_data_dt, days_to_keep)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    return_result = {"time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_autodeleting_routes():
    
    # Bundle all routes
    route_group = "autodelete"
    url = lambda *url_components: "/".join(["/{}".format(route_group), *url_components])
    autodelete_routes = \
    [
     Route(url("get-settings"),
           autodelete_get_settings),
     
     Route(url("set", "max-disk-usage-percent", "{max_usage_pct:int}"),
           autodelete_set_max_disk_usage_pct),
     
     Route(url("set", "max-days-to-keep", "{days_to_keep:float}"),
           autodelete_set_days_to_keep),
     
     Route(url("manual", "delete", "by-disk-usage"),
           autodelete_manual_delete_by_disk_usage),
     
     Route(url("manual", "delete", "by-days-to-keep"),
           autodelete_manual_delete_by_days_to_keep)
    ]
    
    return autodelete_routes

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



