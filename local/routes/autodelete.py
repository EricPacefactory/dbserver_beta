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
from collections import OrderedDict

from local.lib.timekeeper_utils import timestamped_log

from local.lib.mongo_helpers import MCLIENT, get_camera_names_list

from local.lib.data_deletion import AD_SETTINGS, build_autodelete_log_folder_path
from local.lib.data_deletion import get_oldest_snapshot_dt, delete_by_disk_usage, delete_by_days, get_disk_usage

from local.lib.response_helpers import not_allowed_response

from starlette.responses import UJSONResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create autodelete routes

# .....................................................................................................................

def autodelete_check_system_logs(request):
    
    ''' Route used to present system logs (from dbserver filesystem) on browser for remote viewing '''
    
    # Get pathing to system logs
    logging_folder_path = build_autodelete_log_folder_path()
    autodelete_log_files_list = sorted(os.listdir(logging_folder_path))
    log_file_paths_list = [os.path.join(logging_folder_path, each_file) for each_file in autodelete_log_files_list]
    
    # Go through each log file and try to retrieve the contents
    log_files_dict = OrderedDict()
    for each_log_file_path in log_file_paths_list:
        
        try:
            # Read the contents of each log file we found
            with open(each_log_file_path, "r") as in_file:
                full_log_file_str = in_file.read()
                
        except Exception:
            full_log_file_str = "error reading log file!"
        
        # Store log file results, line-by-line, in a JSON-friendly format, indexed by the log file name
        each_log_file_name, _ = os.path.splitext(os.path.basename(each_log_file_path))
        log_files_dict[each_log_file_name] = full_log_file_str.splitlines()
    
    return UJSONResponse(log_files_dict)

# .....................................................................................................................

def autodelete_get_settings(request):
    
    ''' Route used to check current autodelete settings '''
    
    # Get the hour to run, just out of interest
    hour_to_run = AD_SETTINGS.get_hour_to_run()
    
    # Read current autodelete settings
    days_to_keep, max_disk_usage_pct = AD_SETTINGS.get_settings()
    return_response = {"hour_to_run": hour_to_run,
                       "days_to_keep": days_to_keep,
                       "max_disk_usage_pct": max_disk_usage_pct}
    
    return UJSONResponse(return_response)

# .....................................................................................................................

def autodelete_set_max_disk_usage_pct(request):
    
    ''' Route used to alter the autodelete max disk usage percentage setting '''
    
    # Get information from route url
    max_usage_pct = request.path_params["max_usage_pct"]
    
    # Don't allow disk usage above a given system maximum
    system_upper_max_usage_pct = AD_SETTINGS.get_upper_max_disk_usage_percent()
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
    
    ''' Route used to alter the autodelete 'days to keep' setting '''
    
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
    
    # Some feedback, mostly for docker logs
    delete_msg = timestamped_log("Manual 'delete-by-disk-usage'")
    print("", delete_msg, sep = "\n", flush = True)
    
    # Start timing
    t_start = perf_counter()
    
    # Get disk usage before deletion
    _, used_bytes_before, _, _ = get_disk_usage()
    
    # Get data needed to delete by disk usage
    camera_names_list = get_camera_names_list(MCLIENT, sort_names = True)
    _, max_disk_usage_pct = AD_SETTINGS.get_settings()
    oldest_data_dt, _ = get_oldest_snapshot_dt(camera_names_list)
    
    # Run deletion
    delete_by_disk_usage(MCLIENT, camera_names_list, oldest_data_dt, max_disk_usage_pct)
    
    # Get disk usage after deletion for comparison
    _, used_bytes_after, _, current_disk_usage_pct = get_disk_usage()
    bytes_deleted = max(0, (used_bytes_before - used_bytes_after))
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    return_result = {"used_bytes_before": used_bytes_before,
                     "used_bytes_after": used_bytes_after,
                     "bytes_deleted": bytes_deleted,
                     "percent_usage": current_disk_usage_pct,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def autodelete_manual_delete_by_days_to_keep(request):
    
    '''
    Route used to manually trigger a deletion check based on the number of days to keep setting
    Most likely to be needed after updating the settings (and wanting immediate deletion update)
    '''
    
    # Some feedback, mostly for docker logs
    delete_msg = timestamped_log("Manual 'delete-by-days-to-keep'")
    print("", delete_msg, sep = "\n", flush = True)
    
    # Start timing
    t_start = perf_counter()
    
    # Get disk usage before deletion
    _, used_bytes_before, _, _ = get_disk_usage()
    
    # Get data needed to delete by days to keep
    camera_names_list = camera_names_list = get_camera_names_list(MCLIENT)
    days_to_keep, _ = AD_SETTINGS.get_settings()
    oldest_data_dt, _ = get_oldest_snapshot_dt(camera_names_list)
    
    # Run deletion
    delete_by_days(MCLIENT, camera_names_list, oldest_data_dt, days_to_keep)
    
    # Get disk usage after deletion for comparison
    _, used_bytes_after, _, current_disk_usage_pct = get_disk_usage()
    bytes_deleted = max(0, (used_bytes_before - used_bytes_after))
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    return_result = {"used_bytes_before": used_bytes_before,
                     "used_bytes_after": used_bytes_after,
                     "bytes_deleted": bytes_deleted,
                     "percent_usage": current_disk_usage_pct,
                     "time_taken_ms": time_taken_ms}
    
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
     Route(url("check-system-logs"),
           autodelete_check_system_logs),
     
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



