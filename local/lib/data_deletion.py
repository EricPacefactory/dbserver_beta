#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 15:21:41 2020

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

import json

from multiprocessing import Process, Event
from time import perf_counter
from shutil import rmtree, disk_usage

from random import random as unit_random

from local.lib.pathing import BASE_DATA_FOLDER_PATH, build_camera_data_path
from local.lib.pathing import build_system_configs_folder_path, build_system_logs_folder_path
from local.lib.pathing import get_old_snapshot_image_folders_list, get_old_background_image_folders_list

from local.lib.environment import get_env_upper_max_disk_usage_pct, get_env_hour_to_run
from local.lib.environment import get_default_max_disk_usage_pct, get_default_days_to_keep

from local.lib.mongo_helpers import connect_to_mongo, get_camera_names_list, remove_camera_entry
from local.lib.query_helpers import get_closest_metadata_before_target_ems, get_oldest_metadata

from local.lib.timekeeper_utils import datetime_to_epoch_ms, datetime_convert_to_day_start, epoch_ms_to_local_datetime
from local.lib.timekeeper_utils import get_local_datetime, get_local_datetime_tomorrow, get_local_datetime_in_past
from local.lib.timekeeper_utils import get_seconds_between_datetimes, add_to_datetime

from local.routes.camerainfo import get_camera_info_collection
from local.routes.configinfo import get_config_info_collection
from local.routes.backgrounds import get_background_collection
from local.routes.snapshots import get_snapshot_collection
from local.routes.objects import get_object_collection
from local.routes.stations import get_station_collection

from local.routes.objects import FINAL_EPOCH_MS_FIELD as OBJ_FINAL_EMS_FIELD
from local.routes.stations import FINAL_EPOCH_MS_FIELD as STN_FINAL_EMS_FIELD

from local.eolib.utils.logging import Daily_Logger


# ---------------------------------------------------------------------------------------------------------------------
#%% Define classes

class Autodelete_Settings:
    
    # .................................................................................................................
    
    def __init__(self):
        
        '''
        Class used to control access to autodelete settings, which are saved directly on the file-system
        Note that this class does not directly store any settings, instead it loads/saves
        the settings through the filesystem, as needed
        '''
        
        # Build path to the autodelete settings file
        self.settings_file_path = build_system_configs_folder_path(BASE_DATA_FOLDER_PATH, "autodelete_settings.json")
        
        # Hard-code keys used to look-up settings
        self.days_to_keep_key = "days_to_keep"
        self.max_disk_usage_key = "max_disk_usage_pct"
    
    # .................................................................................................................
    
    def _load_settings_file(self):
        
        ''' Helper function which loads settings file data, with environment defaults if data is missing '''
        
        # First get default values, in case values are missing
        default_days_to_keep = get_default_days_to_keep()
        default_max_disk_usage_pct = get_default_max_disk_usage_pct()
        
        # Set up default settings, in case loading fails
        settings_dict = {self.days_to_keep_key: default_days_to_keep,
                         self.max_disk_usage_key: default_max_disk_usage_pct}
        
        # Create the settings file, if it doesn't already exist
        settings_file_exists = os.path.exists(self.settings_file_path)
        if not settings_file_exists:
            save_config_json(self.settings_file_path, settings_dict)
        
        # Load existing settings and use them to update the default configs
        load_success, loaded_settings_dict = load_config_json(self.settings_file_path)
        if load_success:
            settings_dict.update(loaded_settings_dict)
        
        return settings_dict
    
    # .................................................................................................................
    
    def _update_and_save_settings_file(self, *,
                                       new_days_to_keep = None,
                                       new_max_disk_usage_pct = None):
        
        ''' Helper function which loads settings file data '''
        
        # Load existing settings
        settings_dict = self._load_settings_file()
        
        # Create update dictionary
        if new_days_to_keep is not None:
            settings_dict[self.days_to_keep_key] = new_days_to_keep        
        if new_max_disk_usage_pct is not None:
            settings_dict[self.max_disk_usage_key] = new_max_disk_usage_pct
        
        # Save new settings
        save_config_json(self.settings_file_path, settings_dict)
        
        return settings_dict
    
    # .................................................................................................................
    
    def get_settings(self):
        
        # First get default values, in case values are missing
        default_days_to_keep = get_default_days_to_keep()
        default_max_disk_usage_pct = get_default_max_disk_usage_pct()
        
        # Load settings
        settings_dict = self._load_settings_file()
        
        # Grab settings (or use defaults)
        days_to_keep = settings_dict.get(self.days_to_keep_key, default_days_to_keep)
        max_disk_usage_pct = settings_dict.get(self.max_disk_usage_key, default_max_disk_usage_pct)
        
        # Also grab the hour to run from the environment, for reporting purposes
        hour_to_run = get_env_hour_to_run()
        
        return hour_to_run, days_to_keep, max_disk_usage_pct
    
    # .................................................................................................................
    
    def set_days_to_keep(self, new_days_to_keep):
        
        # Make sure new value is valid
        is_valid = (new_days_to_keep > 0)
        
        # Save new settings if valid
        if is_valid:
            self._update_and_save_settings_file(new_days_to_keep = new_days_to_keep)
        
        return is_valid
    
    # .................................................................................................................
    
    def set_max_disk_usage_pct(self, new_max_disk_usage_pct):
        
        # Make sure new value is valid
        upper_allowable_usage_pct = get_env_upper_max_disk_usage_pct()
        is_valid = (1 < new_max_disk_usage_pct < upper_allowable_usage_pct)
        
        # Save new settings if valid
        if is_valid:
            self._update_and_save_settings_file(new_max_disk_usage_pct = new_max_disk_usage_pct)
        
        return is_valid
    
    # .................................................................................................................
    # .................................................................................................................

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

# .....................................................................................................................

def delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field):
    
    ''' Helper function which deletes all documents (from MongoDB) older than a provided cutoff epoch ms value '''
    
    # Send deletion command to the db
    filter_dict = {epoch_ms_field: {"$lt": oldest_allowed_ems}}
    pymongo_DeleteResult = collection_ref.delete_many(filter_dict)
    
    # Get the number of deleted documents from the response (if possible!)
    num_deleted = pymongo_DeleteResult.deleted_count
    
    return num_deleted

# .....................................................................................................................
    
def load_config_json(load_path):
    
    '''
    Helper function for loading json config data
    Returns:
        load_success, config_dict
    '''
    
    # Initialize outputs
    load_success = False
    config_dict = {}
    
    # Load the file, with some error handling
    try:
        with open(load_path, "r") as in_file:
            config_dict = json.load(in_file)
        load_success = True
        
    except FileNotFoundError:
        # Happens if the load path is not good
        pass
    
    except json.decoder.JSONDecodeError:
        # Happens if there is an error with the JSON document formatting (usually a missing comma or something)
        pass
    
    return load_success, config_dict

# .....................................................................................................................
    
def save_config_json(save_path, config_dict):
    
    '''
    Helper function for saving json config data
    Returns nothing!
    '''
    
    # Create the save path, in case it doesn't already exist
    save_folder_path = os.path.dirname(save_path)
    os.makedirs(save_folder_path, exist_ok = True)
    
    # Save the file, with 'nice' formatting
    with open(save_path, "w") as out_file:
        json.dump(config_dict, out_file, sort_keys = True, indent = 2)
    
    return

# .....................................................................................................................

def create_autodelete_logger(enabled = True):
    
    ''' Helper function to create a logger for autodeletion results '''
    
    logging_folder_path = build_system_logs_folder_path(BASE_DATA_FOLDER_PATH, "autodelete")
    logger = Daily_Logger(logging_folder_path, log_files_to_keep = 10, enabled = enabled, include_timestamp = True)
    
    return logger

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Collection deletion function

# .....................................................................................................................

def get_oldest_snapshot_dt(camera_names_list):
    
    '''
    Function which returns the oldest snapshot datetime from a list of available cameras
    Intended to be used to determine a 'starting point' to begin deleting data from
    '''
    
    # Get the current time to use as a default timing in case something goes wrong...
    current_dt = get_local_datetime()
    current_ems = datetime_to_epoch_ms(current_dt)
    
    # Loop over all camera to get oldest snapshot timing info
    empty_camera_names_list = []
    oldest_snap_ems_list = [current_ems]    
    for each_camera_name in camera_names_list:
        
        # Get oldest snapshot timing for each camera, so can check which is the oldest overall
        each_collection_ref = get_snapshot_collection(each_camera_name)
        no_oldest_metadata, oldest_metadata_dict = get_oldest_metadata(each_collection_ref, DEFAULT_EMS_FIELD)
        
        # Keep track of cameras that don't have old snapshot data (should be deleted!)
        if no_oldest_metadata:
            empty_camera_names_list.append(each_camera_name)
        
        # Try to retrieve the oldest epoch ms value, otherwise use the current value
        # --> will happen in 'no metadata case' for example
        each_old_snap_ems = oldest_metadata_dict.get("epoch_ms", current_ems)
        oldest_snap_ems_list.append(each_old_snap_ems)
    
    # Figure out the oldest snapshot timing and convert to a datetime for output
    oldest_snap_ems = min(oldest_snap_ems_list)
    oldest_snap_dt = epoch_ms_to_local_datetime(oldest_snap_ems)
    
    return oldest_snap_dt, empty_camera_names_list

# .....................................................................................................................

def delete_caminfos_by_cutoff(camera_select, cutoff_ems):
    
    # Find the closest camera info before the target time, since we'll want to keep it!
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_camera_info_collection(camera_select)
    no_result, entry_dict = get_closest_metadata_before_target_ems(collection_ref, cutoff_ems, epoch_ms_field)
    
    # Determine the deletion time to use, depending on whether we found an older camera info
    oldest_allowed_ems = cutoff_ems
    if not no_result:
        keep_oldest_ems = entry_dict[epoch_ms_field]
        oldest_allowed_ems = keep_oldest_ems - 1
    
    # Delete old camera info metadata!
    num_deleted = delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    return num_deleted

# .....................................................................................................................

def delete_cfginfos_by_cutoff(camera_select, cutoff_ems):
    
    # Find the closest config info before the target time, since we'll want to keep it!
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_config_info_collection(camera_select)
    no_result, entry_dict = get_closest_metadata_before_target_ems(collection_ref, cutoff_ems, epoch_ms_field)
    
    # Determine the deletion time to use, depending on whether we found an older config info
    oldest_allowed_ems = cutoff_ems
    if not no_result:
        keep_oldest_ems = entry_dict[epoch_ms_field]
        oldest_allowed_ems = keep_oldest_ems - 1
    
    # Delete old config info metadata!
    num_deleted = delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    return num_deleted

# .....................................................................................................................

def delete_backgrounds_by_cutoff(camera_select, cutoff_ems):
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Determine oldest background to delete
    
    # Find the closest background before the target time, since we'll want to keep it!
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_background_collection(camera_select)
    no_result, entry_dict = get_closest_metadata_before_target_ems(collection_ref, cutoff_ems, epoch_ms_field)
    
    # Determine the deletion time to use, depending on whether we found an older background
    oldest_allowed_ems = cutoff_ems
    if not no_result:
        keep_oldest_ems = entry_dict[epoch_ms_field]
        oldest_allowed_ems = keep_oldest_ems - 1
    
    # -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    # Delete background metadata then image data
    
    # Delete old metadata
    # Important to do this before image data so that we won't have any metadata pointing at missing images!
    num_deleted = delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    # Get list of all folder paths that hold data older than the allowable time
    old_image_folder_paths = get_old_background_image_folders_list(BASE_DATA_FOLDER_PATH,
                                                                   camera_select, oldest_allowed_ems)
    
    # Delete all the folders containing old background images
    for each_folder_path in old_image_folder_paths:
        rmtree(each_folder_path, ignore_errors = True)
    
    return num_deleted

# .....................................................................................................................

def delete_objects_by_cutoff(camera_select, cutoff_ems):
    
    # Delete old object metadata, based on when objects ended
    epoch_ms_field = OBJ_FINAL_EMS_FIELD
    collection_ref = get_object_collection(camera_select)
    num_deleted = delete_collection_by_target_time(collection_ref, cutoff_ems, epoch_ms_field)
    
    return num_deleted

# .....................................................................................................................

def delete_stations_by_cutoff(camera_select, cutoff_ems):
    
    # Find the closest station data before the target time, since we'll want to keep it!
    epoch_ms_field = STN_FINAL_EMS_FIELD
    collection_ref = get_station_collection(camera_select)
    no_result, entry_dict = get_closest_metadata_before_target_ems(collection_ref, cutoff_ems, epoch_ms_field)
    
    # Determine the deletion time to use, depending on whether we found an older station data entry
    oldest_allowed_ems = cutoff_ems
    if not no_result:
        keep_oldest_ems = entry_dict[epoch_ms_field]
        oldest_allowed_ems = keep_oldest_ems - 1
    
    # Delete old station metadata!
    num_deleted = delete_collection_by_target_time(collection_ref, oldest_allowed_ems, epoch_ms_field)
    
    return num_deleted

# .....................................................................................................................

def delete_snapshots_by_cutoff(camera_select, cutoff_ems):
    
    # Delete old metadata
    # Important to do this before deleting image data, so we don't have any metadata pointing to missing images!
    epoch_ms_field = DEFAULT_EMS_FIELD
    collection_ref = get_snapshot_collection(camera_select)
    num_deleted = delete_collection_by_target_time(collection_ref, cutoff_ems, epoch_ms_field)
    
    # Get list of all folder paths that hold data older than the allowable time
    old_image_folder_paths = get_old_snapshot_image_folders_list(BASE_DATA_FOLDER_PATH,
                                                                 camera_select, cutoff_ems)
    
    # Delete all the folders containing old snapshot images
    for each_folder_path in old_image_folder_paths:
        rmtree(each_folder_path, ignore_errors = True)
    
    return num_deleted

# .....................................................................................................................

def delete_all_realtime_by_cutoff_dt(camera_select, cutoff_datetime):
    
    ''' Helper function which just bundles data deletion for all realtime datasets in one place, with timing '''
    
    # Start timing
    t_start = perf_counter()
    
    # Build arguments for deletion functions
    cutoff_ems = datetime_to_epoch_ms(cutoff_datetime)
    deletion_args = (camera_select, cutoff_ems)
    
    # Delete each of the realtime datasets based on the cutoff time
    delete_caminfos_by_cutoff(*deletion_args)
    delete_cfginfos_by_cutoff(*deletion_args)
    delete_backgrounds_by_cutoff(*deletion_args)
    delete_objects_by_cutoff(*deletion_args)
    delete_stations_by_cutoff(*deletion_args)
    delete_snapshots_by_cutoff(*deletion_args)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    return time_taken_ms

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Major deletion functions

# .....................................................................................................................

def delete_empty_cameras(mongo_client, cameras_to_delete_list):
    
    '''
    Function which completely wipes out camera entries from both MongoDB & the file-system
    Intended to be called on cameras with no more useful data (i.e. cameras which have gone offline)
    '''
    
    # Loop through all cameras to delete and wipe out all mongo + file-system data
    for each_camera_name in cameras_to_delete_list:    
        camera_data_folder_path = build_camera_data_path(BASE_DATA_FOLDER_PATH, each_camera_name)
        remove_camera_entry(mongo_client, each_camera_name)
        rmtree(camera_data_folder_path, ignore_errors = True)
    
    return

# .....................................................................................................................

def delete_by_disk_usage(mongo_client, camera_names_list, oldest_data_dt, max_disk_usage_percent):
    
    '''
    Function which clears data (both image data + mongo documents) based on a maximum disk usage percentage
    Begins deleting data in chunks of time, based on a given 'oldest' datetime as a starting point
    '''
    
    # Get current disk usage (at least for the partition holding data for the dbserver itself)
    total_bytes, used_bytes, free_bytes = disk_usage(BASE_DATA_FOLDER_PATH)
    current_disk_usage_pct = int(round(100 * (used_bytes / total_bytes)))
    
    # If we don't need to delete by disk-usage, we're done
    dont_delete = (current_disk_usage_pct < max_disk_usage_percent)
    if dont_delete:
        results_str_list = ["Current disk usage: {}%".format(current_disk_usage_pct),
                            "       Max allowed: {}%".format(max_disk_usage_percent),
                            "  --> Skipping delete-by-disk usage"]
        return results_str_list
    
    # For clarity
    safety_margin_pct = 3
    target_disk_usage = (max_disk_usage_percent - safety_margin_pct)
    hours_per_delete_chunk = 8
    hours_per_day = 24
    max_additional_days_to_delete = 1.5
    max_re_deletes = int(round(max_additional_days_to_delete * hours_per_day / hours_per_delete_chunk))
    
    # Allocate space for storing per-camera timing data (used to provide feedback when logging results)
    time_taken_ms_dict = {each_camera_name: 0 for each_camera_name in camera_names_list}
    
    # If we get here, we need to delete based on disk usage
    # --> Performed by moving cutoff datetime forward in time and deleting by cutoff again, until we've cleared space
    # --> We'll first delete an entire day, since we expect to call this function (at most) once per day
    disk_usage_cutoff_dt = add_to_datetime(oldest_data_dt, days = 1)
    for _ in range(max_re_deletes):
        
        # Loop over all cameras and delete anything prior to the cutoff date & store timing results
        for each_camera_name in camera_names_list:
            time_taken_ms = delete_all_realtime_by_cutoff_dt(each_camera_name, disk_usage_cutoff_dt)
            time_taken_ms_dict[each_camera_name] += time_taken_ms
        
        # Re-check the disk usage to see if we can stop deleting!
        total_bytes, used_bytes, free_bytes = disk_usage(BASE_DATA_FOLDER_PATH)
        current_disk_usage_pct = int(round(100 * (used_bytes / total_bytes)))
        deleted_enough = (current_disk_usage_pct < target_disk_usage)
        if deleted_enough:
            break
        
        # If we still haven't deleted enough data, inch the cutoff date forward slightly and retry
        disk_usage_cutoff_dt = add_to_datetime(disk_usage_cutoff_dt, hours = hours_per_delete_chunk)
    
    # Finally, build results strings (i.e. timing for each camera)
    results_str_list = ["{}: {} ms".format(*name_and_time_ms) for name_and_time_ms in time_taken_ms_dict.items()]
    
    return results_str_list

# .....................................................................................................................

def delete_by_days(mongo_client, camera_names_list, oldest_data_dt, days_to_keep):
    
    '''
    Function which clears data (both image data + mongo documents) based on a cutoff date
    calculated based on a specified number of days to keep, relative to 'today' (i.e. when function is called)
    '''
    
    # Get current date so we can decide how far back we need to go before deleting data
    past_dt = get_local_datetime_in_past(days_to_keep)
    cutoff_dt = datetime_convert_to_day_start(past_dt)
    
    # Bail if the oldest data is 'newer' than the target cutoff date
    dont_delete = (oldest_data_dt > cutoff_dt)
    if dont_delete:
        results_str_list = ["Oldest data occurred less than {} days ago".format(days_to_keep),
                            "  --> Skipping delete-by-days-to-keep"]
        return results_str_list
    
    # Loop over all cameras and delete anything prior to the cutoff date
    results_str_list = []
    for each_camera_name in camera_names_list:
        
        # Perform deletion & build reporting string, for each camera
        time_taken_ms = delete_all_realtime_by_cutoff_dt(each_camera_name, cutoff_dt)
        each_result_str = "{}: {} ms".format(each_camera_name, time_taken_ms)
        results_str_list.append(each_result_str)
    
    return results_str_list

# .....................................................................................................................

def scheduled_delete(mongo_client, shutdown_event, log_to_file = True):
    
    '''
    Function which repeatedly deletes data from all cameras accessible through the provided mongo client
    Runs once per day, and tries to delete by a 'days to keep' setting as well as a 'max disk usage' setting
    Will fully remove camera entries when they no longer have any snapshot data
    '''
    
    # Create logger to handle saving feedback (or printing to terminal)
    logger = create_autodelete_logger(enabled = log_to_file)
    logger.log("Startup event!")
    
    # For clarity
    mongo_client_connection_config = {"connection_timeout_ms": 30000,
                                      "max_connection_attempts": 30}
    
    try:
    
        # Sleep & delete & sleep & delete & sleep & ...
        while True:
            
            # Generate a new (slightly random) time for the next deletion
            deletion_hour = get_env_hour_to_run()
            deletion_minute = int(round(59 * unit_random()))
            deletion_second = int(round(59 * unit_random()))
            deletion_dt = get_local_datetime_tomorrow(deletion_hour, deletion_minute, deletion_second)
            
            # Delay deletion until 'tomorrow' unless we're told to shutdown before then
            dt_now = get_local_datetime()
            seconds_to_wait = get_seconds_between_datetimes(dt_now, deletion_dt, round_to_int = True)
            shutdown_process = shutdown_event.wait(seconds_to_wait)
            if shutdown_process:
                logger.log("Shutdown event!")
                break
            
            # Get connection to mongo
            connection_success, mongo_client = connect_to_mongo(**mongo_client_connection_config)
            if not connection_success:
                logger.log("MongoDB connection failure")
                continue
            
            # Get deletion settings & camera listing
            days_to_keep, max_disk_usage_pct = AD_SETTINGS.get_settings()
            camera_names_list = get_camera_names_list(mongo_client)
            
            # Get the oldest snapshot timing, since we'll use this as a staring point for deleting data
            oldest_data_dt, empty_camera_names_list = get_oldest_snapshot_dt(camera_names_list)
            
            # Delete camera entries with no data if needed
            empty_cameras_to_delete = (len(empty_camera_names_list) > 0)
            if empty_cameras_to_delete:
                delete_empty_cameras(mongo_client, empty_camera_names_list)
                camera_names_list = sorted(list(set(camera_names_list).difference(empty_camera_names_list)))
                logger.log_list(["Deleted empty cameras:", *empty_camera_names_list])
            
            # Delete data by a max disk usage setting as well as a specified number of 'days to keep'
            shared_args = (mongo_client, camera_names_list, oldest_data_dt)
            mdu_results_list = delete_by_disk_usage(*shared_args, max_disk_usage_pct)
            dtk_results_list = delete_by_days(*shared_args, days_to_keep)
            
            # Print or log response
            logger.log_list(["Realtime data deletion results",
                             "Max disk usage:", *mdu_results_list,
                             "",
                             "Days to keep:", *dtk_results_list])
            
            # Close mongo connection
            mongo_client.close()
        
    except KeyboardInterrupt:
        logger.log("Keyboard interrupt!")    
    
    return

# .....................................................................................................................

def create_parallel_scheduled_delete(mongo_client,
                                     log_to_file = True,
                                     start_on_call = True):
    
    ''' Function which generates a separate process for handling data deletion in the background '''
    
    # Build configuration input for parallel process setup
    config_dict = {"mongo_client": mongo_client,
                   "shutdown_event": AD_SHUTDOWN_EVENT,
                   "log_to_file": log_to_file}
    
    # Create a parallel process to run the scheduled delete function and start it, if needed
    close_when_parent_closes = True
    proc_pid = None
    parallel_delete_ref = Process(target = scheduled_delete, kwargs = config_dict, daemon = close_when_parent_closes)    
    if start_on_call:
        parallel_delete_ref.start()
        proc_pid = parallel_delete_ref.pid
    
    return proc_pid, parallel_delete_ref

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Hard-code (global!) variable used to indicate timing field if not otherwise specified
DEFAULT_EMS_FIELD = "_id"

# Set up access to autodeletion settings
AD_SETTINGS = Autodelete_Settings()

# Set up event used to shutdown parallel deletion process
AD_SHUTDOWN_EVENT = Event()


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap

# TODO:
# - add fallback deletion of image data in case db in not reachable

