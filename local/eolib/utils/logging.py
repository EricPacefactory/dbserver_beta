#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  3 12:44:02 2020

@author: eo
"""


# ---------------------------------------------------------------------------------------------------------------------
#%% Imports

import os

import datetime as dt

# ---------------------------------------------------------------------------------------------------------------------
#%% Define classes

# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


class Daily_Logger:
    
    '''
    Object which handles logging to files saved by the date on which they are called.
    Assumes all log files are stored together in a specified folder.
    '''

    # .................................................................................................................
    
    def __init__(self, log_folder_path, log_files_to_keep = 20, file_name_strformat = "%Y-%m-%d", 
                 enabled = True, print_when_disabled = True, include_timestamp = True):
        
        # Store configuration data
        self._log_folder_path = log_folder_path
        self._files_to_keep = log_files_to_keep
        self._name_format = file_name_strformat
        self._enabled = enabled
        self._print_when_disabled = print_when_disabled
        self._include_timestamp = include_timestamp
        
        # Set up initial 'previous' date
        date_now, _ = self._get_date_and_time()
        self._previous_date = date_now - dt.timedelta(days = 1)
        
        # Make sure folder pathing exists, if needed
        self.enable_logging(enabled)
    
    # .................................................................................................................
    
    def __repr__(self):
        
        repr_str_list = ["Daily Logger",
                         "  Log name format: {}".format(self._name_format),
                         "         Log path: {}".format(self._log_folder_path),
                         "    Files to keep: {}".format(self._files_to_keep),
                         "          Enabled: {}".format(self._enabled)]
        
        return "\n".join(repr_str_list)
    
    # .................................................................................................................
    
    def enable_logging(self, enable_logging = True):
        
        ''' Function to allow enabling of logging after initialization '''
        
        self._enabled = enable_logging
        
        # Make sure the folder path is valid, so we don't have to constantly check it when writing
        if enable_logging:
            os.makedirs(self._log_folder_path, exist_ok = True)
        
        return
    
    # .................................................................................................................
    
    def disable_logging(self):
        
        ''' Function to allow disabling of logging after initialization '''
        
        self._enabled = False
        
        return
        
    # .................................................................................................................
    
    def log(self, message_string, prepend_empty_line = True):
        
        ''' Function which logs a message to a log file, based on the date of calling the function '''
        
        # Get the current date so that we write into the correct file
        current_date, current_time = self._get_date_and_time()
        
        # Build newline entry for writing, along with potentially blank line prepended for separating
        string_list = [""] if prepend_empty_line else []
        if self._include_timestamp:
            timestamp_str = current_time.strftime("## %I:%M:%S%p")
            string_list.append(timestamp_str)
        
        # Finally, add the actual message and convert to string for writing to file
        string_list.append(message_string)
        string_list.append("")
        output_string = "\n".join(string_list)
        
        # Handle disabled state
        if not self._enabled:
            if self._print_when_disabled:
                print(output_string)
            return
        
        # Build pathing to the target log file and append a new message to it
        write_file_path = self._build_file_path(current_date)
        with open(write_file_path, "a") as log_file:
            log_file.write(output_string)
        
        # Delete old log files if needed, but only check when the date changes (avoid hammering the filesystem)
        today_is_a_new_day = (current_date != self._previous_date)
        if today_is_a_new_day:
            self._delete_excess_log_files()
        
        # Finally, record the current date as the 'previous' one for our next iteration
        self._previous_date = current_date
        
        return
    
    # .................................................................................................................
    
    def log_list(self, message_string_list, prepend_empty_line = True, entry_separator = "\n"):
        
        ''' Helper function which converts lists of strings to a single string (with newline separator by default) '''
        
        return self.log(entry_separator.join(message_string_list), prepend_empty_line)
        
    # .................................................................................................................
    
    def _build_file_path(self, dt_date_now):
        
        ''' Helper function to build file pathing to a log file, given a date '''
        
        date_now_str = dt_date_now.strftime(self._name_format)
        
        file_name = "{}.log".format(date_now_str)
        file_path = os.path.join(self._log_folder_path, file_name)
        
        return file_path
    
    # .................................................................................................................
    
    def _get_date_and_time(self):
        
        ''' Helper function for getting the current (local!) date & time '''
        
        # Get current (local) datetime and split into date & time
        dt_now = dt.datetime.now()
        date_now = dt_now.date()
        time_now = dt_now.time()
        
        return date_now, time_now
    
    # .................................................................................................................
    
    def _delete_excess_log_files(self):
        
        # Don't delete anything if we don't have a file limit!
        if self._files_to_keep is None:
            return
        
        # Find existing log files for possible deletion
        existing_file_names = os.listdir(self._log_folder_path)
        existing_file_paths = [os.path.join(self._log_folder_path, each_file) for each_file in existing_file_names]
        sorted_file_paths_newest_first = sorted(existing_file_paths, key = os.path.getmtime, reverse = True)
        
        # Remove tany files past some file count threshold
        file_paths_to_remove = sorted_file_paths_newest_first[self._files_to_keep:]
        for each_file_path in file_paths_to_remove:
            os.remove(each_file_path)
        
        return
    
    # .................................................................................................................
    # .................................................................................................................


# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

# ---------------------------------------------------------------------------------------------------------------------
#%% Define Functions

# .....................................................................................................................

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    log_folder_path = os.path.expanduser(os.path.join("~", "Desktop", "Daily_Logger_Example"))
    logger = Daily_Logger(log_folder_path, log_files_to_keep = 3)
    logger.log("Hello?")
    logger.log("Testing daily logger from {}".format(os.path.basename(__file__)))
    logger.log_list(["A", "AB", "ABC", "ABCD", "ABCDE", "12345"])
    

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


