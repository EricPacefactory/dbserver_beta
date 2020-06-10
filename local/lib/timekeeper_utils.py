#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 15:48:05 2020

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

import time
import datetime as dt


# ---------------------------------------------------------------------------------------------------------------------
#%% Define classes

# .....................................................................................................................

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% General functions

# .....................................................................................................................
    
def get_utc_datetime():

    ''' Returns a datetime object based on UTC time, with timezone information included '''

    return dt.datetime.utcnow().replace(tzinfo = get_utc_tzinfo())
    
# .....................................................................................................................
    
def get_local_datetime():

    ''' Returns a datetime object based on the local time, with timezone information included '''

    return dt.datetime.now(tz = get_local_tzinfo())

# .....................................................................................................................

def get_local_tzinfo():
    
    ''' Function which returns a local tzinfo object. Accounts for daylight savings '''
    
    # Figure out utc offset for local time, accounting for daylight savings
    is_daylight_savings = time.localtime().tm_isdst
    utc_offset_sec = time.altzone if is_daylight_savings else time.timezone
    utc_offset_delta = dt.timedelta(seconds = -utc_offset_sec)
    
    return dt.timezone(offset = utc_offset_delta)
    
# .....................................................................................................................

def get_utc_tzinfo():
    
    ''' Convenience function which returns a utc tzinfo object '''
    
    return dt.timezone.utc

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Isoformat conversion functions

# .....................................................................................................................

def isoformat_to_datetime(isoformat_datetime_str):
    
    '''
    Function for parsing isoformat strings
    Example string:
        "2019-05-11T17:22:33+00:00.999"
    '''
    
    # Check if the end of the string contains timezone offset info
    includes_offset = isoformat_datetime_str[-6] in ("+", "-")
    offset_dt = dt.timedelta(0)
    if includes_offset:
        
        # Figure out the timezone offset amount
        offset_hrs = int(isoformat_datetime_str[-6:-3])
        offset_mins = int(isoformat_datetime_str[-2:])
        offset_mins = offset_mins if offset_hrs > 0 else -1 * offset_mins
        offset_dt = dt.timedelta(hours = offset_hrs, minutes = offset_mins)
        
        # Remove offset from string before trying to parse
        isoformat_datetime_str = isoformat_datetime_str[:-6]
    
    # Convert timezone information into a timezone object that we can add back into the returned result
    parsed_tzinfo = dt.timezone(offset = offset_dt)
    
    # Decide if we need to parse milli/micro seconds
    includes_subseconds = len(isoformat_datetime_str) > 19
    string_format = "%Y-%m-%dT%H:%M:%S.%f" if includes_subseconds else "%Y-%m-%dT%H:%M:%S"
    
    # Finally, create the output datetime, with timezone info
    parsed_dt = dt.datetime.strptime(isoformat_datetime_str[:], string_format).replace(tzinfo = parsed_tzinfo)
    
    return parsed_dt

# .....................................................................................................................

def isoformat_to_epoch_ms(datetime_isoformat_string):
    
    '''
    Helper function which first converts an isoformat datetime string into a python datetime object
    then converts the datetime object into an epoch_ms value
    '''
    
    return datetime_to_epoch_ms(isoformat_to_datetime(datetime_isoformat_string))

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Datetime conversion functions

# .....................................................................................................................

def datetime_to_isoformat_string(input_datetime):
    
    '''
    Converts a datetime object into an isoformat string
    Example:
        "2019-01-30T11:22:33+00:00.000000"
    
    Note: This function assumes the datetime object has timezone information (tzinfo)
    '''
    
    return input_datetime.isoformat()

# .....................................................................................................................

def datetime_to_epoch_ms(input_datetime):
    
    ''' Function which converts a datetime to the number of milliseconds since the 'epoch' (~ Jan 1970) '''
    
    return int(round(1000 * input_datetime.timestamp()))

# .....................................................................................................................

def local_datetime_to_utc_datetime(local_datetime):
    
    ''' Convenience function for converting datetime objects from local timezones to utc '''
    
    return (local_datetime - local_datetime.utcoffset()).replace(tzinfo = get_utc_tzinfo())

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Epoch conversion functions

# .....................................................................................................................

def epoch_ms_to_utc_datetime(epoch_ms):
    
    ''' Function which converts a millisecond epoch value into a utc datetime object '''
    
    epoch_sec = epoch_ms / 1000.0
    return dt.datetime.utcfromtimestamp(epoch_sec).replace(tzinfo = get_utc_tzinfo())

# .....................................................................................................................

def epoch_ms_to_local_datetime(epoch_ms):
    
    ''' Function which converts a millisecond epoch value into a datetime object with the local timezone '''
    
    epoch_sec = epoch_ms / 1000.0
    return dt.datetime.fromtimestamp(epoch_sec).replace(tzinfo = get_local_tzinfo())

# .....................................................................................................................

def epoch_ms_to_utc_isoformat(epoch_ms):
    
    '''
    Helper function which first converts an epoch_ms value into a python datetime object
    then converts the datetime object into an isoformat string
    The result will use a UTC timezone
    '''
    
    return datetime_to_isoformat_string(epoch_ms_to_utc_datetime(epoch_ms))

# .....................................................................................................................

def epoch_ms_to_local_isoformat(epoch_ms):
    
    '''
    Helper function which first converts an epoch_ms value into a python datetime object
    then converts the datetime object into an isoformat string
    The result will use the local timezone
    '''
    
    return datetime_to_isoformat_string(epoch_ms_to_local_datetime(epoch_ms))

# .................................................................................................................

def any_time_type_to_epoch_ms(time_value):
    
    # Decide how to handle the input time value based on it's type
    value_type = type(time_value)
    
    # If an integer is provided, assume it is already an epoch_ms value
    if value_type is int:
        return time_value
    
    # If a float is provided, assume it is an epoch_ms value, so return integer version
    elif value_type is float:
        return int(round(time_value))
    
    # If a datetime vlaue is provided, use timekeeper library to convert
    elif value_type is dt.datetime:
        return datetime_to_epoch_ms(time_value)
    
    # If a string is provided, assume it is an isoformat datetime string
    elif value_type is str:
        return isoformat_to_epoch_ms(time_value)
    
    # If we get here, we couldn't parse the time!
    raise TypeError("Unable to parse input time value: {}, type: {}".format(time_value, value_type))

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Timing delta functions

# .....................................................................................................................

def get_utc_datetime_in_past(num_days_in_past):
    
    '''
    Helper function for getting a datetime from several days ago. Mostly intended for deletion/time cut-offs
    Returns a datetime object
    '''
    
    # Make sure days backward is greater than 0
    num_days_in_past = max(0, num_days_in_past)
    
    # Calculate the datetime from several days ago
    current_utc_dt = get_utc_datetime()
    past_utc_dt = current_utc_dt - dt.timedelta(days = num_days_in_past)
    
    return past_utc_dt

# .....................................................................................................................

def get_local_datetime_in_past(num_days_in_past):
    
    '''
    Helper function for getting a datetime from several days ago. Mostly intended for deletion/time cut-offs
    Returns a datetime object
    '''
    
    # Make sure days backward is greater than 0
    num_days_in_past = max(0, num_days_in_past)
    
    # Calculate the datetime from several days ago
    current_dt = get_local_datetime()
    past_dt = current_dt - dt.timedelta(days = num_days_in_past)
    
    return past_dt

# .....................................................................................................................

def add_days_to_datetime(input_datetime, num_days_to_add):
    
    '''
    Helper function for offseting a datetime by a specified number of days. Can be negative!
    Returns a datetime object
    '''
    
    return input_datetime + dt.timedelta(days = num_days_to_add)

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Image file formatting functions

# .....................................................................................................................

def image_folder_names_to_epoch_ms(date_folder_name, hour_folder_name):
    
    '''
    Helper function used to generate an epoch_ms (local) value from provided date/hour folder names.
    Returns:
        start_of_hour_epoch_ms, end_of_hour_epoch_ms
    '''
    
    # Get the starting datetime, based on the given date/hour folder names
    datetime_str = "{} {}".format(date_folder_name, hour_folder_name)
    str_format = "{} {}".format(DATE_FORMAT, HOUR_FORMAT)
    start_of_hour_dt_no_tz = dt.datetime.strptime(datetime_str, str_format)
    
    # Make sure to explicitly use local timing, to hopefully avoid weird timezone errors
    start_of_hour_dt_local = start_of_hour_dt_no_tz.replace(tzinfo = get_local_tzinfo())
    start_of_hour_epoch_ms = datetime_to_epoch_ms(start_of_hour_dt_local)
    
    # Calculate the end of hour epoch_ms value
    ms_in_one_hour = 3600000    # (60 mins/hr * 60 sec/min * 1000 ms/sec)
    end_of_hour_epoch_ms = start_of_hour_epoch_ms + ms_in_one_hour - 1
    
    return start_of_hour_epoch_ms, end_of_hour_epoch_ms

# .....................................................................................................................

def epoch_ms_to_image_folder_names(epoch_ms):
    
    '''
    Helper function used to provided consistent folder naming, based on input epoch_ms times
    Returns:
        date_folder_name, hour_folder_name
    '''
    
    # Convert provided epoch_ms value into a datetime, so we can create date + hour folder names from it
    target_time_dt = epoch_ms_to_local_datetime(epoch_ms)
    date_name = target_time_dt.strftime(DATE_FORMAT)
    hour_name = target_time_dt.strftime(HOUR_FORMAT)
    
    return date_name, hour_name

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Set string formatting globally, so it can be applied consistently wherever possible
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
HOUR_FORMAT = "%H"


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    # Check that image folder naming works properly (can be weird due to utc conversion!)
    ex_ems = 800000000000
    ex_date_folder, ex_hour_folder = epoch_ms_to_image_folder_names(ex_ems)
    ex_start_ems, ex_end_ems = image_folder_names_to_epoch_ms(ex_date_folder, ex_hour_folder)
    print("",
          "  Input EMS: {}".format(ex_ems),
          "Date folder: {}".format(ex_date_folder),
          "Hour folder: {}".format(ex_hour_folder),
          "  Start EMS: {}".format(ex_start_ems),
          "    End EMS: {}".format(ex_end_ems),
          "",
          "Input is within start/end: {}".format(ex_start_ems <= ex_ems <= ex_end_ems),
          "", sep = "\n")
    
    pass


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap



