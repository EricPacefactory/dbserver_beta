#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 15:21:53 2020

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
from shutil import rmtree

from local.lib.mongo_helpers import MCLIENT, check_mongo_connection
from local.lib.mongo_helpers import remove_camera_entry, get_camera_names_list

from local.lib.timekeeper_utils import get_local_datetime
from local.lib.timekeeper_utils import datetime_to_isoformat_string, datetime_to_epoch_ms
from local.lib.timekeeper_utils import epoch_ms_to_local_isoformat, isoformat_to_epoch_ms

from local.lib.pathing import BASE_DATA_FOLDER_PATH, BASE_CAMERAS_FOLDER_PATH, GIT_READER, build_camera_data_path
from local.lib.response_helpers import bad_request_response, not_allowed_response, calculate_time_taken_ms

from starlette.responses import UJSONResponse, HTMLResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Helper functions

# .....................................................................................................................

def get_current_timing_info():
    
    # Add some additional info to html
    local_time_as_dt = get_local_datetime()
    local_time_as_isoformat = datetime_to_isoformat_string(local_time_as_dt)
    local_time_as_ems = datetime_to_epoch_ms(local_time_as_dt)
    
    return local_time_as_dt, local_time_as_isoformat, local_time_as_ems

# .....................................................................................................................

def check_for_sanity(sanity_check):
    
    try:
        provided_epoch_ms = int(sanity_check)
    except ValueError:
        return False
    
    # Get current timing info
    _, _, current_epoch_ms = get_current_timing_info()
    
    # For clarity
    one_hour_of_milliseconds = (60 * 60 * 1000)
    cutoff_epoch_ms = (current_epoch_ms - one_hour_of_milliseconds)
    passed_sanity_check = (provided_epoch_ms > cutoff_epoch_ms)
    
    return passed_sanity_check

# .....................................................................................................................

def check_git_version():
    
    ''' Helper function used to generate versioning info to be displayed on main web page '''
    
    # Initialize output in case of errors
    is_valid = False
    version_indicator_str = "unknown"
    commit_date_str = "unknown"
    
    # Try to get versioning info    
    try:
        commit_id, commit_tags_list, commit_dt = GIT_READER.get_current_commit()
        
        # Use tag if possible to represent the version
        version_indicator_str = ""
        if len(commit_tags_list) > 0:
            version_indicator_str = ", ".join(commit_tags_list)
        else:
            version_indicator_str = commit_id
        
        # Add time information
        commit_date_str = commit_dt.strftime("%b %d")
        
        # If we get here, the info is probably good
        is_valid = True
        
    except:
        pass
    
    return is_valid, commit_date_str, version_indicator_str

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create miscellaneous routes

# .....................................................................................................................

def root_page(request):
    
    ''' Home page route. Meant to provide (rough) UI to inspect available data '''
    
    # For convenience
    indent_by_2 = lambda message: "  {}".format(message)
    indent_by_4 = lambda message: indent_by_2(indent_by_2(message))
    
    # Request camera (database) names
    camera_names_list = get_camera_names_list(MCLIENT, sort_names = True)
    
    # Build html for each camera to show some sample data
    cam_html_list = []
    for each_camera_name in camera_names_list:
        pretty_camera_name = each_camera_name.replace("_", " ")
        caminfo_url = "/{}/camerainfo/get-newest-metadata".format(each_camera_name)
        cfginfo_url = "/{}/configinfo/get-newest-metadata".format(each_camera_name)
        snap_md_url = "/{}/snapshots/get-newest-metadata".format(each_camera_name)
        newest_image_url = "/{}/snapshots/get-newest-image".format(each_camera_name)
        img_html = "<a href='{}'><img src='{}' alt='Missing image data!'></a>".format(snap_md_url, newest_image_url)
        caminfo_link_html = "<a href='{}'>{}</a>".format(caminfo_url, pretty_camera_name)
        cgfinfo_link_html = "<a href='{}'>config</a>".format(cfginfo_url)
        camera_html = "<h3>{} ({})</h3>".format(caminfo_link_html, cgfinfo_link_html)
        cam_html_list += ["{}".format(camera_html),
                          indent_by_4(img_html),
                          indent_by_4("<br><br>")]
    
    # In special case of no cameras, include text to indicate it!
    no_cameras = (len(camera_names_list) == 0)
    if no_cameras:
        cam_html_list += ["<h4>No camera data!</h4>"]
    
    # Add dbserver versioning info
    version_is_valid, version_date_str, version_id_str = check_git_version()
    bad_version_entry = "<p>error getting version info!</p>"
    good_version_entry = "<p>version: {} ({})</p>".format(version_id_str, version_date_str)
    git_version_str = (good_version_entry if version_is_valid else bad_version_entry)
    
    # Finally build the full html string to output
    html_list = ["<!DOCTYPE html>",
                 "<html>",
                 "<head>",
                 indent_by_2("<title>DB Server</title>"),
                 indent_by_2("<link rel='icon' href='data:;base64,iVBORw0KGgo='>"),
                 "</head>",
                 "<body>",
                 indent_by_2("<h1><a href='/help'>Safety-cv-2 DB Server</a></h1>"),
                 *(indent_by_2(each_cam_str) for each_cam_str in cam_html_list),
                 indent_by_2(git_version_str),
                 "</body>",
                 "</html>"]
    html_resp = "\n".join(html_list)
    
    return HTMLResponse(html_resp)

# .....................................................................................................................

def is_alive_check(request):
    
    ''' Route used to check that this server is still up (before making a ton of requests for example) '''
    
    mongo_is_connected, server_info_dict = check_mongo_connection(MCLIENT)
    
    return UJSONResponse({"dbserver": True, "mongo": mongo_is_connected})

# .....................................................................................................................

def get_dbserver_version(request):
    
    
    ''' Route used to check the current dbserver version (based on git repo details) '''
    
    try:
        commit_id, commit_tags_list, commit_dt = GIT_READER.get_current_commit()
        isoformat_datetime = datetime_to_isoformat_string(commit_dt)
        
    except Exception:
        commit_id = "error"
        commit_tags_list = []
        isoformat_datetime = "error"
    
    # Bundle results for better return
    return_result = {"commit_id": commit_id,
                     "tags_list": commit_tags_list,
                     "commit_datetime_isoformat": isoformat_datetime}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_all_camera_names(request):
    
    ''' Route which is intended to return a list of camera names '''
    
    camera_names_list = get_camera_names_list(MCLIENT)
    
    return UJSONResponse(camera_names_list)

# .....................................................................................................................

def time_epoch_ms_to_datetime_isoformat(request):
    
    ''' Route which converts epoch ms values to datetime isoformat values '''
    
    # Get information from route url
    epoch_ms = request.path_params["epoch_ms"]
    
    # Perform time conversion
    try:
        datetime_isoformat_str = epoch_ms_to_local_isoformat(epoch_ms)
        
    except (OverflowError, ValueError):
        error_message = "Epoch ms value was too large!"
        return bad_request_response(error_message)
    
    return UJSONResponse(datetime_isoformat_str)

# .....................................................................................................................

def time_datetime_isoformat_to_epoch_ms(request):
    
    ''' Route which converts isoformat datetime values to epoch ms values '''
    
    # Get information from route url
    datetime_isoformat_str = request.path_params["datetime_isoformat"]
    
    # Perform time conversion if possible
    try:
        epoch_ms = isoformat_to_epoch_ms(datetime_isoformat_str)
        
    except (IndexError, ValueError):
        _, local_time_as_isoformat, _ = get_current_timing_info()
        error_message = ["Couldn't convert to epoch ms value!",
                         "Isoformat:",
                         "YYYY-MM-DDThh:mm:ss.ms+ZZ:ZZ",
                         "",
                         "Example: {}".format(local_time_as_isoformat),
                         "    Got: {}".format(datetime_isoformat_str)]
        return bad_request_response(error_message)
    
    return UJSONResponse(epoch_ms)

# .....................................................................................................................

def remove_one_camera(request):
    
    ''' Nuclear route. Completely removes a camera (+ image data) from the system '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    sanity_check = request.path_params["sanity_check"]
    
    # Don't delete unless the sanity check if passed
    passed_sanity_check = check_for_sanity(sanity_check)
    if not passed_sanity_check:
        return not_allowed_response("Failed sanity check... Camera removal cancelled!")
    
    # Don't allow deletion of built-in dbs
    ignore_db_names = {"admin", "local", "config"}
    if camera_select in ignore_db_names:
        return not_allowed_response("Can't delete the given entry! ({})".format(camera_select))
    
    # Start timing
    t_start = perf_counter()
    
    # Get camera (file system) data pathing
    camera_data_folder_path = build_camera_data_path(BASE_DATA_FOLDER_PATH, camera_select)
    
    # Check if camera is in our list
    camera_names_before_list = get_camera_names_list(MCLIENT)
    camera_in_mongo_before = (camera_select in camera_names_before_list)
    camera_in_image_storage_before = (os.path.exists(camera_data_folder_path))
    camera_exists_before = (camera_in_mongo_before or camera_in_image_storage_before)
    
    # Wipe out entire camera database and data folder, if possible
    remove_camera_entry(MCLIENT, camera_select)
    rmtree(camera_data_folder_path, ignore_errors = True)
    
    # Check if the camera has been removed
    camera_names_after_list = get_camera_names_list(MCLIENT)
    camera_in_mongo_after = (camera_select in camera_names_after_list)
    camera_in_image_storage_after = (os.path.exists(camera_data_folder_path))
    camera_exists_after = (camera_in_mongo_after or camera_in_image_storage_after)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    
    # Build output for feedback
    return_result = {"camera_exists_before": camera_exists_before,
                     "camera_exists_after": camera_exists_after,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def remove_all_cameras(request):
    
    ''' Extra-nuclear option!!! Completely removes all data from mongo and image data from storage '''
    
    # Get information from route url
    sanity_check = request.path_params["sanity_check"]
    
    # Don't delete unless the sanity check if passed
    passed_sanity_check = check_for_sanity(sanity_check)
    if not passed_sanity_check:
        return not_allowed_response("Failed sanity check... Camera removals cancelled!")
    
    # Start timing
    t_start = perf_counter()
    
    # Clear all database entries, except the system ones
    camera_names_list = get_camera_names_list(MCLIENT)
    for each_camera_name in camera_names_list:
        remove_camera_entry(MCLIENT, each_camera_name)
    
    # Delete cameras folder (but re-create an empty folder)
    cameras_removed_list = os.listdir(BASE_CAMERAS_FOLDER_PATH)
    rmtree(BASE_CAMERAS_FOLDER_PATH, ignore_errors = True)
    os.makedirs(BASE_CAMERAS_FOLDER_PATH, exist_ok = True)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    
    # Build output for feedback
    return_result = {"data_removed": camera_names_list,
                     "cameras_removed": cameras_removed_list,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_help_route(routes_ordered_dict):
    
    '''
    Function used to create the help page. Should be used after creating all other routes
    Returns a 'list' of routes, although the list is only 1 element long! (the help route itself)
    '''
    
    def help_page(request):
        
        # For clarity
        unique_methods = ["POST", "PUT", "DELETE"]
        
        # Initialize output html listing
        html_list = ["<title>DB Server Help</title>",
                     "",
                     "<style>",
                     "h1 { text-align: center; }",
                     "h3 { margin: 0; padding: 0.5em; padding-top: 2em; }",
                     "p  { margin-left: 2em; }",
                     "</style>",
                     "",
                     "<h1>Route Help</h1>"]
        
        # Build sections for each set of routes
        for each_route_title, each_route_list in routes_ordered_dict.items():
            
            # Build the section title html
            title_str = "<h3>{}</h3>".format(each_route_title)
            html_list += ["", title_str]
            
            # Build each url entry
            for each_route in each_route_list:
                
                # Pull out the route url for printing & check for route methods (GET, POST etc.)
                each_url = each_route.path
                try:
                    each_unique_methods_list = [e_meth for e_meth in each_route.methods if e_meth in unique_methods]
                    
                except AttributeError:
                    # Websocket routes do not have 'methods', so do nothing
                    each_unique_methods_list = []
                
                # Figure out what to print for unique methods (if anything!)
                has_unique_methods = (len(each_unique_methods_list) > 0)
                unique_methods_str = ", ".join(each_unique_methods_list)
                unique_methods_html = "<b><em>{}</em></b>&nbsp;&nbsp;".format(unique_methods_str)
                
                # Build the html for displaying each route (either as plain text or as a clickable link)
                post_tag_html = unique_methods_html if has_unique_methods else ""
                each_url_html = "  <p>{}{}</p>".format(post_tag_html, each_url)
                each_link_html = "  <p>{}<a href={}>{}</a></p>".format(post_tag_html, each_url, each_url)
                
                # Decide whether we show plain/linked urls
                unlinkable_url = ("{" in each_url)
                each_display_url = each_url_html if unlinkable_url else each_link_html
                html_list.append(each_display_url)
        
        # Add some additional info to html
        _, isoformat_example, epoch_ms_example = get_current_timing_info()
        spacer_str = 10 * "&nbsp;"
        html_list += ["", "<br><br>",
                      "<h3>Note:</h3>",
                      "  <p>If not specified, 'time' values can be provided in string or integer format</p>",
                      "  <p>--> String format times must follow isoformat</p>",
                      "    <p>{}ex: {}</p>".format(spacer_str, isoformat_example),
                      "  <p>--> Integer format times must be epoch millisecond values</p>",
                      "    <p>{}ex: {}</p>".format(spacer_str, epoch_ms_example),
                      "", "<br>", "<b><a href='/'>BACK</a></b>"]
        
        # Finally build the full html string to output
        html_resp = "\n".join(html_list)
        
        return HTMLResponse(html_resp)
    
    # Finally, associate a url with the help page output
    help_route = Route("/help", help_page)
    
    return help_route

# .....................................................................................................................

def build_misc_routes():
    
    # Bundle all miscellaneous routes
    misc_routes = \
    [
     Route("/", root_page),
     Route("/is-alive", is_alive_check),
     Route("/get-version-info", get_dbserver_version),
     Route("/get-all-camera-names", get_all_camera_names),
     Route("/time/ems-to-isoformat/{epoch_ms:int}", time_epoch_ms_to_datetime_isoformat),
     Route("/time/isoformat-to-ems/{datetime_isoformat:str}", time_datetime_isoformat_to_epoch_ms),
     Route("/remove/one-camera/{camera_select:str}/{sanity_check}", remove_one_camera),
     Route("/remove/all-cameras/{sanity_check}", remove_all_cameras)
    ]
    
    return misc_routes

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


