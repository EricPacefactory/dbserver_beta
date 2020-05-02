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

from local.lib.mongo_helpers import check_mongo_connection, connect_to_mongo
from local.lib.timekeeper_utils import get_local_datetime, datetime_to_isoformat_string, datetime_to_epoch_ms
from local.lib.image_pathing import build_base_image_pathing, build_camera_image_path
from local.lib.response_helpers import not_allowed_response

from starlette.responses import UJSONResponse, HTMLResponse
from starlette.routing import Route


# ---------------------------------------------------------------------------------------------------------------------
#%% Create miscellaneous routes

# .....................................................................................................................

def root_page(request):
    
    ''' Home page route. Meant to provide (rough) UI to inspect available data '''
    
    # Request data from all dbs
    all_db_names_list = mclient.list_database_names()
    
    # Remove built-in databases
    ignore_db_names = {"admin", "local", "config"}
    camera_names_list = [each_name for each_name in all_db_names_list if each_name not in ignore_db_names]
    
    # Build html line by line for each camera to show some sample data
    html_list = ["<title>DB Server</title>", "<h1><a href='/help'>Safety-cv-2 DB Server</a></h1>"]
    for each_camera_name in camera_names_list:
        caminfo_url = "/{}/camerainfo/get-newest-camera-info".format(each_camera_name)
        snap_md_url = "/{}/snapshots/get-newest-metadata".format(each_camera_name)
        newest_image_url = "/{}/snapshots/get-newest-image".format(each_camera_name)
        img_html = "<a href='{}'><img src='{}' alt='Missing image data!'></a>".format(snap_md_url, newest_image_url)
        camera_html = "<h3><a href='{}'>{}</a></h3>".format(caminfo_url, each_camera_name.replace("_", " "))
        html_list += [camera_html, img_html, "<br><br>"]
    
    # In special case of no cameras, include text to indicate it!
    no_cameras = (len(camera_names_list) == 0)
    if no_cameras:
        html_list += ["<h4>No camera data!</h4>"]
    
    # Finally build the full html string to output
    html_resp = "\n".join(html_list)
    
    return HTMLResponse(html_resp)

# .....................................................................................................................

def is_alive_check(request):
    
    ''' Route used to check that this server is still up (before making a ton of requests for example) '''
    
    mongo_is_connected, server_info_dict = check_mongo_connection(mclient)
    
    return UJSONResponse({"dbserver": True, "mongo": mongo_is_connected})

# .....................................................................................................................

def cameras_get_all_names(request):
    
    ''' Route which is intended to return a list of camera names '''
    
    # Request data from all dbs
    all_db_names_list = mclient.list_database_names()
    
    # Remove built-in databases
    ignore_db_names = {"admin", "local", "config"}
    return_result = [each_name for each_name in all_db_names_list if each_name not in ignore_db_names]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def remove_one_camera(request):
    
    ''' Nuclear route. Completely removes a camera (+ image data) from the system '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    sanity_check = request.path_params["password"]
    
    # Don't delete unless the correct 'password' was given, just to avoid accidents
    correct_password = "pf"
    if sanity_check != correct_password:
        return not_allowed_response("Wrong password. Database reset cancelled!")
    
    # Don't allow deletion of built-in dbs
    ignore_db_names = {"admin", "local", "config"}
    if camera_select in ignore_db_names:
        return not_allowed_response("Can't delete the given entry! ({})".format(camera_select))
    
    # Start timing
    t_start = perf_counter()
    
    # Get image pathing
    base_image_path = build_base_image_pathing()
    camera_image_folder_path = build_camera_image_path(base_image_path, camera_select)
    
    # Check if camera is in our list
    all_db_names = mclient.list_database_names()
    camera_in_mongo_before = (camera_select in all_db_names)
    camera_in_image_storage_before = (os.path.exists(camera_image_folder_path))
    camera_exists_before = (camera_in_mongo_before or camera_in_image_storage_before)
    
    # Wipe out entire camera database and image folder, if possible
    mclient.drop_database(camera_select)
    rmtree(camera_image_folder_path, ignore_errors = True)
    
    # Check if the camera has been removed
    all_db_names = mclient.list_database_names()
    camera_in_mongo_after = (camera_select in all_db_names)
    camera_in_image_storage_after = (os.path.exists(camera_image_folder_path))
    camera_exists_after = (camera_in_mongo_after or camera_in_image_storage_after)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output for feedback
    return_result = {"camera_exists_before": camera_exists_before,
                     "camera_exists_after": camera_exists_after,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def remove_all_cameras(request):
    
    ''' Extra-nuclear option!!! Completely removes all data from mongo and image data from storage '''
    
    # Get information from route url
    sanity_check = request.path_params["password"]
    
    # Don't reset unless the correct 'password' was given, just to avoid accidents
    correct_password = "pf"
    if sanity_check != correct_password:
        return not_allowed_response("Wrong password. Database reset cancelled!")
    
    # Start timing
    t_start = perf_counter()
    
    # Clear all database entries, except the system ones
    ignore_db_names = {"admin", "local", "config"}
    all_db_names = mclient.list_database_names()
    data_removed = []
    for each_db_name in all_db_names:
        if each_db_name in ignore_db_names:
            continue
        mclient.drop_database(each_db_name)
        data_removed.append(each_db_name)
    
    # Delete image folder (but re-create an empty folder)
    base_image_path = build_base_image_pathing()
    images_removed = os.listdir(base_image_path)
    rmtree(base_image_path, ignore_errors = True)
    os.makedirs(base_image_path, exist_ok = True)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = int(round(1000 * (t_end - t_start)))
    
    # Build output for feedback
    return_result = {"data_removed": data_removed,
                     "images_removed": images_removed,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define call functions

# .....................................................................................................................

def build_help_route(*route_name_and_list_tuples):
    
    '''
    Function used to create the help page. Should be used after creating all other routes 
    Returns a 'list' of routes, although the list is only 1 element long! (the help route itself)
    '''
    
    def help_page(request):
        
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
        for each_route_title, each_route_list in route_name_and_list_tuples:
            
            # First pull out the actual route url (as strings) for printing
            route_urls_gen = (each_route.path_format for each_route in each_route_list)
            
            # Build a little section for each set of routes, and add some spacing just for nicer looking raw html
            title_str = "<h3>{}</h3>".format(each_route_title)
            line_strs_gen = ("  <p>{}</p>".format(each_url) for each_url in route_urls_gen)
            html_list += ["", title_str, *line_strs_gen]

        # Add some additional info to html
        local_dt = get_local_datetime()
        isoformat_example = datetime_to_isoformat_string(local_dt)
        epoch_ms_example = datetime_to_epoch_ms(local_dt)
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
    
    # Build the output as a list, so it can be 'added' to other lists of routes more easily
    help_route_as_list = [Route("/help", help_page)]
    
    return help_route_as_list

# .....................................................................................................................

def build_misc_routes():
    
    # Bundle all miscellaneous routes
    misc_routes = \
    [
     Route("/", root_page),
     Route("/is-alive", is_alive_check),
     Route("/get-all-camera-names", cameras_get_all_names),
     Route("/remove/one-camera/{camera_select:str}/{password:str}", remove_one_camera),
     Route("/remove/all-cameras/{password:str}", remove_all_cameras)
    ]
    
    return misc_routes

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Global setup

# Connection to mongoDB
mclient = connect_to_mongo()


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    pass

# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


