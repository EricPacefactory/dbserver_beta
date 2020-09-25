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
from shutil import rmtree, disk_usage, which
from subprocess import check_output

from local.lib.mongo_helpers import MCLIENT, check_mongo_connection
from local.lib.mongo_helpers import get_collection_names_list, remove_camera_entry, get_camera_names_list

from local.lib.timekeeper_utils import get_local_datetime, datetime_to_isoformat_string, datetime_to_epoch_ms
from local.lib.image_pathing import build_base_image_pathing, build_camera_image_path
from local.lib.response_helpers import not_allowed_response

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

def calculate_time_taken_ms(time_start, time_end):
    return int(round(1000 * (time_end - time_start)))

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
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create miscellaneous routes

# .....................................................................................................................

def root_page(request):
    
    ''' Home page route. Meant to provide (rough) UI to inspect available data '''
    
    # Request camera (database) names
    camera_names_list = get_camera_names_list(MCLIENT)
    
    # Build html line by line for each camera to show some sample data
    html_list = ["<title>DB Server</title>", "<h1><a href='/help'>Safety-cv-2 DB Server</a></h1>"]
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
    
    mongo_is_connected, server_info_dict = check_mongo_connection(MCLIENT)
    
    return UJSONResponse({"dbserver": True, "mongo": mongo_is_connected})

# .....................................................................................................................

def cameras_get_all_names(request):
    
    ''' Route which is intended to return a list of camera names '''
    
    camera_names_list = get_camera_names_list(MCLIENT)
    
    return UJSONResponse(camera_names_list)

# .....................................................................................................................

def get_memory_usage(request):
    
    '''
    Route which returns info about memory usage (RAM & swap)
    Relies on the output from the 'free' command, which looks like:
    
    $ free -b
                  total        used        free      shared  buff/cache   available
    Mem:     8359682048  1588756480  1553772544    44699648  5217153024  6440677376
    Swap:    2147479552           0  2147479552
    '''
    
    # First make sure the free command is available
    mem_cmd = "free"
    command_bin_path = which(mem_cmd)
    if command_bin_path is None:
        return_result = {"error": "Memory-check program ({}) is not present!".format(mem_cmd)}
        return UJSONResponse(return_result)
    
    # Initialize outputs, in case this fails
    ram_bytes_dict = "error"
    swap_bytes_dict = "error"
    ram_percent_usage = "error"
    
    # Start timing, mostly for fun (and vaguely useful as a heads-up if this command is super slow)
    t_start = perf_counter()
    
    try:
        # Get result from the free command and split into rows
        free_memory_response = check_output([mem_cmd, "-b"], universal_newlines = True)
        free_memory_response_rows = free_memory_response.splitlines()
        
        # Split rows of free command into header/RAM/swap
        headers_str_list = free_memory_response_rows[0].split()
        ram_str_list = free_memory_response_rows[1].split()
        swap_str_list = free_memory_response_rows[2].split()
        
        # Build RAM output
        ram_int_list = (int(each_str) for each_str in ram_str_list[1:])
        ram_bytes_dict = dict(zip(headers_str_list, ram_int_list))
        
        # Build swap output
        swap_int_list = (int(each_str) for each_str in swap_str_list[1:])
        swap_bytes_dict = dict(zip(headers_str_list, swap_int_list))
        
        # Try to calculate a simple 'human-readable' value for output
        ram_percent_usage = int(round(100 * ram_bytes_dict["used"] / ram_bytes_dict["total"]))
        
    except (ValueError, IndexError, TypeError) as err:
        print("Error checking memory usage ({})".format(err.__class__.__name__))
        print(err)
    
    # End timing
    t_end = perf_counter()
    
    # Bundle returned data
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    return_result = {"ram_bytes": ram_bytes_dict,
                     "swap_bytes": swap_bytes_dict,
                     "ram_percent_usage": ram_percent_usage,
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_disk_usage_for_images(request):
    
    ''' Route which returns info regarding the current disk (HDD/SSD) usage '''
    
    # Start timing
    t_start = perf_counter()
    
    # Find the (full-drive) usage of the partition holding the image data
    base_image_path = build_base_image_pathing()
    total_bytes, used_bytes, free_bytes = disk_usage(base_image_path)
    
    # Calculate a simple 'human-readable' value for output
    percent_usage = int(round(100 * used_bytes / total_bytes))
    
    # End timing
    t_end = perf_counter()
    
    # Bundle returned data
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    return_result = {"total_bytes": total_bytes,
                     "used_bytes": used_bytes,
                     "free_bytes": free_bytes,
                     "percent_usage": percent_usage,
                     "note": "Usage for drive containing images only! May not account for metadata storage",
                     "time_taken_ms": time_taken_ms}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def get_document_count_tree(request):
    
    ''' Function which lists all database -> collection -> document counts '''
    
    # Loop over every collection of every camera and count all documents
    doc_count_tree = {}
    camera_names_list = sorted(get_camera_names_list(MCLIENT))
    for each_camera_name in camera_names_list:
        
        # Add each camera name to the tree
        doc_count_tree[each_camera_name] = {}
        
        # Loop over all collections for the given camera
        camera_collection_names_list = sorted(get_collection_names_list(MCLIENT, each_camera_name))
        for each_collection_name in camera_collection_names_list:
            
            # Get the document count for each collection
            collection_ref = MCLIENT[each_camera_name][each_collection_name]
            collection_document_count = collection_ref.count_documents({})
            
            # Store results nested by camera & collection name
            doc_count_tree[each_camera_name][each_collection_name] = collection_document_count
    
    return  UJSONResponse(doc_count_tree)

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
    
    # Get image pathing
    base_image_path = build_base_image_pathing()
    camera_image_folder_path = build_camera_image_path(base_image_path, camera_select)
    
    # Check if camera is in our list
    camera_names_before_list = get_camera_names_list(MCLIENT)
    camera_in_mongo_before = (camera_select in camera_names_before_list)
    camera_in_image_storage_before = (os.path.exists(camera_image_folder_path))
    camera_exists_before = (camera_in_mongo_before or camera_in_image_storage_before)
    
    # Wipe out entire camera database and image folder, if possible
    remove_camera_entry(MCLIENT, camera_select)
    rmtree(camera_image_folder_path, ignore_errors = True)
    
    # Check if the camera has been removed
    camera_names_after_list = get_camera_names_list(MCLIENT)
    camera_in_mongo_after = (camera_select in camera_names_after_list)
    camera_in_image_storage_after = (os.path.exists(camera_image_folder_path))
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
    
    # Delete image folder (but re-create an empty folder)
    base_image_path = build_base_image_pathing()
    images_removed = os.listdir(base_image_path)
    rmtree(base_image_path, ignore_errors = True)
    os.makedirs(base_image_path, exist_ok = True)
    
    # End timing
    t_end = perf_counter()
    time_taken_ms = calculate_time_taken_ms(t_start, t_end)
    
    # Build output for feedback
    return_result = {"data_removed": camera_names_list,
                     "images_removed": images_removed,
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
     Route("/get-all-camera-names", cameras_get_all_names),
     Route("/get-memory-usage", get_memory_usage),
     Route("/get-disk-usage", get_disk_usage_for_images),
     Route("/get-document-count-tree", get_document_count_tree),
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


