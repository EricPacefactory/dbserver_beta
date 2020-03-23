#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 17:04:16 2020

@author: eo
"""


# ---------------------------------------------------------------------------------------------------------------------
#%% Imports

import os

from local.lib.environment import get_debugmode_protocol, get_dbserver_protocol, get_dbserver_host, get_dbserver_port

from local.lib.timekeeper_utils import time_to_epoch_ms
from local.lib.mongo_helpers import connect_to_mongo, post_many_to_mongo
from local.lib.image_pathing import build_base_image_pathing, build_image_pathing
from local.lib.quitters import ide_catcher

from starlette.applications import Starlette
from starlette.responses import FileResponse, UJSONResponse, HTMLResponse
from starlette.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_405_METHOD_NOT_ALLOWED
from starlette.routing import Route
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

import pymongo
import uvicorn


# ---------------------------------------------------------------------------------------------------------------------
#%% Define classes

# .....................................................................................................................

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Response functions

# .....................................................................................................................

def first_of_query(query_result, return_if_missing = None):
    
    ''' 
    Helper function for dealing with mongodb queries that are expected to return 1 result in a 'list' format
    Also handles errors if there is no result
    '''
    
    try:
        return_result = next(query_result)
    except StopIteration:
        return_result = return_if_missing
    
    return return_result

# .....................................................................................................................

def no_data_response(error_message):
    
    ''' Helper function to respond when there is no data '''
    
    return UJSONResponse({"error": error_message}, status_code = HTTP_404_NOT_FOUND)

# .....................................................................................................................

def bad_request_response(error_message):
    
    ''' Helper function for bad requests '''
    
    return UJSONResponse({"error": error_message}, status_code = HTTP_400_BAD_REQUEST)

# .....................................................................................................................

def not_allowed_response(error_message):
    
    ''' Helper function for requests doing something they shouldn't (e.g. trying to save an existing entry) '''
    
    return UJSONResponse({"error": error_message}, status_code = HTTP_405_METHOD_NOT_ALLOWED)

# .....................................................................................................................

def post_success_response(success_message = True):
    
    ''' Helper function for post requests, when data is successfully added to the db '''
    
    return UJSONResponse({"success": success_message}, status_code = HTTP_201_CREATED)

# .....................................................................................................................

def server_startup():
    pass

# .....................................................................................................................
# .....................................................................................................................

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
    html_list = ["<title>DB Server</title>", 
                 "<h1><a href='/help'>Safety-cv-2 DB Server</a></h1>"]
    for each_camera_name in camera_names_list:
        caminfo_url = "/{}/camerainfo/get-newest-camera-info".format(each_camera_name)
        newest_image_url = "/{}/snapshots/get-newest-image".format(each_camera_name)
        img_html = "<a href='{}'><img src='{}' alt='Missing image data!'></a>".format(caminfo_url, newest_image_url)
        camera_html = "<h3>{}</h3>".format(each_camera_name.replace("_", " "))
        html_list += [camera_html, img_html, "<br><br>"]
    
    # Finally build the full html string to output
    html_resp = "\n".join(html_list)
    
    return HTMLResponse(html_resp)

# .....................................................................................................................

def is_alive_check(request):
    
    ''' Route used to check that this server is still up (before making a ton of requests for example) '''
    
    return UJSONResponse({"connection": True})

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
        html_list += ["", "<br><br>",
                      "<h3>Note:</h3>",
                      "  <p>If not specified, 'time' values can be provided in string or integer format</p>",
                      "  <p>--> String format times must follow isoformat</p>",
                      "  <p>--> Integer format times must be epoch millisecond values</p>"]
        
        # Finally build the full html string to output
        html_resp = "\n".join(html_list)
        
        return HTMLResponse(html_resp)
    
    # Build the output as a list, so it can be 'added' to other lists of routes more easily
    help_route_as_list = [Route("/help", help_page)]
    
    return help_route_as_list

# .....................................................................................................................

# Bundle all miscellaneous routes
misc_routes = \
[
 Route("/", root_page),
 Route("/get-all-camera-names", cameras_get_all_names),
 Route("/is-alive", is_alive_check)
]

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Create data posting routes

# .....................................................................................................................

def post_metadata(collection_name):
    
    '''
    Function used to create functions used for handling metadata POSTing
    Inputs:
        collection_name: (String) --> Specify the collection of metadata to handle. 
                                      Should be something like "snapshots", "objects", "camerainfo" etc.
    
    Outputs:
        post_metadata_by_collection (function)
        
        (The output should be used to create a Route object)
        (Ex: Route("/url/to/post/metadata", post_metadata_by_collection)
    '''
    
    async def post_metadata_by_collection(request):
        
        # Convert post data to python data
        post_data = await request.json()
        
        # Get the camera selection from the url path, and send off the data!
        camera_select = request.path_params["camera_select"]
        post_success = post_many_to_mongo(mclient, camera_select, collection_name, post_data)
        
        # Return an error response if there was a problem posting 
        # Hard-coded: assuming the issue is with duplicate entries
        if not post_success:
            error_message = "Error posting metadata. Entries likely exist already!"
            return not_allowed_response(error_message)
        
        return post_success_response()
    
    return post_metadata_by_collection

# .....................................................................................................................

def post_image(image_category):
    
    ''' 
    Function used to create functions used for handling image POSTing 
    Inputs:
        image_category: (String) --> Specify the category of image to handle. 
                                     Likely going to be either "snapshots" or "backgrounds"
    
    Outputs:
        post_image_by_category (function)
        
        (The output should be used to create a Route object)
        (Ex: Route("/url/to/post/image", post_image_by_category)
    '''

    async def post_image_by_category(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        image_epoch_ms = request.path_params["epoch_ms"]
        
        # Generate the image file pathing, so we can first make sure the image doesn't already exist
        image_save_path = build_image_pathing(IMAGE_FOLDER, camera_select, image_category, image_epoch_ms,
                                              create_folder_if_missing = True)
        
        # Return error if the image file has already been stored
        if os.path.exists(image_save_path):
            error_message = "Can't upload, image already exists ({})".format(image_epoch_ms)
            return not_allowed_response(error_message)
        
        # Get the image data from the post body
        image_data = await request.body()
        if not image_data:
            error_message = "No {} image data in body ({})".format(image_category, image_epoch_ms)
            return bad_request_response(error_message)
        
        # Save the data to the filesystem (not mongodb!)
        with open(image_save_path, "wb") as out_file:
            out_file.write(image_data)
        
        return post_success_response(image_epoch_ms)
    
    return post_image_by_category

# .....................................................................................................................

# Bundle all data posting routes
post_url = lambda post_route: "".join(["/{camera_select:str}", post_route])
post_data_routes = \
[
 Route(post_url("/bdb/metadata/camerainfo"), post_metadata("camerainfo"), methods=["POST"]),
 Route(post_url("/bdb/metadata/snapshots"), post_metadata("snapshots"), methods=["POST"]),
 Route(post_url("/bdb/metadata/objects"), post_metadata("objects"), methods=["POST"]),
 Route(post_url("/bdb/metadata/backgrounds"), post_metadata("backgrounds"), methods=["POST"]),
 Route(post_url("/bdb/image/snapshots/{epoch_ms:int}"), post_image("snapshots"), methods=["POST"]),
 Route(post_url("/bdb/image/backgrounds/{epoch_ms:int}"), post_image("backgrounds"), methods=["POST"])
]

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create camera info routes

# .....................................................................................................................

def caminfo_get_all_info(request):
    
    '''
    Returns all camera info for a given camera. This will include entries from every time the camera is reset.
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Build query
    query_dict = {}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["camerainfo"]
    query_result = collection_ref.find(query_dict, projection_dict)
    
    return UJSONResponse(list(query_result))

# .....................................................................................................................

def caminfo_get_newest_info(request):
    
    '''
    Returns the newest camera info entry for a specific camera.
    Note that this may not be the best thing to do if working with a specific alarm time!
    A better option is the 'relative' info entry, which will take into account camera reset events
    (and possible reconfiguration associated with those events)
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Build query
    target_field = "_id"
    query_dict = {}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["camerainfo"]
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, pymongo.DESCENDING).limit(1)
    
    # Pull out only the newest entry & handle missing data
    return_result = first_of_query(query_result)
    if return_result is None:
        error_message = "No camera info for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def caminfo_get_relative_info(request):
    
    ''' 
    Returns the camera info that was most applicable to the given target time
    For example, 
      If a camera started on 2020-01-01, ran until 2020-01-05, then was restarted so the snapshots could be resized.
      The camera would dump a camera info entry on 2020-01-01 and again on 2020-01-05 when it restarts.
      However, if something about the camera was changed in that time period (e.g. snapshot frame sizing),
      then alarms between 2020-01-01 and 2020-01-05 would need to reference 
      the 2020-01-01 camera info to get the correct information.
      Given an input time between 2020-01-01 to 2020-01-05, this function will return the info from 2020-01-01
    '''
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_time = int(target_time) if target_time.isnumeric() else target_time
    target_ems = time_to_epoch_ms(target_time)
    
    # Build query
    target_field = "_id"
    query_dict = {target_field: {"$lte": target_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["camerainfo"]
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, pymongo.DESCENDING).limit(1)
    
    # Try to get the newest data, from the given list (which may be empty!)
    return_result = first_of_query(query_result, return_if_missing = None)
    empty_query = (return_result is None)
    if empty_query:
        error_message = "No camera info before time {}".format(target_ems)
        return no_data_response(error_message)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

# Bundle all camera info routes
caminfo_url = lambda caminfo_route: "".join(["/{camera_select:str}/camerainfo", caminfo_route])
camerainfo_routes = \
[
 Route(caminfo_url("/get-all-camera-info"), caminfo_get_all_info),
 Route(caminfo_url("/get-newest-camera-info"), caminfo_get_newest_info),
 Route(caminfo_url("/get-relative-camera-info/{target_time}"), caminfo_get_relative_info),
]

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create image+metadata routes

# .....................................................................................................................

def get_newest_metadata(data_category):
    
    def inner_get_newest_metadata(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        
        # Build query
        target_field = "_id"
        query_dict = {}
        projection_dict = None
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]    
        query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, pymongo.DESCENDING).limit(1)
        
        # Pull out a single entry (there should only be one)
        return_result = first_of_query(query_result)
        if return_result is None:
            error_message = "No metadata for {}".format(camera_select)
            return no_data_response(error_message)
        
        return UJSONResponse(return_result)
    
    return inner_get_newest_metadata

# .....................................................................................................................

def get_newest_image(data_category):
    
    def inner_get_newest_image(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        
        # Build query
        target_field = "_id"
        query_dict = {}
        projection_dict = {}
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]    
        query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, pymongo.DESCENDING).limit(1)
        
        # Pull out a single entry (there should only be one), if possible
        return_result = first_of_query(query_result)
        if return_result is None:
            error_message = "No image data for {}".format(camera_select)
            return no_data_response(error_message)
        
        # Build pathing to the file
        newest_id = return_result[target_field]
        image_load_path = build_image_pathing(IMAGE_FOLDER, camera_select, data_category, newest_id)
        if not os.path.exists(image_load_path):
            error_message = "No image at {}".format(newest_id)
            return no_data_response(error_message)
        
        return FileResponse(image_load_path)
    
    return inner_get_newest_image

# .....................................................................................................................

def get_bounding_times(data_category):
    
    def inner_get_bounding_times(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        
        # Request data from the db
        target_field = "_id"
        collection_ref = mclient[camera_select][data_category]
        min_query = collection_ref.find().sort(target_field, pymongo.ASCENDING).limit(1)
        max_query = collection_ref.find().sort(target_field, pymongo.DESCENDING).limit(1)
        
        # Get results, if possible
        min_result = first_of_query(min_query)
        max_result = first_of_query(max_query)
        if (min_result is None) or (max_result is None):
            error_message = "No bounding times for {}".format(camera_select)
            return no_data_response(error_message)
        
        # Pull out only the timing info from the min/max entries to minimize the data being sent
        return_result = {"min_epoch_ms": min_result["epoch_ms"],
                         "max_epoch_ms": max_result["epoch_ms"],
                         "min_datetime_isoformat": min_result["datetime_isoformat"],
                         "max_datetime_isoformat": max_result["datetime_isoformat"]}
        
        return UJSONResponse(return_result)
    
    return inner_get_bounding_times

# .....................................................................................................................

def get_closest_epoch_by_time(data_category):
    
    def inner_get_closest_epoch_by_time(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_time = request.path_params["target_time"]
        target_time = int(target_time) if target_time.isnumeric() else target_time
        target_ems = time_to_epoch_ms(target_time)
        
        # Build query components for the upper/lower bounding entries
        target_field = "_id"
        lower_search_query = {target_field: {"$lte": target_ems}}
        upper_search_query = {target_field: {"$gte": target_ems}}
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        lower_query_result = collection_ref.find(lower_search_query).sort(target_field, pymongo.DESCENDING).limit(1)
        upper_query_result = collection_ref.find(upper_search_query).sort(target_field, pymongo.ASCENDING).limit(1)
        
        # Handle missing query values
        upper_result = first_of_query(upper_query_result)
        lower_result = first_of_query(lower_query_result)
        if (upper_result is None) and (lower_result is None):
            error_message = "No data for {}".format(camera_select)
            return no_data_response(error_message)
        
        # Pull out upper/lower bound epoch_ms values (if possible)
        upper_ems = None if (upper_result is None) else upper_result[target_field]
        lower_ems = None if (lower_result is None) else lower_result[target_field]
        
        # Determine the closest epoch value while handling missing values
        closest_ems = None
        if lower_ems is None:
            closest_ems = upper_ems
        elif upper_ems is None:
            closest_ems = lower_ems
        else:
            lower_diff = (target_ems - lower_ems)
            upper_diff = (upper_ems - target_ems)
            closest_ems = lower_ems if (lower_diff < upper_diff) else upper_ems
        
        # Bundle outputs
        return_result = {"upper_bound_epoch_ms": upper_ems,
                         "lower_bound_epoch_ms": lower_ems,
                         "closest_epoch_ms": closest_ems}
        
        return UJSONResponse(return_result)
    
    return inner_get_closest_epoch_by_time

# .....................................................................................................................

def get_epochs_by_time_range(data_category):
    
    def inner_get_epochs_by_time_range(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        start_time = request.path_params["start_time"]
        end_time = request.path_params["end_time"]
        
        # Convert epoch inputs to integers, if needed
        start_time = int(start_time) if start_time.isnumeric() else start_time
        end_time = int(end_time) if end_time.isnumeric() else end_time
        
        # Convert times to epoch values for db lookup
        start_ems = time_to_epoch_ms(start_time)
        end_ems = time_to_epoch_ms(end_time)
        
        # Build query
        target_field = "_id"
        query_dict = {target_field: {"$gte": start_ems, "$lt": end_ems}}
        projection_dict = {}
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, pymongo.ASCENDING)
        
        # Pull out the epoch values into a list, instead of returning a list of dictionaries
        return_result = [each_entry[target_field] for each_entry in query_result]
        
        return UJSONResponse(return_result)
    
    return inner_get_epochs_by_time_range

# .....................................................................................................................

def get_closest_metadata_by_time(data_category):
    
    def inner_get_closest_metadata_by_time(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_time = request.path_params["target_time"]
        target_time = int(target_time) if target_time.isnumeric() else target_time
        target_ems = time_to_epoch_ms(target_time)
        
        # Build aggregate query
        time_absdiff_cmd = {"$abs": {"$subtract": [target_ems, "$_id"]}}
        projection_cmd = {"$project": {"doc": "$$ROOT",  "time_absdiff": time_absdiff_cmd}}
        sort_cmd = {"$sort": {"time_absdiff": 1}}
        limit_cmd = {"$limit": 1}
        query_cmd_list = [projection_cmd, sort_cmd, limit_cmd]
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        query_result = collection_ref.aggregate(query_cmd_list)
        
        # Deal with missing data
        first_entry = first_of_query(query_result)
        if first_entry is None:
            error_message = "No closest metadata for {}".format(target_ems)
            return no_data_response(error_message)
        
        # If we get here we probably have data so return the document data except for the id
        return_result = first_entry["doc"]
        
        return UJSONResponse(return_result)

    return inner_get_closest_metadata_by_time

# .....................................................................................................................

def get_one_metadata(data_category):

    def inner_get_one_metadata(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_ems = request.path_params["epoch_ms"]
        
        # Build query
        query_dict = {"_id": target_ems}
        projection_dict = None
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        query_result = collection_ref.find_one(query_dict, projection_dict)
        
        # Deal with missing data
        if not query_result:
            error_message = "No metadata at {}".format(target_ems)
            return bad_request_response(error_message)
        
        return UJSONResponse(query_result)
    
    return inner_get_one_metadata

# .....................................................................................................................

def get_many_metadata(data_category):
    
    def inner_get_many_metadata(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        start_time = request.path_params["start_time"]
        end_time = request.path_params["end_time"]
        
        # Convert epoch inputs to integers, if needed
        start_time = int(start_time) if start_time.isnumeric() else start_time
        end_time = int(end_time) if end_time.isnumeric() else end_time
        
        # Convert times to epoch values for db lookup
        start_ems = time_to_epoch_ms(start_time)
        end_ems = time_to_epoch_ms(end_time)
        
        # Build query
        target_field = "_id"
        query_dict = {target_field: {"$gte": start_ems, "$lt": end_ems}}
        projection_dict = None
        
        # Request data from the db
        collection_ref = mclient[camera_select][data_category]
        query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, pymongo.ASCENDING)
        
        return UJSONResponse(list(query_result))
    
    return inner_get_many_metadata

# .....................................................................................................................

def get_one_image(data_category):
    
    def inner_get_one_image(request):
        
        # Get information from route url
        camera_select = request.path_params["camera_select"]
        target_ems = request.path_params["epoch_ms"]
        
        # Build pathing to the file
        image_load_path = build_image_pathing(IMAGE_FOLDER, camera_select, data_category, target_ems)
        if not os.path.exists(image_load_path):
            error_message = "No image at {}".format(target_ems)
            return bad_request_response(error_message)
        
        return FileResponse(image_load_path)
    
    return inner_get_one_image

# .....................................................................................................................

# Bundle all snapshot routes
snap_category = "snapshots"
snap_url = lambda snap_route: "".join(["/{camera_select:str}/snapshots", snap_route])
snapshot_routes = \
[
 Route(snap_url("/get-newest-metadata"), get_newest_metadata(snap_category)),
 Route(snap_url("/get-newest-image"), get_newest_image(snap_category)),
 Route(snap_url("/get-bounding-times"), get_bounding_times(snap_category)),
 Route(snap_url("/get-closest-ems/by-time-target/{target_time}"), get_closest_epoch_by_time(snap_category)),
 Route(snap_url("/get-ems-list/by-time-range/{start_time}/{end_time}"), get_epochs_by_time_range(snap_category)),
 Route(snap_url("/get-closest-metadata/by-time-target/{target_time}"), get_closest_metadata_by_time(snap_category)),
 Route(snap_url("/get-one-metadata/by-ems/{epoch_ms:int}"), get_one_metadata(snap_category)),
 Route(snap_url("/get-many-metadata/by-time-range/{start_time}/{end_time}"), get_many_metadata(snap_category)),
 Route(snap_url("/get-one-image/by-ems/{epoch_ms:int}"), get_one_image(snap_category))
]

# .....................................................................................................................

# Bundle all background routes
bg_category = "backgrounds"
bg_url = lambda bg_route: "".join(["/{camera_select:str}/backgrounds", bg_route])
background_routes = \
[
 Route(bg_url("/get-newest-metadata"), get_newest_metadata(bg_category)),
 Route(bg_url("/get-newest-image"), get_newest_image(bg_category)),
 Route(bg_url("/get-bounding-times"), get_bounding_times(bg_category)),
 Route(bg_url("/get-closest-ems/by-time-target/{target_time}"), get_closest_epoch_by_time(bg_category)),
 Route(bg_url("/get-ems-list/by-time-range/{start_time}/{end_time}"), get_epochs_by_time_range(bg_category)),
 Route(bg_url("/get-closest-metadata/by-time-target/{target_time}"), get_closest_metadata_by_time(bg_category)),
 Route(bg_url("/get-one-metadata/by-ems/{epoch_ms:int}"), get_one_metadata(bg_category)),
 Route(bg_url("/get-many-metadata/by-time-range/{start_time}/{end_time}"), get_many_metadata(bg_category)),
 Route(bg_url("/get-one-image/by-ems/{epoch_ms:int}"), get_one_image(bg_category))
]

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Create object routes

# .....................................................................................................................

def objects_get_newest_metadata(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    
    # Build query
    target_field = "_id"
    query_dict = {}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, pymongo.DESCENDING).limit(1)
    
    # Pull out a single entry (there should only be one)
    return_result = first_of_query(query_result)
    if return_result is None:
        error_message = "No object metadata for {}".format(camera_select)
        return no_data_response(error_message)
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_ids_at_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_time = int(target_time) if target_time.isnumeric() else target_time
    target_ems = time_to_epoch_ms(target_time)
    
    # Build query
    target_field = "_id"
    query_dict = {"first_epoch_ms": {"$lte": target_ems}, "final_epoch_ms": {"$gte": target_ems}}
    projection_dict = {}
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict)
    
    # Convert to list of ids only
    return_result = [each_entry[target_field] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_ids_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert epoch inputs to integers, if needed
    start_time = int(start_time) if start_time.isnumeric() else start_time
    end_time = int(end_time) if end_time.isnumeric() else end_time
    
    # Convert times to epoch values for db lookup
    start_ems = time_to_epoch_ms(start_time)
    end_ems = time_to_epoch_ms(end_time)
    
    # Build query
    target_field = "_id"
    query_dict = {"first_epoch_ms": {"$lt": end_ems}, "final_epoch_ms": {"$gt": start_ems}}
    projection_dict = {}
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict).sort(target_field, pymongo.ASCENDING)
    
    # Pull out the epoch values into a list, instead of returning a list of dictionaries
    return_result = [each_entry[target_field] for each_entry in query_result]
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_one_metadata_by_id(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    object_full_id = int(request.path_params["object_full_id"])
    
    # Build query
    query_dict = {"_id": object_full_id}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find_one(query_dict, projection_dict)
    
    # Deal with missing data
    if not query_result:
        error_message = "No object with id {}".format(object_full_id)
        return bad_request_response(error_message)
    
    return UJSONResponse(query_result)

# .....................................................................................................................

def objects_get_many_metadata_at_time(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    target_time = request.path_params["target_time"]
    target_time = int(target_time) if target_time.isnumeric() else target_time
    target_ems = time_to_epoch_ms(target_time)
    
    # Build query
    query_dict = {"first_epoch_ms": {"$lte": target_ems}, "final_epoch_ms": {"$gte": target_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict)
    
    # Convert to dictionary, with object ids as keys
    filter_key = "full_id"
    return_result = {each_result[filter_key]: each_result for each_result in query_result}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

def objects_get_many_metadata_by_time_range(request):
    
    # Get information from route url
    camera_select = request.path_params["camera_select"]
    start_time = request.path_params["start_time"]
    end_time = request.path_params["end_time"]
    
    # Convert epoch inputs to integers, if needed
    start_time = int(start_time) if start_time.isnumeric() else start_time
    end_time = int(end_time) if end_time.isnumeric() else end_time
    
    # Convert times to epoch values for db lookup
    start_ems = time_to_epoch_ms(start_time)
    end_ems = time_to_epoch_ms(end_time)
    
    # Build query
    query_dict = {"first_epoch_ms": {"$lt": end_ems}, "final_epoch_ms": {"$gt": start_ems}}
    projection_dict = None
    
    # Request data from the db
    collection_ref = mclient[camera_select]["objects"]
    query_result = collection_ref.find(query_dict, projection_dict)
    
    # Convert to dictionary, with object ids as keys
    filter_key = "full_id"
    return_result = {each_result[filter_key]: each_result for each_result in query_result}
    
    return UJSONResponse(return_result)

# .....................................................................................................................

# Bundle all object routes
obj_url = lambda obj_route: "".join(["/{camera_select:str}/objects", obj_route])
object_routes = \
[
 Route(obj_url("/get-newest-metadata"), objects_get_newest_metadata),
 Route(obj_url("/get-ids-list/by-time-target/{target_time}"), objects_get_ids_at_time),
 Route(obj_url("/get-ids-list/by-time-range/{start_time}/{end_time}"), objects_get_ids_by_time_range),
 Route(obj_url("/get-one-metadata/by-id/{object_full_id:int}"), objects_get_one_metadata_by_id),
 Route(obj_url("/get-many-metadata/by-time-target/{target_time}"), objects_get_many_metadata_at_time),
 Route(obj_url("/get-many-metadata/by-time-range/{start_time}/{end_time}"), objects_get_many_metadata_by_time_range),
]

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Configure server

# Establish (global!) variable used to access the persistent image folder
IMAGE_FOLDER = build_base_image_pathing(__file__)

# Start connection to mongoDB
mclient = connect_to_mongo()

# Setup CORs and gzip responses
middleware = [Middleware(CORSMiddleware, allow_origins=["*"]),
              Middleware(GZipMiddleware, minimum_size = 1500)]

# Create the help route
help_route_as_list = build_help_route(["Miscellaneous", misc_routes],
                                      ["Camera Info", camerainfo_routes],
                                      ["Backgrounds", background_routes],
                                      ["Snapshots", snapshot_routes],
                                      ["Objects", object_routes],
                                      ["POSTing", post_data_routes])

# Gather all the routes
all_routes = post_data_routes + misc_routes + help_route_as_list \
             + camerainfo_routes + background_routes + snapshot_routes + object_routes

# Initialize the server
enable_debug_mode = get_debugmode_protocol()
server = Starlette(debug = enable_debug_mode, 
                   routes = all_routes, 
                   middleware = middleware, 
                   on_startup = [server_startup])


# ---------------------------------------------------------------------------------------------------------------------
#%% Launch (manually)

# This section of code only runs if using 'python3 launch.py'
# Better to use: 'uvicorn launch:server --host "0.0.0.0" --port 8050'
if __name__ == "__main__":
    
    # Prevent this script from launching the server inside of IDE (spyder)
    ide_catcher("Can't run server from IDE. Use a terminal!")
    
    # Pull environment variables, if possible
    dbserver_protocol = get_dbserver_protocol()
    dbserver_host = get_dbserver_host()
    dbserver_port = get_dbserver_port()
    
    # Get the "filename:varname" command needed by uvicorn
    file_name_only, _ = os.path.splitext(os.path.basename(__file__))
    server_variable_name = "server"
    app_command = "{}:{}".format(file_name_only, server_variable_name)
    
    # Unleash the server!
    uvicorn.run(app_command, host = dbserver_host, port = dbserver_port)


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap

# TODO
# - setup to work within docker
# - add mongo client connection cycling (script shouldn't just end if a connection isn't made!!)
# - split routing functions into separate files for clarity (difficult due to use of shared functions/mongoclient...)
# - consider using websockets? Esp. for posting image data

