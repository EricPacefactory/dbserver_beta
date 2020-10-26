#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 17:04:16 2020

@author: eo
"""


# ---------------------------------------------------------------------------------------------------------------------
#%% Imports

import os
import signal

from collections import OrderedDict

from local.lib.mongo_helpers import MCLIENT
from local.lib.data_deletion import AD_SHUTDOWN_EVENT, create_parallel_scheduled_delete

from local.routes.posting import build_posting_routes
from local.routes.deleting import build_deleting_routes
from local.routes.logging import build_logging_routes
from local.routes.misc import build_help_route, build_misc_routes
from local.routes.diagnostics import build_diagnostics_routes
from local.routes.uinotes import build_uinotes_routes
from local.routes.uistore import build_uistore_routes
from local.routes.camerainfo import build_camerainfo_routes
from local.routes.configinfo import build_configinfo_routes
from local.routes.backgrounds import build_background_routes
from local.routes.objects import build_object_routes
from local.routes.favorites import build_favorite_routes
from local.routes.stations import build_station_routes
from local.routes.snapshots import build_snapshot_routes
from local.routes.websockets import build_websocket_routes
from local.routes.autodelete import build_autodeleting_routes

from local.lib.environment import get_debugmode, get_dbserver_protocol, get_dbserver_host, get_dbserver_port
from local.lib.timekeeper_utils import timestamped_log
from local.lib.response_helpers import get_exception_handlers
from local.lib.quitters import ide_catcher

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware


# ---------------------------------------------------------------------------------------------------------------------
#%% Define functions

# .....................................................................................................................

def build_all_routes():
    
    # Bundle all routes in order, with titles used to group routes on the help page
    all_routes_dict = OrderedDict()
    all_routes_dict["Miscellaneous"] = build_misc_routes()
    all_routes_dict["Diagnostics"] = build_diagnostics_routes()
    all_routes_dict["UI Notes"] = build_uinotes_routes()
    all_routes_dict["UI Storage"] = build_uistore_routes()
    all_routes_dict["Camera Info"] = build_camerainfo_routes()
    all_routes_dict["Configuration Info"] = build_configinfo_routes()
    all_routes_dict["Backgrounds"] = build_background_routes()
    all_routes_dict["Snapshots"] = build_snapshot_routes()
    all_routes_dict["Objects"] = build_object_routes()
    all_routes_dict["Favorites"] = build_favorite_routes()
    all_routes_dict["Stations"] = build_station_routes()
    all_routes_dict["Websockets"] = build_websocket_routes()
    all_routes_dict["POSTing"] = build_posting_routes()
    all_routes_dict["Deleting"] = build_deleting_routes()
    all_routes_dict["Autodelete"] = build_autodeleting_routes()
    all_routes_dict["Server Logs"] = build_logging_routes()
    
    # Convert to a list of routes for use in starlette init
    all_routes_list = []
    for each_route_list in all_routes_dict.values():
        all_routes_list += each_route_list
    
    # Build the help route using all routing info
    help_route = build_help_route(all_routes_dict)
    all_routes_list += [help_route]

    return all_routes_list

# .....................................................................................................................

def register_shutdown_command():
    
    ''' Awkward hack to get starlette server to close on SIGTERM signals '''
    
    def convert_sigterm_to_keyboard_interrupt(signal_number, stack_frame):
        
        # Some feedback about catching kill signal
        print("", "", "*" * 48, "Kill signal received! ({})".format(signal_number), "*" * 48, "", sep = "\n")
        
        # Raise a keyboard interrupt, which starlette will respond to! (unlike SIGTERM)
        raise KeyboardInterrupt
    
    signal.signal(signal.SIGTERM, convert_sigterm_to_keyboard_interrupt)
    
    return

# .....................................................................................................................

def asgi_startup():
    
    # Speed up shutdown when calling 'docker stop ...'
    register_shutdown_command()
    
    # Some feedback, mostly for docker logs
    start_msg = timestamped_log("Started dbserver!")
    print("", start_msg, sep = "\n", flush = True)
    
    # Create parallel process which handle auto deletion of data over time
    parallel_proc_pid, _ = create_parallel_scheduled_delete(MCLIENT, log_to_file = True, start_on_call = True)
    if parallel_proc_pid is not None:
        print("--> Started parallel autodelete process (PID: {})".format(parallel_proc_pid), flush = True)
    
    return

# .....................................................................................................................

def asgi_shutdown():
    
    # Some feedback, mostly for docker logs
    stop_msg = timestamped_log("Stopping dbserver!")
    print("", stop_msg, sep = "\n", flush = True)
    
    # Close the (global!) mongo connection
    MCLIENT.close()
    
    # Shutdown autodelete process
    AD_SHUTDOWN_EVENT.set()
    
    return

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Shared setup

# Determine if we're running in special debug mode (which can enable/disable certain features)
enable_debug_mode = get_debugmode()

# Set up mongo-disconnect error handling
exception_handlers = get_exception_handlers()


# ---------------------------------------------------------------------------------------------------------------------
#%% Configure server

# Setup CORs and gzip responses
middleware = [Middleware(CORSMiddleware, allow_origins = ["*"], allow_methods = ["*"], allow_headers = ["*"]),
              Middleware(GZipMiddleware, minimum_size = 1500)]

# Set up mongo-disconnect error handling
exception_handlers = get_exception_handlers()

# Initialize the asgi application
all_routes_list = build_all_routes()
asgi_app = Starlette(debug = enable_debug_mode, 
                     routes = all_routes_list,
                     middleware = middleware, 
                     on_startup = [asgi_startup],
                     on_shutdown = [asgi_shutdown],
                     exception_handlers = exception_handlers)


# ---------------------------------------------------------------------------------------------------------------------
#%% Launch (manually)

# This section of code only runs if using 'python3 launch.py'
# Better to use: 'uvicorn launch:asgi_app --host "0.0.0.0" --port 8050 --log-level "warning"'
if __name__ == "__main__":
    
    # Only import this if we need to use it...
    import uvicorn
    
    # Prevent this script from launching the server inside of IDE (spyder)
    ide_catcher("Can't run server from IDE. Use a terminal!")
    
    # Pull environment variables, if possible
    dbserver_protocol = get_dbserver_protocol()
    dbserver_host = get_dbserver_host()
    dbserver_port = get_dbserver_port()
    
    # Get the "filename:varname" command needed by uvicorn
    file_name_only, _ = os.path.splitext(os.path.basename(__file__))
    asgi_variable_name = "asgi_app"
    import_command = "{}:{}".format(file_name_only, asgi_variable_name)
    
    # Unleash the server!
    uvicorn.run(import_command, host = dbserver_host, port = dbserver_port)
    
    # Shutdown the connection when running as main
    MCLIENT.close()


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap

# TODO
# - consider using websockets? Esp. for posting image data
#   -> May instead want to post data in larger packs? e.g. save/upload as tar files???

