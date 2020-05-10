#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 17:04:16 2020

@author: eo
"""


# ---------------------------------------------------------------------------------------------------------------------
#%% Imports

import os

from collections import OrderedDict

from local.routes.posting import build_posting_routes
from local.routes.deleting import build_deleting_routes
from local.routes.logging import build_logging_routes
from local.routes.misc import build_help_route, build_misc_routes
from local.routes.camerainfo import build_camerainfo_routes
from local.routes.backgrounds import build_background_routes
from local.routes.objects import build_object_routes
from local.routes.snapshots import build_snapshot_routes
from local.lib.environment import get_debugmode, get_dbserver_protocol, get_dbserver_host, get_dbserver_port
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
    all_routes_dict["Camera Info"] = build_camerainfo_routes()
    all_routes_dict["Backgrounds"] = build_background_routes()
    all_routes_dict["Snapshots"] = build_snapshot_routes()
    all_routes_dict["Objects"] = build_object_routes()
    all_routes_dict["POSTing"] = build_posting_routes()
    all_routes_dict["Deleting"] = build_deleting_routes()
    all_routes_dict["Logging"] = build_logging_routes()
    
    # Convert to a list of routes for use in starlette init
    all_routes_list = []
    for each_route_list in all_routes_dict.values():
        all_routes_list += each_route_list
    
    # Build the help route using all routing info
    help_route = build_help_route(all_routes_dict)
    all_routes_list += [help_route]

    return all_routes_list

# .....................................................................................................................

def asgi_startup():
    pass

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Configure server

# Setup CORs and gzip responses
middleware = [Middleware(CORSMiddleware, allow_origins = ["*"], allow_methods = ["*"], allow_headers = ["*"]),
              Middleware(GZipMiddleware, minimum_size = 1500)]

# Initialize the asgi application
enable_debug_mode = get_debugmode()
all_routes_list = build_all_routes()
asgi_app = Starlette(debug = enable_debug_mode, 
                     routes = all_routes_list,
                     middleware = middleware, 
                     on_startup = [asgi_startup])


# ---------------------------------------------------------------------------------------------------------------------
#%% Launch (manually)

# This section of code only runs if using 'python3 launch.py'
# Better to use: 'uvicorn launch:asgi_app --host "0.0.0.0" --port 8050'
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


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap

# TODO
# - add mongo client connection cycling (script shouldn't just end if a connection isn't made!!)
# - consider using websockets? Esp. for posting/retrieving image data

