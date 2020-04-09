#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 17:04:16 2020

@author: eo
"""


# ---------------------------------------------------------------------------------------------------------------------
#%% Imports

import os

from local.routes.posting import build_posting_routes
from local.routes.misc import build_help_route, build_misc_routes
from local.routes.objects import build_object_routes
from local.routes.camerainfo import build_camerainfo_routes
from local.routes.image_based import build_background_routes, build_snapshot_routes
from local.lib.environment import get_debugmode, get_dbserver_protocol, get_dbserver_host, get_dbserver_port
from local.lib.quitters import ide_catcher

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware


# ---------------------------------------------------------------------------------------------------------------------
#%% Define functions

# .....................................................................................................................

def asgi_startup():
    pass

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Create routes

# Create all the working routes
posting_routes = build_posting_routes()
misc_routes = build_misc_routes()
camerainfo_routes = build_camerainfo_routes()
background_routes = build_background_routes()
snapshot_routes = build_snapshot_routes()
object_routes = build_object_routes()

# Create the help route for listing simple documentation
help_route_as_list = build_help_route(["Miscellaneous", misc_routes],
                                      ["Camera Info", camerainfo_routes],
                                      ["Backgrounds", background_routes],
                                      ["Snapshots", snapshot_routes],
                                      ["Objects", object_routes],
                                      ["POSTing", posting_routes])

# Gather all the routes to pass to the asgi application
all_routes = posting_routes + misc_routes + help_route_as_list \
             + camerainfo_routes + background_routes + snapshot_routes + object_routes


# ---------------------------------------------------------------------------------------------------------------------
#%% Configure server

# Setup CORs and gzip responses
middleware = [Middleware(CORSMiddleware, allow_origins = ["*"], allow_methods = ["*"], allow_headers = ["*"]),
              Middleware(GZipMiddleware, minimum_size = 1500)]

# Initialize the asgi application
enable_debug_mode = get_debugmode()
asgi_app = Starlette(debug = enable_debug_mode, 
                     routes = all_routes, 
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
# - setup to work within docker
# - add mongo client connection cycling (script shouldn't just end if a connection isn't made!!)
# - consider using websockets? Esp. for posting/retrieving image data

