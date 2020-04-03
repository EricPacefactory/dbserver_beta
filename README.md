# SCV2 dbserver

(Only tested on: Linux Mint 19.1 Tessa, Python 3.6.7)

## Requirements

No system requirements! Everything is in python. To install, use:

`pip3 install -r requirements`

This should be called from the root project folder. You probably want to use a virtual environment for this, unless installing into docker!

## Usage

This isn't meant to be interactive, simply launch the server and walk away! 

The following command can be used to launch the server after installing requirements:

`uvicorn launch:asgi_app --host "0.0.0.0" --port 8050`

This should be called from the project root folder. Note that the launch script can be called using python as well, but the uvicorn call is recommended.

## Configuration

There are a number of environment variables that can be used to alter the default configuration of the server. These are as follows:

`DBSERVER_IMAGE_FOLDER_PATH` (default: None)

`MONGO_PROTOCOL` (default: mongodb)

`MONGO_HOST` (default: localhost)

`MONGO_PORT` (default: 27017)

`DBSERVER_PROTOCOL` (default: http)

`DBSERVER_HOST` (default: 0.0.0.0)

`DBSERVER_PORT` (default 8050)

`DEBUG_MODE` (default: 1)

## Major TODOs

- Document environment variables (and update defaults?)

- Clean up/separate routing functions

- Add cycling mongo connection attempts

- Look into using pymongo find_one(...) function to optimize certain mongo calls

- Look into async + pymongo (isn't supported?)

- Consider websockets for uploading/requesting image data
