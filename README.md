# SCV2 dbserver

(Only tested on: Linux Mint 19.1 Tessa, Python 3.6.7)

## Requirements

No system requirements! Everything is in python. However, this server expects a MongoDB database to be running (and accessible through the network) in order to function correctly.

To install, use:

`pip3 install -r requirements`

This should be called from the root project folder. 

You probably want to use a virtual environment for this, unless installing into docker!

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



## Use with Docker (manually)

Note that the `pybase` docker image is needed to build the dbserver image (see the scv2_deploy files for the pybase image).

To build the docker image, navigate one level outside of the dbserver project root folder. Assuming the folder is called `dbserver`, use the following command:

`sudo docker build -t dbserver_image -f ./dbserver/build/docker/Dockerfile ./dbserver`

To run the dbserver image in a container, use the following command:

`sudo docker run -d --network="host" -v /tmp/images_dbserver:/images_dbserver --name dbserver_container dbserver_image`

This should launch the dbserver on `http:localhost:8050`

Note that the run command assumes the persistent data (images) will be stored in `/tmp/images_dbserver`

This  mapping means that data will eventually be deleted automatically by your operating system (on reboot?). While this is convenient for quick tests, a more permanent folder path should be used if the data is not meant to be lost! However, if data persistance isn't an issue and you don't need to be able to check the image data directly, the entire volume mapping command can be left out (i.e. delete the `-v /tmp/images_dbserver:/images_dbserver` part of the command).

## Major TODOs

- Document environment variables (and update defaults?)

- Add cycling mongo connection attempts to all mongo calls

- Look into optimizing routes/mongo calls

- Look into async + pymongo (isn't supported?)

- Consider websockets for uploading/requesting image data
