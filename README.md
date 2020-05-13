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

## Docker (manual use)

The dbserver can be (manually) launched through docker using the following instructions.

#### Build:

From inside the dbserver folder:

`docker build -t dbserver_image -f ./build/docker/Dockerfile .`

This command will create a docker image (called dbserver_image) with all dependencies installed.

#### Run:

From anywhere:

```
docker run -d \
--network="host" \
-v /tmp/images_dbserver:/home/scv2/images_dbserver \
--name dbserver \
dbserver_image
```

This command will start up a container running the dbserver. The easiest way to confirm the system is running is by going to the dbserver url (default: `localhost:8050`). 

Note that MongoDB should be running beforehand, since the dbserver will try to connect on startup! 

Also note that the run command above will map persistent data into a temporary folder (`/tmp/images_dbserver`), this may be fine for testing/experimentation, but beware of data loss.

---

## Environment variables

`DBSERVER_IMAGE_FOLDER_PATH` = (none, defaults to the project root folder)

`MONGO_PROTOCOL` = mongodb

`MONGO_HOST` = localhost

`MONGO_PORT` = 27017

`DBSERVER_PROTOCOL` = http

`DBSERVER_HOST` = 0.0.0.0

`DBSERVER_PORT` = 8050

`DEBUG_MODE` = 1

---



## Major TODOs

- Document environment variables (and update defaults?)

- Add cycling mongo connection attempts to all mongo calls

- Look into optimizing routes/mongo calls

- Look into async + pymongo (isn't supported?)

- Consider websockets for uploading/requesting image data
