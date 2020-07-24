#! /bin/sh

# Command to run when launching as a docker container (Blocking!)
exec uvicorn launch:asgi_app --host $DBSERVER_HOST --port $DBSERVER_PORT

