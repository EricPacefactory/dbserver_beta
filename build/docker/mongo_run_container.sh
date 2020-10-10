#!/bin/bash

# -------------------------------------------------------------------------
# Set image-specific variables

# Set naming
image_name="mongo:4.2.3-bionic"
container_name="mongo"

# Set networking
host_port="27017"
container_port="27017"

# Set volume pathing
host_volume_path="$HOME/scv2/volumes/$container_name"
container_volume_path="/data/db"


# -------------------------------------------------------------------------
# Prompt to make sure we actually want to run mongo

echo ""
echo "WARNING:"
echo "  Restarting Mongo will cause (temporary) performance isssues"
echo "  This script should only be run when first setting up MongoDB,"
echo "  or if some significant update is being made to the container"
echo ""
read -p "Are you sure you want to continue? (y/[n]) " user_response
case "$user_response" in
  y|Y ) echo "  --> Confirmed launch/restart!";;
  * ) echo "  --> Cancelling..."; echo ""; exit;;
esac


# -------------------------------------------------------------------------
# Prompt to force container to always restart

# Assume we always restart containers, but allow disabling
container_restart="always"
echo ""
read -p "Enable container auto-restart? ([y]/n) " user_response
case "$user_response" in
  n|N ) echo "  --> Auto-restart disabled!"; echo ""; container_restart="no";;
  * ) echo "  --> Enabling auto-restart!";;
esac


# -------------------------------------------------------------------------
# Automated commands

# Some feedback while stopping the container
echo ""
echo "Stopping existing container..."
docker stop $container_name > /dev/null 2>&1
echo "  --> Success!"

# Some feedback while removing the existing container
echo ""
echo "Removing existing container..."
docker rm $container_name > /dev/null 2>&1
echo "  --> Success!"

# Now run the container
echo ""
echo "Running container ($container_name)"
docker run -d \
           -p $host_port:$container_port \
           -v $host_volume_path:$container_volume_path \
           --name $container_name \
           --restart $container_restart \
           $image_name \
           --quiet \
           --slowms 200 \
           > /dev/null
echo "  --> Success!"

# Ask about removing unused images
echo ""
read -p "Remove unused images? (y/[n]) " user_response
case "$user_response" in
  y|Y ) echo "  --> Removing images..."; echo ""; docker image prune;;
  * ) echo "  --> Not removing images";;
esac

# Some final feedback
echo ""
echo "-----------------------------------------------------------------"
echo ""
echo "To check the status of all running containers use:"
echo "docker ps -a"
echo ""
echo "To stop this container use:"
echo "docker stop $container_name"
echo ""
echo "To 'enter' into the container (for debugging/inspection) use:"
echo "docker exec -it $container_name bash"
echo ""
echo "-----------------------------------------------------------------"
echo ""


