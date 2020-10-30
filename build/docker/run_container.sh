#!/bin/bash

# -------------------------------------------------------------------------
# Set image-specific variables

# Set naming
image_name="dbserver_image"
container_name="dbserver"

# Set networking
network_setting="host"

# Set volume pathing
host_volume_path="$HOME/scv2/volumes/$container_name"
container_volume_path="/home/scv2/volume"


# -------------------------------------------------------------------------
# Get environment variables from script args

# Get script arguments for env variables
env_vars=""
if [[ $1 ]]
then
  # Build docker environment argument
  for arg in $@; do
    env_vars+="-e $arg "
  done
  
  # Some feedback about environment settings
  echo ""
  echo "Got environment variables:"
  echo "$env_vars"
fi


# -------------------------------------------------------------------------
# Prompt to force container to always restart

# Assume we always restart containers, but allow disabling
container_restart="always"
echo ""
read -p "Enable container auto-restart? ([y]/n) " user_response
case "$user_response" in
  n|N ) echo "  --> Auto-restart disabled!"; container_restart="no";;
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
           $env_vars \
           --network=$network_setting \
           -v $host_volume_path:$container_volume_path \
           --name $container_name \
           --restart $container_restart \
           $image_name \
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


