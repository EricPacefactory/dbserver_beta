#!/bin/bash

# -------------------------------------------------------------------------
# Set image-specific variables

# Set image name
image_name="dbserver_image"


# -------------------------------------------------------------------------
# Figure out pathing

# Get shared pathing info
this_script_relative_path=$0
this_script_full_path=$(realpath $this_script_relative_path)
docker_folder_path=$(dirname $this_script_full_path)
build_folder_path=$(dirname $docker_folder_path)

# Get important paths
root_project_folder_path=$(dirname $build_folder_path)
build_name=$(basename $root_project_folder_path)
dockerfile_path="$docker_folder_path/Dockerfile"


# -------------------------------------------------------------------------
# Prompt to run git pull

# Assume no by default, since we don't want to modify files accidentally
echo ""
read -p "Run git pull before build? (y/[n]) " user_response
case "$user_response" in
  y|Y ) pushd $root_project_folder_path > /dev/null; git pull; popd > /dev/null;;
  * ) ;;
esac


# -------------------------------------------------------------------------
# Build new image

# Some feedback
echo ""
echo "*** $build_name ***"
echo "Building from dockerfile:"
echo "@ $dockerfile_path"
echo ""
echo ""

# Actual build command
docker build -t $image_name -f $dockerfile_path $root_project_folder_path


