# -----------------------------------------------------------------------------
#%% Base setup

# Start with ubuntu-python base
FROM pacefactory/scv2_ubuntupython_base:1.0.3


# -----------------------------------------------------------------------------
#%% Setup python requirements

# Install python requirements
COPY requirements.txt /tmp/scv2/requirements.txt
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r /tmp/scv2/requirements.txt


# -----------------------------------------------------------------------------
#%% Set environment variables

# Create a 'home' folder path to avoid storing everything in the root fs
ENV HOME                            /home/scv2
ENV DOCKER_IMAGE_PATH               $HOME/from_docker
ENV DOCKER_VOLUME_PATH              $HOME/volume

# Set up persistent storage pathing
ENV DBSERVER_CODE_FOLDER_PATH       $DOCKER_VOLUME_PATH/code
ENV DBSERVER_DATA_FOLDER_PATH       $DOCKER_VOLUME_PATH/data

# Set environment variables for MongoDB access
ENV MONGO_HOST                      localhost
ENV MONGO_PORT                      27017

# Set environment variables for dbserver config
ENV DEBUG_MODE                      0
ENV DBSERVER_HOST                   0.0.0.0
ENV DBSERVER_PORT                   8050

# Set up autodelete config
ENV DELETE_ON_STARTUP               0
ENV HOUR_TO_RUN                     1
ENV UPPER_DISK_USAGE_PCT            85
ENV MAX_DISK_USAGE_PCT              70
ENV DAYS_TO_KEEP                    10


# -----------------------------------------------------------------------------
#%% Launch!

# Work with files outside of the root fs
WORKDIR $DOCKER_IMAGE_PATH

# Move system files into the image
COPY . .

# Make sure the entrypoint script is executable
RUN chmod +x docker_entrypoint.sh

# Run the server! This is a blocking call...
ENTRYPOINT ["./docker_entrypoint.sh"]


# -----------------------------------------------------------------------------
# To use manually:

# From the project root directory:
# docker build -t dbserver_image -f ./build/docker/Dockerfile .
# docker run -d --network="host" -v /tmp/dbserver:/home/scv2/volume --name dbserver dbserver_image

