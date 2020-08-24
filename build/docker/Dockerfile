# -----------------------------------------------------------------------------
#%% Base setup

# Start with ubuntu-python base
FROM pacefactory/scv2_ubuntupython_base:1.0.2


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

# Set environment variables for mongo db access
ENV MONGO_HOST                      localhost
ENV MONGO_PORT                      27017

# Set environment variables for db server config
ENV DEBUG_MODE                      0
ENV DBSERVER_HOST                   0.0.0.0
ENV DBSERVER_PORT                   8050

# Set image saving path (i.e. persistent storage)
ENV DBSERVER_IMAGE_FOLDER_PATH      $HOME/images_dbserver


# -----------------------------------------------------------------------------
#%% Launch!

# Work with files outside of the root fs
WORKDIR $HOME/dbserver

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
# docker run -d --network="host" -v /tmp/images_dbserver:/home/scv2/images_dbserver --name dbserver dbserver_image

