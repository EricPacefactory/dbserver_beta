# -----------------------------------------------------------------------------
#%% Base setup

# Start with ubuntu-python base
FROM pacefactory/scv2_ubuntupython_base:1.0.0


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
#%% Setup python

# Work with files outside of the root fs
WORKDIR $HOME/dbserver

# Install python requirements
COPY requirements.txt $HOME/dbserver
RUN pip3 install -r $HOME/dbserver/requirements.txt


# -----------------------------------------------------------------------------
#%% Launch!

# Move system files into the image
COPY . .

# Run the server! This is a blocking call...
CMD uvicorn launch:asgi_app --host $DBSERVER_HOST --port $DBSERVER_PORT


# -----------------------------------------------------------------------------
# To use manually:

# From outside dbserver project directory:
# sudo docker build -t dbserver_image -f ./dbserver/build/docker/Dockerfile ./dbserver
# sudo docker run -d --network="host" -v /tmp/images_dbserver:/home/scv2/images_dbserver --name dbserver_container dbserver_image

