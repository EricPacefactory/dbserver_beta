# -----------------------------------------------------------------------------
#%% Base setup

# Start ubuntu-python base
FROM pybase_image


# -----------------------------------------------------------------------------
#%% Compatibility

# Needed for 'click' library to work properly
ENV LC_ALL                          C.UTF-8
ENV LANG                            C.UTF-8


# -----------------------------------------------------------------------------
#%% Set environment variables

# Set environment variables for mongo db access
ENV MONGO_HOST                      localhost
ENV MONGO_PORT                      27017

# Set environment variables for db server config
ENV DEBUG_MODE                      0
ENV DBSERVER_HOST                   0.0.0.0
ENV DBSERVER_PORT                   8050

# Set image saving path (i.e. persistent storage)
ENV DBSERVER_IMAGE_FOLDER_PATH      $HOME/db_images


# -----------------------------------------------------------------------------
#%% Setup python 

# Install python requirements
COPY requirements.txt $HOME/realtime
RUN pip3 install -r requirements.txt


# -----------------------------------------------------------------------------
#%% Launch!

# Move system files into the image
WORKDIR $HOME/realtime
COPY . .

# Run the db server! This is a blocking call...
CMD uvicorn launch:asgi_app --host $DBSERVER_HOST --port $DBSERVER_PORT


# -----------------------------------------------------------------------------
# To use manually:

# sudo docker build -t dbserver_image .
# sudo docker run -d --network="host" -v /tmp/db_images:/home/scv2/db_images --name dbserver_container dbserver_image

