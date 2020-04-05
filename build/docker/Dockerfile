# -----------------------------------------------------------------------------
#%% Base setup

# Start ubuntu-python base
FROM pybase_image


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
ENV DBSERVER_IMAGE_FOLDER_PATH      $HOME/images_dbserver


# -----------------------------------------------------------------------------
#%% Setup python 

# Install python requirements
COPY requirements.txt $HOME/dbserver
RUN pip3 install -r requirements.txt


# -----------------------------------------------------------------------------
#%% Launch!

# Move system files into the image
WORKDIR $HOME/dbserver
COPY . .

# Run the server! This is a blocking call...
CMD uvicorn launch:asgi_app --host $DBSERVER_HOST --port $DBSERVER_PORT


# -----------------------------------------------------------------------------
# To use manually:

# sudo docker build -t dbserver_image .
# sudo docker run -d --network="host" -v /tmp/images_dbserver:/home/scv2/images_dbserver --name dbserver_container dbserver_image

