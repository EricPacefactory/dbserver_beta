#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 16:47:52 2020

@author: eo
"""


# ---------------------------------------------------------------------------------------------------------------------
#%% Imports

from local.lib.mongo_helpers import connect_to_mongo

import pymongo

# ---------------------------------------------------------------------------------------------------------------------
#%% Define functions

# .....................................................................................................................

def manual_connect(mongo_url, connection_timeout_ms = 4000):
    
    ''' Helper function which handles the mongo connection, but with a directly supplied url '''
    
    # Get database object for manipulation
    mongo_client = pymongo.MongoClient(mongo_url, tz_aware = False, serverSelectionTimeoutMS = connection_timeout_ms)
    
    return mongo_client

# .....................................................................................................................
    
def print_existing_dbs(mongo_client):
    print(mongo_client.list_database_names())

# .....................................................................................................................

def print_existing_collections(mongo_client, database_name = None):
    
    # Build a list of database names to check (or use just the one provided)
    database_name_list = [database_name]
    if database_name is None:
        database_name_list = mongo_client.list_database_names()
    
    # Print out the listing of all collections for each database name
    for each_database_name in database_name_list:
        collection_name_list = mongo_client[each_database_name].list_collection_names()
        collection_name_list = collection_name_list if collection_name_list else ["--> database is empty!"]
        print("",
              "{} collections:".format(each_database_name),
              *["  {}".format(each_col_name) for each_col_name in collection_name_list],
              sep = "\n")
    
    return

# .....................................................................................................................

def print_summary(mongo_client):
        
    ''' For convenience, just list out all the databases & corresponding collections '''
    
    # Get all databases
    database_name_list = mongo_client.list_database_names()
    
    # Build the repr string for all collections for each database name
    repr_strs = []
    for each_database_name in database_name_list:
        
        # For convenience
        db_ref = mongo_client[each_database_name]
        collection_name_list = db_ref.list_collection_names()
        
        # Hard-code a special response if there are no collections in the database
        if not collection_name_list:
            repr_strs += ["  --> database is empty!"]
            continue
        
        # Add the name of each collection, along with the 'estimated document count' under each database name
        repr_strs += ["", "{} collections:".format(each_database_name)]
        for each_col_name in collection_name_list:
            each_doc_count = db_ref[each_col_name].estimated_document_count()
            repr_strs += ["  {} ({} documents)".format(each_col_name, each_doc_count)]
        
    print("\n".join(repr_strs))

# .....................................................................................................................
# .....................................................................................................................

# ---------------------------------------------------------------------------------------------------------------------
#%% Connect to mongo

# Try connecting to mongo
mclient = connect_to_mongo(connection_timeout_ms = 2000, max_connection_attempts = 3)

# Print info about the client variable name for manual usage
print("", "Mongo client is bound to variable:", "  mclient", "", sep = "\n")


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap
