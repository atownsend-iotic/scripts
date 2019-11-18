#!/usr/bin/env python3
#-*- coding: utf-8 -*-

# import stuff we need
from datetime import datetime, timedelta, time
import random

# import the MongoClient class of the PyMongo library
from pymongo import MongoClient

# import ObjectID from MongoDB's BSON library
# (use pip3 to install bson)
from bson import ObjectId

# Set a date range
start = datetime.now()
end = start - timedelta(days=31)

# create a client instance of the MongoClient class
# readwrite conn string
mongo_client = MongoClient('mongodb://<<connection_string>>')

# create database and collection instances
db = mongo_client["<<database_name>>"]
col = db["<<collection_name>>"]

# check if collection has documents
total_docs = col.count_documents( {} )
print (col.name, "has", total_docs, "total documents.")

# Query to get the docs
docs = col.find({ "_id": ObjectId("5d5e1dc70e7bf70001a14b11")})

# Query to get all the docs
#docs = col.find()

# Loop through each doc in collection
# Generate a random date string in the past 31days
# Update doc in CosmosDB with new date

for doc in docs:
  # Generate a random date string
  random_date = start + (end - start) * random.random()
  newDate = random_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')
  
  # Set a new value 
  newvalues = { "$set": { "Ts": newDate } }  

  # Get the ID of the doc, needed for update query
  docId = '{0}'.format(doc['_id'])
  myquery = { "_id": ObjectId(docId) }

  print("Updating ID: " + docId + " with value : " + newDate)

  # Build query string
  result = col.update_one(myquery, newvalues) 
  print (result.modified_count, " documents updated")  
    
