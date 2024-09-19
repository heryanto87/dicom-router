from pymongo import MongoClient
from utils import config
import certifi
import os

# Initialize configuration
config.init()

def connect_mongodb(coll=None, client=None):
		"""
		Connect to a MongoDB collection.

		Args:
				coll (str, optional): The name of the collection to connect to. Defaults to "dicom_metadata".
				client (MongoClient, optional): An existing MongoClient instance. If not provided, a new instance is created.

		Returns:
				Collection: A MongoDB collection object.
		"""
		mongodb_url = config.mongodb_url
		db_name = config.pacs_db_name
		if client is None:
				client = MongoClient(mongodb_url,  tlsCAFile=certifi.where())

		db = client[db_name]
		collection_name = coll or "dicom_metadata"  # Use default collection if coll is None
		return db[collection_name]

def client_mongodb():
		"""
		Create and return a new MongoClient instance.

		Returns:
				MongoClient: A new MongoClient connected to the MongoDB server.
		"""
		mongodb_url = config.mongodb_url
		return MongoClient(mongodb_url, tlsCAFile=certifi.where())
