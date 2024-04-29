from pymongo import MongoClient
from utils import config

config.init()

def connect_mongodb(coll, client=None):
  if not client:
      client = MongoClient(config.mongodb_url)
  db = client['pacs_live']
  collection = db[coll if coll else "dicom_metadata"]
  return collection

def client_mongodb():
  return MongoClient(config.mongodb_url)