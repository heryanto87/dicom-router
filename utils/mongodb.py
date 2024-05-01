from pymongo import MongoClient
from utils import config

config.init()

def connect_mongodb():
  client = MongoClient(config.mongodb_url)

  db = client['pacs']
  # collection = db['dicom_metadata']
  return db